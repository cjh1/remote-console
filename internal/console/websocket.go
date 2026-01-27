package console

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = 30 * time.Second
)

type webSockMessage struct {
	messageType int
	data        []byte
}

type sessionCloseReason int

const (
	sessionCloseNormal sessionCloseReason = iota
	sessionCloseCanceled
	sessionCloseError
)

type webSocketSession struct {
	conn        *websocket.Conn
	send        chan webSockMessage // outbound messages to be sent to the client
	name        string
	ctx         context.Context // cancelled when the session is closed
	cancel      context.CancelFunc
	closeMutex  sync.Mutex
	closeCode   int
	closeReason string
}

func newWebSocketSession(conn *websocket.Conn, name string) *webSocketSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &webSocketSession{
		conn:   conn,
		send:   make(chan webSockMessage, 64),
		ctx:    ctx,
		cancel: cancel,
		name:   name,
	}
}

func (ws *webSocketSession) Start() {
	go ws.writePump()
}

func (ws *webSocketSession) configureReadDeadlines() {
	if err := ws.conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		slog.Warn("Failed to set read deadline", "name", ws.name, "error", err)
	}
	ws.conn.SetPongHandler(func(string) error {
		if err := ws.conn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			slog.Warn("Failed to extend read deadline on pong", "name", ws.name, "error", err)
		}
		return nil
	})
}

func (ws *webSocketSession) Read() (int, []byte, error) {
	ws.configureReadDeadlines()
	return ws.conn.ReadMessage()
}

func (ws *webSocketSession) Write(messageType int, data []byte) error {
	payload := append([]byte(nil), data...)

	select {
	// Offload the actual send to the writePump goroutine
	case ws.send <- webSockMessage{messageType: messageType, data: payload}:
		return nil
	case <-ws.ctx.Done():
		return errors.New("websocket session closed")
	}
}

func (ws *webSocketSession) close(reason sessionCloseReason, message string) {
	ws.closeMutex.Lock()
	code := mapCloseReason(reason)
	ws.closeCode = code
	ws.closeReason = message
	ws.closeMutex.Unlock()
	ws.cancel()
}

func (ws *webSocketSession) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	defer func() {
		if err := ws.conn.Close(); err != nil {
			slog.Debug("Failed to close websocket connection", "name", ws.name, "error", err)
		}
	}()

	for {
		select {
		// Handle outbound messages
		case msg, ok := <-ws.send:
			if err := ws.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				slog.Warn("Failed to set write deadline", "name", ws.name, "error", err)
			}
			if !ok {
				if err := ws.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
					slog.Debug("Failed to write close message", "name", ws.name, "error", err)
				}
				return
			}
			if err := ws.conn.WriteMessage(msg.messageType, msg.data); err != nil {
				slog.Error("WebSocket write failed", "name", ws.name, "error", err)
				// Connection broken, close channels immediately (can't drain)
				ws.cancel()
				return
			}
		// Handle periodic ping
		case <-ticker.C:
			if err := ws.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				slog.Warn("Failed to set write deadline for ping", "name", ws.name, "error", err)
			}
			if err := ws.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				slog.Error("WebSocket ping failed", "name", ws.name, "error", err)
				// Connection broken, close channels immediately (can't drain)
				ws.cancel()
				return
			}

		case <-ws.ctx.Done():
			// Session closed, drain outbound messages
			for {
				select {
				case msg := <-ws.send:
					if err := ws.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
						slog.Warn("Failed to set write deadline during drain", "name", ws.name, "error", err)
					}
					if err := ws.conn.WriteMessage(msg.messageType, msg.data); err != nil {
						slog.Error("WebSocket write failed during drain", "name", ws.name, "error", err)
						return
					}
				default:
					if err := ws.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
						slog.Warn("Failed to set write deadline for close", "name", ws.name, "error", err)
					}
					ws.closeMutex.Lock()
					code := ws.closeCode
					reason := ws.closeReason
					ws.closeMutex.Unlock()
					if err := ws.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason)); err != nil {
						slog.Debug("Failed to write close message", "name", ws.name, "error", err)
					}
					return
				}
			}
		}
	}
}

// mapCloseReason maps internal session close reasons to WebSocket close codes
func mapCloseReason(reason sessionCloseReason) int {
	switch reason {
	case sessionCloseNormal:
		return websocket.CloseNormalClosure
	case sessionCloseCanceled:
		return websocket.CloseGoingAway
	case sessionCloseError:
		return websocket.CloseInternalServerErr
	default:
		slog.Warn("Unknown session close reason, defaulting to CloseGoingAway", "reason", reason)
		return websocket.CloseGoingAway
	}
}

// Done returns a channel that's closed when the websocket session is closed.
// This allows parent sessions to detect when the websocket has closed.
func (ws *webSocketSession) Done() <-chan struct{} {
	return ws.ctx.Done()
}
