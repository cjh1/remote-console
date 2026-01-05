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

type webSocketSession struct {
	conn       *websocket.Conn
	send       chan webSockMessage // outbound messages to be sent to the client
	closeOnce  sync.Once           // ensures send channel is only closed once
	name       string
	ctx        context.Context // cancelled when the session is closed
	cancel     context.CancelFunc
	onClose    func() // called when the session is closed, used to inform owners of the session that it is closed
}

func NewWebSocketSession(conn *websocket.Conn, name string, onClose func()) *webSocketSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &webSocketSession{
		conn:       conn,
		send:       make(chan webSockMessage, 64),
		ctx:        ctx,
		cancel: cancel,
		name:       name,
		onClose:    onClose,
	}
}

func (ws *webSocketSession) Start() {
	go ws.writePump()
}

func (ws *webSocketSession) configureReadDeadlines() {
	ws.conn.SetReadDeadline(time.Now().Add(pongWait))
	ws.conn.SetPongHandler(func(string) error {
		ws.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
}

func (ws *webSocketSession) Read() (int, []byte, error) {
	return ws.conn.ReadMessage()
}

func (ws *webSocketSession) Write(messageType int, data []byte) error {
	payload := append([]byte(nil), data...)
	
	select {
	case ws.send <- webSockMessage{messageType: messageType, data: payload}:
		return nil
	case <-ws.ctx.Done():
		return errors.New("websocket session closed")
	}
}


func (ws *webSocketSession) closeChannels() {
	// Cancel context to signal closure
	ws.cancel()
	
	// Close send channel (protected by closeOnce)
	ws.closeOnce.Do(func() {
		close(ws.send)
	})
}

func (ws *webSocketSession) Close() {
	// Cancel context to signal closure (safe to call multiple times)
	ws.cancel()

	// Try to drain outstanding messages before closing send channel
	// Use closeOnce to ensure we only close the send channel once
	ws.closeOnce.Do(func() {
		for {
			select {
			case msg := <-ws.send:
				ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
				if err := ws.conn.WriteMessage(msg.messageType, msg.data); err != nil {
					slog.Error("WebSocket write failed during drain", "name", ws.name, "error", err)
				}
			// No more messages to send, we can close the channel
			default:
				close(ws.send)
				return
			}
		}
	})
}


func (ws *webSocketSession) writePump() {
    ticker := time.NewTicker(pingPeriod)
    defer ticker.Stop()
    defer ws.conn.Close()

    for {
        select {
		case msg, ok := <-ws.send:
            ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
            if !ok {
                ws.conn.WriteMessage(websocket.CloseMessage, []byte{})
                return
            }
            if err := ws.conn.WriteMessage(msg.messageType, msg.data); err != nil {
                slog.Error("WebSocket write failed", "name", ws.name, "error", err)
                // Connection broken, close channels immediately (can't drain)
                ws.closeChannels()
                ws.handleClose()
                return
            }
		case <-ticker.C:
			ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
            if err := ws.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
                slog.Error("WebSocket ping failed", "name", ws.name, "error", err)
                // Connection broken, close channels immediately (can't drain)
                ws.closeChannels()
                ws.handleClose()
                return
            }
        }
	}
}

func (ws *webSocketSession) handleClose() {
	if ws.onClose != nil {
		ws.onClose()
	}
}