package console

import (
	"context"
	"errors"
	"log/slog"
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
	name       string
	ctx        context.Context // cancelled when the session is closed
	cancel     context.CancelFunc
}

func NewWebSocketSession(conn *websocket.Conn, name string) *webSocketSession {
	ctx, cancel := context.WithCancel(context.Background())
	return &webSocketSession{
		conn:       conn,
		send:       make(chan webSockMessage, 64),
		ctx:        ctx,
		cancel: cancel,
		name:       name,
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


func (ws *webSocketSession) Close() {
	ws.cancel()
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
				ws.cancel()
				return
            }
		case <-ticker.C:
			ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
            if err := ws.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
                slog.Error("WebSocket ping failed", "name", ws.name, "error", err)
                // Connection broken, close channels immediately (can't drain)
				ws.cancel()
				return
            }
		case <-ws.ctx.Done():
			for {
				select {
				case msg := <-ws.send:
					ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
					if err := ws.conn.WriteMessage(msg.messageType, msg.data); err != nil {
						slog.Error("WebSocket write failed during drain", "name", ws.name, "error", err)
						return
					}
				default:
					ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
					ws.conn.WriteMessage(websocket.CloseMessage, []byte{})
					return
				}
			}
        }
	}
}

// Done returns a channel that's closed when the websocket session is closed.
// This allows parent sessions to detect when the websocket has closed.
func (ws *webSocketSession) Done() <-chan struct{} {
	return ws.ctx.Done()
}
