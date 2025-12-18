package console

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
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
	conn      *websocket.Conn
	send      chan webSockMessage
	closeOnce sync.Once
	name      string
	onClose   func()
	// TODO is there a better way to do this?
	closed    atomic.Bool
}

func newWebSocketSession(conn *websocket.Conn, name string, onClose func()) *webSocketSession {
	return &webSocketSession{
		conn:    conn,
		send:    make(chan webSockMessage, 64),
		name:    name,
		onClose: onClose,
	}
}

func (ws *webSocketSession) start(ctx context.Context) {
	go ws.writePump(ctx)
}

func (ws *webSocketSession) configureReadDeadlines() {
	ws.conn.SetReadDeadline(time.Now().Add(pongWait))
	ws.conn.SetPongHandler(func(string) error {
		ws.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
}

func (ws *webSocketSession) readMessage() (int, []byte, error) {
	return ws.conn.ReadMessage()
}

func (ws *webSocketSession) write(ctx context.Context, messageType int, data []byte) error {
	if ws.closed.Load() {
		return websocket.ErrCloseSent
	}
	// if send channel if close
	val, ok := <-ws.send
	if !ok {
		return websocket.ErrCloseSent
	} 

	payload := append([]byte(nil), data...)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case ws.send <- webSockMessage{messageType: messageType, data: payload}:
		return nil
	}
}

func (ws *webSocketSession) close() {
	ws.closeOnce.Do(func() {
		log.Printf("Closing WebSocket session: %s", ws.name)
		ws.closed.Store(true)
		// Close send channel - this will cause writePump to exit
		close(ws.send)
	})
}

func (ws *webSocketSession) writePump(ctx context.Context) {
    ticker := time.NewTicker(pingPeriod)
    defer ticker.Stop()
    defer ws.conn.Close()

	cancelled := false

    for {
        select {
		case <-ctx.Done():
			if !cancelled {
				cancelled = true
                // Allow outstanding messages to flush before we close the socket.
                ws.close()
            }
		case msg, ok := <-ws.send:
            ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
            if !ok {
                ws.conn.WriteMessage(websocket.CloseMessage, []byte{})
                return
            }
            if err := ws.conn.WriteMessage(msg.messageType, msg.data); err != nil {
                log.Printf("WebSocket write failed for %s: %v", ws.name, err)
                ws.handleClose()
                return
            }
		case <-ticker.C:
			// If we've been cancelled, don't send any more pings.
			if cancelled {
                continue
            }
            ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
            if err := ws.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
                log.Printf("WebSocket ping failed for %s: %v", ws.name, err)
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
