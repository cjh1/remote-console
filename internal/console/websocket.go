package console

import (
	"log"
	"sync"
	"time"
	"errors"

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
	send      chan webSockMessage // outbound messages to be sent to the client
	closeOnce sync.Once // ensures close operations are only done once
	name      string
	closed 	chan struct{} // closed when the session is closed
	onClose   func() 	  // called when the session is closed, used to inform owners of the session that it is closed
}

func NewWebSocketSession(conn *websocket.Conn, name string, onClose func()) *webSocketSession {
	return &webSocketSession{
		conn:    conn,
		send:    make(chan webSockMessage, 64),
		closed: make(chan struct{}),
		name:    name,
		onClose: onClose,
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
    case <-ws.closed:
        return errors.New("websocket session closed")
    }
}


func (ws *webSocketSession) closeChannels() {
	ws.closeOnce.Do(func() {
		close(ws.closed)
		close(ws.send)
	})
}

func (ws *webSocketSession) Close() {
	ws.closeOnce.Do(func() {
		// Close done channel first to stop new writes
		close(ws.closed)

		// Allow outstanding messages to flush before we close the socket.
		for { 
			select {
			case msg := <-ws.send:
				ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
				if err := ws.conn.WriteMessage(msg.messageType, msg.data); err != nil {
					log.Printf("WebSocket write failed during drain for %s: %v", ws.name, err)
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
                log.Printf("WebSocket write failed for %s: %v", ws.name, err)
                // Connection broken, close channels immediately (can't drain)
                ws.closeChannels()
                ws.handleClose()
                return
            }
		case <-ticker.C:
			ws.conn.SetWriteDeadline(time.Now().Add(writeWait))
            if err := ws.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
                log.Printf("WebSocket ping failed for %s: %v", ws.name, err)
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