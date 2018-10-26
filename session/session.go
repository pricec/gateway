package session

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type SessionId uuid.UUID

func (s SessionId) String() string {
	return uuid.UUID(s).String()
}

// TODO: Check request origin (prevent CSRF)
var upgrader = websocket.Upgrader{
	HandshakeTimeout: 3 * time.Second,
	ReadBufferSize: 1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Session struct {
	id         SessionId
	remoteAddr string
	connection *websocket.Conn
}

// Create a new session by upgrading the HTTP
// connection represented by (w,r).
func NewSession(w http.ResponseWriter, r *http.Request) (*Session, error) {
	var session *Session = nil
	conn, err := upgrader.Upgrade(w, r, nil)
	if err == nil {
		session = &Session{
			id: SessionId(uuid.New()),
			remoteAddr: r.RemoteAddr,
			connection: conn,
		}
	}
	return session, err
}

// Close the session. This will close the underlying
// WebSocket and disconnect the client.
func (s *Session) Close() {
	s.connection.Close()
}

// Returns the Id identifying this session
func (s *Session) Id() SessionId {
	return s.id
}

// Returns a description of the session. At present,
// this is just the remote address, but that could
// change in the future.
func (s *Session) Desc() string {
	return s.remoteAddr
}

// Write the argument message to the remote end
// of the session. Note that the argument message
// will be serialized to JSON, and the serialized
// JSON will be sent on the wire.
func (s *Session) Write(v interface{}) error {
	return s.connection.WriteJSON(v)
}

// Reads the next message from the session. This
// function assumes that the incoming message is
// in JSON format. If the returned error is nil,
// the value pointed to by v will be populated
// with the next message on return.
//
// NOTE: Beware that the elements of *v (if v is
//       a struct) will only be populated if they
//       are public.
func (s *Session) Read(v interface{}) error {
	return s.connection.ReadJSON(v)
}

type ReadTimeoutError struct {
	Id       string
	Duration time.Duration
}

func (t ReadTimeoutError) Error() string {
	return fmt.Sprintf(
		"(%v) Timed out after waiting %v to read",
		t.Id,
		t.Duration,
	)
}

// Same as Read(v), but with a timeout. On
// a timeout, a ReadTimeoutError is returned.
func (s *Session) ReadTimeout(timeout time.Duration, v interface{}) error {
	errChan := make(chan error, 1)
	go func() {
		errChan <- s.Read(v)
		close(errChan)
	}()
	
	select {
	case err := <- errChan:
		return err
	case <- time.After(timeout):
		return &ReadTimeoutError{ Id: s.Desc(), Duration: timeout }
	}
}

// Same as Read(v), but with a context.
func (s *Session) ReadContext(ctx context.Context, v interface{}) error {
	errChan := make(chan error, 1)
	go func() {
		errChan <- s.Read(v)
		close(errChan)
	}()
	
	select {
	case err := <- errChan:
		return err
	case <- ctx.Done():
		return errors.New("Context canceled")
	}
}

// Same as ReadContext(v), except instead of marshaling the JSON
// into the struct, it returns the raw bytes instead. The returned
// integer is the message type.
func (s *Session) ReadMessageContext(ctx context.Context) (int, []byte, error) {
	type msgStruct struct {
		messageType int
		message     []byte
		err         error
	}
	errChan := make(chan msgStruct, 1)
	go func() {
		mt, p, err := s.connection.ReadMessage()
		errChan <- msgStruct{
			messageType: mt,
			message: p,
			err: err,
		}
	}()

	select {
	case ms := <- errChan:
		return ms.messageType, ms.message, ms.err
	case <- ctx.Done():
		return 0, nil, errors.New("Context canceled")
	}
}
