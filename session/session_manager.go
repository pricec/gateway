package session

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/pricec/golib/log"
)

type SessionTable struct {
	rw sync.RWMutex
	sessions map[SessionId]*Session
}

type SessionManager struct {
	ctx context.Context
	cancel context.CancelFunc
	readCb func(SessionId, []byte)
	sessionTable SessionTable
}

// Returns a new SessionManager. The readCb function
// will be invoked once for each message received. The
// first argument (id) will be the SessionId of the session;
// when calling the Write function, pass this Id to
// respond to the original caller.
func NewSessionManager(
	ctx context.Context,
	readCb func(SessionId, []byte),
) (*SessionManager, error) {
	smCtx, cancel := context.WithCancel(ctx)
	sm := &SessionManager{
		ctx: smCtx,
		cancel: cancel,
		readCb: readCb,
		sessionTable: SessionTable{
			rw: sync.RWMutex{},
			sessions: make(map[SessionId]*Session),
		},
	}
	return sm, nil
}

// Send the argument object to the argument session. The
// object will be serialized as JSON and sent to the client,
// if they exist. Any error encountered will be returned.
func (s *SessionManager) Write(id SessionId, v interface{}) error {
	s.sessionTable.rw.RLock()
	defer s.sessionTable.rw.RUnlock()
	if sess, ok := s.sessionTable.sessions[id]; !ok {
		return fmt.Errorf("No session with ID %v", id)
	} else {
		return sess.Write(v)
	}
}

// TODO: This function is not really thread safe, but it
//       is if you make the following two assumptions.
//
//   1. We will never add a session with the same Id twice
//   2. We will never iterate the session table
func (s *SessionManager) add(session *Session) error {
	log.Debug("Adding session %v", session.Desc())
	s.sessionTable.sessions[session.Id()] = session
	return nil
}

func (s *SessionManager) remove(session *Session) error {
	log.Debug("Removing session %v", session.Desc())
	s.sessionTable.rw.Lock()
	defer s.sessionTable.rw.Unlock()
	delete(s.sessionTable.sessions, session.Id())
	return nil
}

// Hook this up to an HTTP endpoint, and the incoming
// connections will be managed by this SessionManager.
func (s *SessionManager) Open(w http.ResponseWriter, r *http.Request) {
	session, err := NewSession(w,r)
	if err != nil {
		log.Notice("(%v) Failed to open session: %v", r.RemoteAddr, err)
		return
	}
	defer session.Close()

	s.add(session)
	defer s.remove(session)

	s.handleRequests(session)
}

// This function handles incoming messages from the client.
// For each message received, the read callback is invoked
// with the SessionId and the data sent by the client.
func (s *SessionManager) handleRequests(session *Session) {
	for {
		_, msg, err := session.ReadMessageContext(s.ctx)
		if err != nil {
			log.Notice("(%v) Error reading message: %v", session.Desc(), err)
			return
		}

		if s.readCb == nil {
			log.Warning("Session manager read callback is not initialized")
		} else {
			s.readCb(session.Id(), msg)
		}
	}
}
