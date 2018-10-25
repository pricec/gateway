package session

import (
	"context"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/pricec/golib/log"
)

type SessionMessage struct {
	uuid      uuid.UUID `json:"uuid"`
	senderId  uuid.UUID `json:"sender_id"`
	timestamp time.Time `json:"timestamp"`
	data      []byte    `json:"data"`
}

type SessionManager struct {
	ctx context.Context
	cancel context.CancelFunc
	readCb func(SessionMessage)
}

// Returns a new SessionManager. Note that requestType should
// be precisely the type expected, NOT a pointer to a variable
// of that type.
func NewSessionManager(
	ctx context.Context,
	readCb func(SessionMessage),
) (*SessionManager, error) {
	smCtx, cancel := context.WithCancel(ctx)
	sm := &SessionManager{
		ctx: smCtx,
		cancel: cancel,
		readCb: readCb,
	}
	return sm, nil
}

func (s *SessionManager) Start() error {
	return nil
}

func (s *SessionManager) Stop() error {
	return nil
}

func (s *SessionManager) Write(uuid uuid.UUID, v interface{}) error {
	return nil
}

func (s *SessionManager) add(session *Session) error {
	log.Debug("Adding session %v", session.Desc())
	return nil
}

func (s *SessionManager) remove(session *Session) error {
	log.Debug("Removing session %v", session.Desc())
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
			s.readCb(SessionMessage{
				uuid: uuid.New(),
				senderId: session.Id(),
				timestamp: time.Now().UTC(),
				data: msg,
			})
		}
	}
}

