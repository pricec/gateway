package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/pricec/golib/kafka"
	"github.com/pricec/golib/log"
	"github.com/pricec/golib/signal"
	"github.com/pricec/gateway/session"
	message "github.com/pricec/protobuf/go/socket-gateway"
)

func responseCb(s *session.SessionManager) func([]byte) {
	return func(data []byte) {
		resp := &message.Response{}
		if err := proto.Unmarshal(data, resp); err != nil {
			log.Notice("Received bad response message '%v': %v", data, err)
			return
		} else {
			log.Debug("Received response message: %+v", resp)
		}

		idStr := resp.GetClientId()
		if id, err := session.NewSessionId(idStr); err != nil {
			log.Notice(
				"Received message for nonexistent client '%v': %v",
				idStr,
				err,
			)
			return
		} else {
			s.Write(id, data)
		}
	}
}

func decodeRequest(data []byte, id session.SessionId) (proto.Message, error) {
	req := &message.Request{}
	if err := proto.Unmarshal(data, req); err != nil {
		return nil, fmt.Errorf("Error unmarshaling request: %v", err)
	}

	reqType := req.GetType()
	switch reqType {
	case message.RequestType_ECHO:
		request := &message.EchoRequest{}
		if err := proto.Unmarshal(data, request); err != nil {
			return nil, fmt.Errorf("Error unmarshaling request: %v", err)
		}
		request.ClientId = id.String()
		return request, nil
	default:
		return nil, fmt.Errorf("Unrecognized request type %v", reqType)
	}
}

func requestCb(
	km *kafka.KafkaManager,
) func(*session.SessionManager, session.SessionId, []byte) {
	return func(s *session.SessionManager, id session.SessionId, data []byte) {
		req, err := decodeRequest(data, id)
		if err != nil {
			log.Err("Failed to decode request for %v: %v", id, err)
		} else {
			if out, err := proto.Marshal(req); err != nil {
				log.Err("Failed to marshal modified request: %v", err)
			} else if err := km.Send("test_request", out); err != nil {
				log.Err("Failed to send message '%v' to kafka: %v", data, err)
				// TODO: send a message indicating the failure
			}
		}
	}
}

func main() {
	// Set up logging
	log.SetLevel(log.LL_DEBUG)
	defer log.Flush()
	log.Info("Starting gateway...")

	// Variable declarations
	ready := false
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize signal handling
	done, reload := signal.Setup(ctx)

	// Initialize the Kafka manager
	km, err := kafka.NewKafkaManager(ctx, "kafka.common", uint16(9092))
	if err != nil {
		log.Crit("Failed to connect to kafka: %v", err)
		return
	}

	// Initialize the session manager
	sm, err := session.NewSessionManager(ctx, requestCb(km))
	if err != nil {
		log.Crit("Failed to create session manager: %v", err)
		return
	}

	// Set up Kafka consumer for responses
	if err := km.ConsumeTopic("test_response", responseCb(sm)); err != nil {
		log.Crit("Failed to set up response consumer")
		return
	}

	// Initialize the HTTP server
	httpServer := &http.Server{ Addr: ":8080" }
	http.HandleFunc("/health", healthCheck)
	http.HandleFunc("/ready", readinessCheck(&ready))
	http.HandleFunc("/open", sm.Open)

	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			log.Info("HTTP server error: %v", err)
		}
	}()

	for !done() {
		log.Info("Initializing gateway")
		ready = true
		<-reload()
		ready = false
	}
	cancel()

	// Clean up the HTTP server
	if err := httpServer.Shutdown(nil); err != nil {
		log.Warning("Failed to shut down HTTP server gracefully: %v", err)
	}

	log.Info("Gateway exiting")
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "", http.StatusOK)
}

func readinessCheck(ready *bool) func(http.ResponseWriter, *http.Request) {
	return func (w http.ResponseWriter, r *http.Request) {
		if *ready {
			http.Error(w, "", http.StatusOK)
		} else {
			http.Error(w, "", http.StatusServiceUnavailable)
		}
	}
}
