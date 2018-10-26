package main

import (
	"context"
	"net/http"

	"github.com/pricec/golib/kafka"
	"github.com/pricec/golib/log"
	"github.com/pricec/golib/signal"
	"github.com/pricec/gateway/session"
)

func ReadCb(
	km *kafka.KafkaManager,
) func(*session.SessionManager, session.SessionId, []byte) {
	return func(s *session.SessionManager, id session.SessionId, data []byte) {
		log.Debug("Received message from %v: %+v", id, data)
		if err := km.Send("test", data); err != nil {
			log.Err("Failed to send message '%v' to kafka: %v", data, err)
			// TODO: send a message indicating the failure
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
	sm, err := session.NewSessionManager(ctx, ReadCb(km))
	if err != nil {
		log.Crit("Failed to create session manager: %v", err)
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
