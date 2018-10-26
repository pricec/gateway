package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/pricec/golib/log"
	"github.com/pricec/gateway/session"
	"github.com/pricec/gateway/kafka"
)

func sigHandler(
	ctx context.Context,
	wantExit *bool,
	doneChan chan<- struct{},
	sigChan <-chan os.Signal,
) {
	go func() {
		for {
			select{
			case sig := <-sigChan:
				switch sig {
				case syscall.SIGINT:
					fallthrough
				case syscall.SIGTERM:
					*wantExit = true
					fallthrough
				case syscall.SIGHUP:
					doneChan <- struct{}{}
				default:
					log.Warning("Received unrecognized signal %v", sig)
				}
			case <- ctx.Done():
				return
			}
		}
	}()
}

func ReadCb(km *kafka.KafkaManager) func(id session.SessionId, data []byte) {
	return func(id session.SessionId, data []byte) {
		log.Info("Received message from %v: %+v", id, data)
		if err := km.Send("test", data); err != nil {
			log.Err("Failed to send message to kafka: %v", err)
		}
	}
}

func main() {
	// Set up logging
	log.SetLevel(log.LL_DEBUG)
	defer log.Flush()
	log.Info("Starting gateway...")

	// Variable declarations
	wantExit := false
	ready := false
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make (chan os.Signal)
	doneChan := make(chan struct{})

	// Signal handling
	sigHandler(ctx, &wantExit, doneChan, sigChan)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

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

	for !wantExit {
		log.Info("Initializing gateway")
		ready = true
		<- doneChan
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
