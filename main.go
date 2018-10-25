package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/pricec/golib/log"
	"github.com/pricec/gateway/session"
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

func ReadCb (id session.SessionId, data []byte) {
	log.Info("Received message from %v: %+v", id, data)
}

func main() {
	// Set up logging
	log.SetLevel(log.LL_DEBUG)
	log.Info("Starting gateway...")

	// Variable declarations
	wantExit := false
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make (chan os.Signal)
	doneChan := make(chan struct{})

	// Signal handling
	sigHandler(ctx, &wantExit, doneChan, sigChan)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Initialize the session manager
	sm, err := session.NewSessionManager(ctx, ReadCb)
	if err != nil {
		log.Crit("Failed to create session manager: %v", err)
		os.Exit(-1)
	}

	// Initialize the HTTP server
	httpServer := &http.Server{ Addr: ":8080" }
	http.HandleFunc("/open", sm.Open)

	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			log.Info("HTTP server error: %v", err)
		}
	}()

	for !wantExit {
		log.Info("Initializing gateway")

		<- doneChan
	}
	cancel()

	// Clean up the HTTP server
	if err := httpServer.Shutdown(nil); err != nil {
		log.Warning("Failed to shut down HTTP server gracefully: %v", err)
	}

	log.Info("Gateway exiting")
	log.Flush()
}
