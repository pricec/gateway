package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/pricec/golib/log"
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

func main() {
	log.Info("Starting gateway...")
	wantExit := false
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make (chan os.Signal)
	doneChan := make(chan struct{})

	sigHandler(ctx, &wantExit, doneChan, sigChan)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	for !wantExit {
		log.Info("Initializing gateway")

		<- doneChan
	}
	cancel()

	log.Info("Gateway exiting")
	log.Flush()
}
