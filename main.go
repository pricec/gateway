package main

import (
	"context"
	"flag"
	"net/http"

	"github.com/pricec/gateway/session"
	"github.com/pricec/golib/config"
	"github.com/pricec/golib/kafka"
	"github.com/pricec/golib/log"
	"github.com/pricec/golib/signal"
)


type Config struct {
	LogLevel      uint8  `yaml:"log_level,omitempty" env:"LOG_LEVEL"`
	KafkaHost     string `yaml:"kafka_host,omitempty" env:"KAFKA_HOST"`
	KafkaPort     uint16 `yaml:"kafka_port,omitempty" env:"KAFKA_PORT"`
	RequestTopic  string `yaml:"request_topic,omitempty" env:"REQUEST_TOPIC"`
	ResponseTopic string `yaml:"response_topic,omitempty" env:"RESPONSE_TOPIC"`
}

var defaultConfig = Config{
	LogLevel: uint8(log.LL_DEBUG),
	KafkaHost: "kafka.common",
	KafkaPort: uint16(9092),
	RequestTopic: "test_request",
	ResponseTopic: "test_response",
}

func main() {
	defer log.Flush()
	log.Info("Starting gateway...")

	// Command line arguments
	var cfgFile = flag.String("config-file", "", "Path to config")
	flag.Parse()

	// Config
	cfg := defaultConfig
	if err := config.ReadConfig(&cfg, *cfgFile); err != nil {
		log.Crit("Failed to initialize config: %v", err)
		return
	}
	log.Info("Configuration: %+v", cfg)

	// Set up logging
	log.SetLevel(log.LogLevel(cfg.LogLevel))

	// Variable declarations
	ready := false
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize signal handling
	done, reload := signal.Setup(ctx)

	// Initialize the Kafka manager
	km, err := kafka.NewKafkaManager(ctx, cfg.KafkaHost, cfg.KafkaPort)
	if err != nil {
		log.Crit("Failed to connect to kafka: %v", err)
		return
	}

	// Initialize the session manager
	sm, err := session.NewSessionManager(ctx, requestCb(km, cfg.RequestTopic))
	if err != nil {
		log.Crit("Failed to create session manager: %v", err)
		return
	}

	// Set up Kafka consumer for responses
	if err := km.ConsumeTopic(cfg.ResponseTopic, responseCb(sm)); err != nil {
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
