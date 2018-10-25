package kafka

import (
	"context"
	"fmt"

	"github.com/pricec/golib/log"
	"github.com/Shopify/sarama"
)

type KafkaManager struct {
	ctx context.Context
	cancel context.CancelFunc
	host string
	port uint16
	topics []string
	config *sarama.Config
	client sarama.Client
	producer sarama.AsyncProducer
	consumer sarama.Consumer
	consumers []sarama.PartitionConsumer
}

func NewKafkaManager(
	ctx context.Context,
	host string,
	port uint16,
) (*KafkaManager, error) {
	// The config contains global options as well as
	// options which apply specifically to the producer,
	// consumer, or other classes.
	config := sarama.NewConfig()

	// The client connects to Kafka
	client, err := sarama.NewClient(
		[]string{fmt.Sprintf("%s:%d", host, port)},
		config,
	)
	if err != nil {
		return nil, err
	}

	// The producer is used to send messages to any topic
	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	// The consumer is used to create partition consumers to
	// consume from each topic.
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	kmCtx, cancel := context.WithCancel(ctx)

	km := &KafkaManager{
		ctx: kmCtx,
		cancel: cancel,
		host: host,
		port: port,
		topics: []string{},
		config: config,
		client: client,
		producer: producer,
		consumer: consumer,
		consumers: []sarama.PartitionConsumer{},
	}

	km.handleProducerFeedback()

	topics, err := client.Topics()
	if err != nil {
		log.Err("Failed looking up topics: %v", err)
	} else {
		log.Notice("Connected to kafka. Topics: %+v", topics)
	}

	for _, topic := range topics {
		partitions, err := client.WritablePartitions(topic)
		if err != nil {
			log.Err("Failed looking up writable partitions: %v", err)
		} else {
			log.Notice("Partitions for topic %v: %+v", topic, partitions)
		}
	}

	return km, nil
}

func (k *KafkaManager) Close() {
	k.cancel()
	for _, consumer := range k.consumers {
		consumer.Close()
	}
	k.consumer.Close()
	k.producer.Close()
	k.client.Close()
}

func (k *KafkaManager) Send(topic string, data []byte) error {
	log.Debug("Sending message '%v' to topic %v", data, topic)
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	k.producer.Input() <- msg
	return nil
}

func (k *KafkaManager) ConsumeTopic() error {
	return fmt.Errorf("KafkaManager::ConsumeTopic is not implemented")
}

// NOTE that messages which cannot be delivered to Kafka are dropped.
func (k *KafkaManager) handleProducerFeedback() {
	go func() {
		select {
		case producerMsg := <- k.producer.Successes():
			log.Debug("Successfully transmitted message '%+v'", producerMsg)
		case producerError := <- k.producer.Errors():
			log.Warning(
				"Error writing message '%+v' to kafka: %v, dropping",
				producerError.Msg,
				producerError.Err,
			)
		case <- k.ctx.Done():
			return
		}
	}()
}
