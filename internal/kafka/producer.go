package kafka

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer struct {
	kafkaProducer *kafka.Producer
}

func NewProducer(broker string) (*Producer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": broker,
	}
	p, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}
	return &Producer{kafkaProducer: p}, nil
}

func (p *Producer) ProduceMessage(topic string, key string, value interface{}) error {
	msgBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Key:            []byte(key),
		Value:          msgBytes,
	}

	return p.kafkaProducer.Produce(message, nil)
}

func (p *Producer) Close() {
	p.kafkaProducer.Flush(15 * 1000)
	p.kafkaProducer.Close()
}
