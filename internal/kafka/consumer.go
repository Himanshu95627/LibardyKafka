package kafka

import (
	"context"
	"log"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/himanshu95627/internal/handler"
)

type Consumer struct {
	kafkaConsumer *kafka.Consumer
	producer      *Producer
}

func NewConsumer(broker, groupId string, topics []string, producer *Producer) (*Consumer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": broker,
		"group.id":          groupId,
		"auto.offset.reset": "earliest",
	}

	c, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		return nil, err
	}

	return &Consumer{kafkaConsumer: c, producer: producer}, nil
}

func (c *Consumer) ConsumeMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := c.kafkaConsumer.ReadMessage(-1)
			if err != nil {
				log.Printf("Consumer error: %v (%v)\n", err, msg)
				continue
			}

			retryCount := extractRetryCount(msg)
			if handler.ProcessMessage(msg) {
				log.Println("Message processed successfully:", string(msg.Value))
			} else {
				c.handleRetryOrDLQ(msg, retryCount)
			}
		}
	}
}

func (c *Consumer) handleRetryOrDLQ(msg *kafka.Message, retryCount int) {
	retryCount++
	var retryTopic string

	switch retryCount {
	case 1:
		retryTopic = "retry-topic-1"
	case 2:
		retryTopic = "retry-topic-2"
	case 3:
		retryTopic = "retry-topic-3"
	default:
		retryTopic = "dlq-topic"
		log.Printf("Message sent to DLQ: %v\n", string(msg.Value))
	}

	c.producer.ProduceMessage(retryTopic, string(msg.Key), msg.Value)
}

func (c *Consumer) Close() {
	c.kafkaConsumer.Close()
}

func extractRetryCount(msg *kafka.Message) int {
	for _, header := range msg.Headers {
		if header.Key == "retryCount" {
			retryCount, err := strconv.Atoi(string(header.Value))
			if err != nil {
				log.Printf("Error converting retryCount to integer: %v", err)
				return 0
			}
			return retryCount
		}
	}
	return 0
}
