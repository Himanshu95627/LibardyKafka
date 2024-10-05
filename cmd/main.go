package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/himanshu95627/internal/kafka"
)

func main() {
	config, err := kafka.LoadConfig("config/config.json")

	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create Kafka topics on startup
	err = kafka.CreateKafkaTopics(config)
	if err != nil {
		log.Fatalf("Failed to create Kafka topics: %v", err)
	}

	// Initialize Producer
	producer, err := kafka.NewProducer(config.Kafka.Brokers)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Initialize Consumer
	consumer, err := kafka.NewConsumer(config.Kafka.Brokers, config.Kafka.GroupId,
		[]string{config.Kafka.Topics.Main, config.Kafka.Topics.Retry1, config.Kafka.Topics.Retry2, config.Kafka.Topics.Retry3}, producer)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Setup a context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		consumer.ConsumeMessages(ctx)
	}()

	// Handle OS signals for clean shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down gracefully...")
	time.Sleep(2 * time.Second)
}
