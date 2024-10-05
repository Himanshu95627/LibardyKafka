package kafka

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func CreateKafkaTopics(config *Config) error {
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": config.Kafka.Brokers})
	if err != nil {
		return err
	}
	defer adminClient.Close()

	topics := []kafka.TopicSpecification{
		{Topic: config.Kafka.Topics.Main, NumPartitions: config.Kafka.Partitions, ReplicationFactor: config.Kafka.ReplicationFactor},
		{Topic: config.Kafka.Topics.Retry1, NumPartitions: config.Kafka.Partitions, ReplicationFactor: config.Kafka.ReplicationFactor},
		{Topic: config.Kafka.Topics.Retry2, NumPartitions: config.Kafka.Partitions, ReplicationFactor: config.Kafka.ReplicationFactor},
		{Topic: config.Kafka.Topics.Retry3, NumPartitions: config.Kafka.Partitions, ReplicationFactor: config.Kafka.ReplicationFactor},
		{Topic: config.Kafka.Topics.DLQ, NumPartitions: config.Kafka.Partitions, ReplicationFactor: config.Kafka.ReplicationFactor},
	}

	results, err := adminClient.CreateTopics(nil, topics, nil)
	if err != nil {
		return err
	}

	for _, result := range results {
		log.Printf("Topic %s creation result: %v\n", result.Topic, result.Error)
	}

	return nil
}
