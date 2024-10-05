package handler

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka" // Ensure this import is correct
)

// ProcessMessage simulates message processing (returns true if successful, false otherwise)
func ProcessMessage(msg *kafka.Message) bool {
	// Dummy failure for every other message
	return time.Now().Unix()%2 == 0
}
