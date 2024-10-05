package kafka

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

type Config struct {
	Kafka struct {
		Brokers string `json:"brokers"`
		GroupId string `json:"groupId"`
		Topics  struct {
			Main   string `json:"main"`
			Retry1 string `json:"retry1"`
			Retry2 string `json:"retry2"`
			Retry3 string `json:"retry3"`
			DLQ    string `json:"dlq"`
		} `json:"topics"`
		Partitions        int               `json:"partitions"`
		ReplicationFactor int               `json:"replicationFactor"`
		DefaultConfig     map[string]string `json:"defaultConfig"`
	} `json:"kafka"`
}

func LoadConfig(file string) (*Config, error) {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
		return nil, err
	}

	var config Config
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		log.Fatalf("Error parsing config file: %v", err)
		return nil, err
	}

	return &config, nil
}
