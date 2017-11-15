//producer.go

package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	config := &kafka.ConfigMap{
		"metadata.broker.list": os.Getenv("CLOUDKARAFKA_BROKERS"),
		"security.protocol":    "SASL_SSL",
		"sasl.mechanisms":      "SCRAM-SHA-256",
		"sasl.username":        os.Getenv("CLOUDKARAFKA_USERNAME"),
		"sasl.password":        os.Getenv("CLOUDKARAFKA_PASSWORD"),
		"group.id":             os.Getenv("CLOUDKARAFKA_GROUPID"),
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"},
		//"debug":                           "generic,broker,security",
	}
	topic := os.Getenv("CLOUDKARAFKA_TOPIC_PREFIX") + ".test"
	p, err := kafka.NewProducer(config)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created Producer %v\n", p)
	deliveryChan := make(chan kafka.Event)

	for i := 0; i < 10; i++ {
		value := fmt.Sprintf("[%d] Hello Go!", i+1)
		err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: []byte(value)}, deliveryChan)
		e := <-deliveryChan
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
	}
	close(deliveryChan)
}
