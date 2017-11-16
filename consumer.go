package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	config := &kafka.ConfigMap{
		"metadata.broker.list":            os.Getenv("CLOUDKARAFKA_BROKERS"),
		"security.protocol":               "SASL_SSL",
		"sasl.mechanisms":                 "SCRAM-SHA-256",
		"sasl.username":                   os.Getenv("CLOUDKARAFKA_USERNAME"),
		"sasl.password":                   os.Getenv("CLOUDKARAFKA_PASSWORD"),
		"group.id":                        "cloudkarafka-example",
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"default.topic.config":            kafka.ConfigMap{"auto.offset.reset": "earliest"},
		//"debug":                           "generic,broker,security",
	}
	topic := os.Getenv("CLOUDKARAFKA_TOPIC_PREFIX") + ".test"

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	c, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Created Consumer %v\n", c)
	err = c.Subscribe(topic, nil)
	run := true
	counter := 0
	commitAfter := 1000
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				c.Unassign()
			case *kafka.Message:
				fmt.Printf("%% Message on %s: %s\n", e.TopicPartition, string(e.Value))
				counter++
				if counter > commitAfter {
					c.Commit()
					counter = 0
				}

			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				run = false
			}
		}
	}
	fmt.Printf("Closing consumer\n")
	c.Close()
}
