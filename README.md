# Apache Kafka Producer/Consumer example in Go

One consumer and one producer where the producer send some simple text messages to the consumer.

## Getting started

Setup your free Apache Kafka instance here: https://www.cloudkarafka.com

Configuration

* `export CLOUDKARAFKA_BROKERS="host1:9094,host2:9094,host3:9094"`
  Hostnames can be found in the Details view in for your CloudKarafka instance.
* `export CLOUDKARAFKA_USERNAME="username"`
  Username can be found in the Details view in for your CloudKarafka instance.
* `export CLOUDKARAFKA_PASSWORD="password"`
  Password can be found in the Details view in for your CloudKarafka instance.
* `export CLOUDKARAFKA_TOPIC_PREFIX="same_as_username"`
  Topic prefix should be the same as your username.

These export commands must be run in both of the terminal windows below.

```
git clone https://github.com/CloudKarafka/go-kafka-example.git
cd go-kafka-example`
go get -u github.com/confluentinc/confluent-kafka-go/kafka
go run consumer.go
```

Open another terminal window and `cd` into same directory and run `go run producer.go`.
