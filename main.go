package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/nkristianto/kafkabulkcommit/kafkaclient"
)

func main() {
	broker := "localhost:29092"
	group := "consumer_group_"
	topics := []string{"kafka_test1"}

	typeFlag := flag.String("type", "consumer", "a string")
	numOfPublish := flag.Int("publishCount", 1, "number of message to publish")

	flag.Parse()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	if *typeFlag == "producer" {
		kafkaclient := new(kafkaclient.KafkaProducer)
		producer := kafkaclient.CreateProducer(broker)
		for i := 0; i < *numOfPublish; i++ {
			if err := kafkaclient.Produce(producer, "kafka_test1"); err != nil {
				println(err)
			}
		}
	} else {
		kafkaClient := new(kafkaclient.KafkaConsumer)
		if err := kafkaClient.Run(broker, group, topics, sigchan); err != nil {
			println(err)
		}
	}

	println("finish")
}
