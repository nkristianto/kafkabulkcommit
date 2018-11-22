package kafkaclient

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConsumer struct {
	consumer      *kafka.Consumer
	readedOffsets int
	sync.RWMutex
}

const (
	commitedMessageCount     = 10
	commitMessageDefaultTime = 10
)

func (k *KafkaConsumer) createConsumer(broker, group string) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       broker,
		"group.id":                group,
		"session.timeout.ms":      6000,
		"heartbeat.interval.ms":   150,
		"socket.keepalive.enable": true,
		"enable.auto.commit":      false,
		"default.topic.config":    kafka.ConfigMap{"auto.offset.reset": "earliest"}})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	k.consumer = c
}

func (k *KafkaConsumer) commit() ([]kafka.TopicPartition, error) {
	commitedOffsets, err := k.consumer.Commit()
	if err != nil {
		return nil, err
	}

	k.readedOffsets = 0

	return commitedOffsets, nil
}

func (k *KafkaConsumer) bgProcess() (chan bool, chan bool) {
	addChan := make(chan bool)
	quitChan := make(chan bool)

	go func() {
		for {
			select {
			case <-addChan:
				k.readedOffsets++
				if k.readedOffsets == commitedMessageCount {
					k.commit()
					println("MESSAGE COMMITED")
				}
			case <-time.After(commitMessageDefaultTime * time.Second):
				if k.readedOffsets > 0 {
					fmt.Printf("Have %v uncommited message. Message will commit.\n", k.readedOffsets)
					k.commit()
					println("MESSAGE COMMITED")
				}
			case <-quitChan:
				fmt.Println("Signal Close")
				return
			}
		}
	}()

	return addChan, quitChan
}

func (k *KafkaConsumer) Run(broker, group string, topics []string, sigchan chan os.Signal) error {
	k.createConsumer(broker, group)
	c := k.consumer

	if err := c.SubscribeTopics(topics, nil); err != nil {
		return fmt.Errorf("error subscribe to topic")
	}

	addChan, quitChan := k.bgProcess()

kafkaConsumer:
	for {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			quitChan <- true
			close(addChan)
			close(quitChan)
			break kafkaConsumer
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				go k.DoSomething(int64(e.TopicPartition.Offset))
				go func() { addChan <- true }()

			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
				break
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()

	return nil
}

func (k *KafkaConsumer) DoSomething(offset int64) {
	minDelay := 2
	maxDelay := 10
	randomSleep := int32(minDelay) + rand.Int31n(int32(maxDelay)-int32(minDelay))
	time.Sleep(time.Duration(randomSleep) * time.Second)

	fmt.Printf("OFFSET %v FINISH\n", offset)
}
