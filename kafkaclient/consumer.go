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
	workersCh     chan int
}

const (
	commitTryCount           = 3
	commitedMessageCount     = 100
	commitMessageDefaultTime = 5
)

func (k *KafkaConsumer) createConsumer(broker, group string, numOfWorker int) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       broker,
		"group.id":                group,
		"session.timeout.ms":      6000,
		"heartbeat.interval.ms":   3000,
		"socket.keepalive.enable": true,
		"enable.auto.commit":      false,
		"default.topic.config":    kafka.ConfigMap{"auto.offset.reset": "earliest"}})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("Consumer %v Created \n", c)

	k.consumer = c
	k.workersCh = make(chan int, numOfWorker)
}

func (k *KafkaConsumer) commit(tryCount int) ([]kafka.TopicPartition, error) {
	commitedOffsets, err := k.consumer.Commit()
	if err != nil {
		if tryCount <= commitTryCount {
			tryCount++
			k.commit(tryCount)
		}

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
					k.commit(0)
				}
			case <-time.After(commitMessageDefaultTime * time.Second):
				if k.readedOffsets > 0 {
					message := fmt.Sprintf("%v message commited.\n", k.readedOffsets)
					k.commit(0)
					fmt.Printf(message)
				}
			case <-quitChan:
				fmt.Println("Signal Close")
				if k.readedOffsets > 0 {
					message := fmt.Sprintf("%v message commited.\n", k.readedOffsets)
					k.commit(0)
					fmt.Printf(message)
				}
				return
			}
		}
	}()

	return addChan, quitChan
}

func (k *KafkaConsumer) Run(broker, group string, topics []string, numOfWorker int, sigchan chan os.Signal) error {
	k.createConsumer(broker, group, numOfWorker)
	c := k.consumer

	if err := c.SubscribeTopics(topics, nil); err != nil {
		return fmt.Errorf("error subscribe to topic")
	}

	addChan, quitChan := k.bgProcess()
	var wgroup sync.WaitGroup
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
				wgroup.Add(1)
				go func() { addChan <- true }()
				k.workersCh <- 1

				fmt.Printf("Consume message from partition : %v, offset : %v \n", e.TopicPartition.Partition, e.TopicPartition.Offset)
				go func() {
					k.DoSomething(e.TopicPartition.Partition, int64(e.TopicPartition.Offset))
					<-k.workersCh
					wgroup.Done()
				}()

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

	fmt.Println("waiting for all process done")
	wgroup.Wait()
	fmt.Println("all process done")

	return nil
}

func (k *KafkaConsumer) DoSomething(partition int32, offset int64) {
	minDelay := 2
	maxDelay := 10
	randomSleep := int32(minDelay) + rand.Int31n(int32(maxDelay)-int32(minDelay))
	time.Sleep(time.Duration(randomSleep) * time.Second)

	fmt.Printf("message from partition: %v, offset: %v finish\n", partition, offset)
}
