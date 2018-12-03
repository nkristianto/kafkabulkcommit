package kafkaclientchannel

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumers struct {
	consumers []*kafkaconsumer
}

type kafkaconsumer struct {
	readedOffsets int
	workersCh     chan int
	consumer      *kafka.Consumer
	topic         string
}

const (
	commitTryCount           = 3
	commitedMessageCount     = 100
	commitMessageDefaultTime = 5
)

func NewConsumer(broker string, group string, topic string, numOfConsumers int, numOfWorker int) Consumers {
	consumers := Consumers{}
	for i := 0; i < numOfConsumers; i++ {
		kafkaCsm := &kafkaconsumer{}
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

		kafkaCsm.topic = topic
		kafkaCsm.consumer = c
		kafkaCsm.workersCh = make(chan int, numOfWorker)

		consumers.consumers = append(consumers.consumers, kafkaCsm)
	}

	return consumers
}

func (k *kafkaconsumer) commit(tryCount int) ([]kafka.TopicPartition, error) {
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

func (k *kafkaconsumer) bgProcess() (chan bool, chan bool) {
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
				k.commitIfHaveMessage()
			case <-quitChan:
				fmt.Println("Signal Close")
				k.commitIfHaveMessage()
				return
			}
		}
	}()

	return addChan, quitChan
}

func (k *kafkaconsumer) commitIfHaveMessage() {
	if k.readedOffsets > 0 {
		message := fmt.Sprintf("%v message commited.\n", k.readedOffsets)
		k.commit(0)
		fmt.Printf(message)
	}
}

// Run wil run kafka consumer
func (c *Consumers) Run(sigchan chan os.Signal) error {
	var waitters sync.WaitGroup
	consumerCtx, cancel := context.WithCancel(context.Background())

	for key, consumer := range c.consumers {
		waitters.Add(1)
		go consumer.MessageProcessor(key, consumerCtx, &waitters, sigchan)
	}

	go func() {
		select {
		case <-sigchan:
			cancel()
			return
		}
	}()

	waitters.Wait()
	return nil
}

func (k *kafkaconsumer) MessageProcessor(ID int, ctx context.Context, wg *sync.WaitGroup, sigchan chan os.Signal) {
	defer wg.Done()
	if err := k.consumer.Subscribe(k.topic, nil); err != nil {
		println("ERROR")
		os.Exit(1)
	}

	addChan, quitChan := k.bgProcess()
	var wgroup sync.WaitGroup
kafkaConsumer:
	for {
		select {
		case <-ctx.Done():
			quitChan <- true
			break kafkaConsumer
		default:
			ev := k.consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				wgroup.Add(1)
				go func() { addChan <- true }()
				k.workersCh <- 1

				fmt.Printf("[%v]Consume message from partition : %v, offset : %v \n", ID, e.TopicPartition.Partition, e.TopicPartition.Offset)
				go func() {
					k.doSomething(e.TopicPartition.Partition, int64(e.TopicPartition.Offset))
					<-k.workersCh
					wgroup.Done()
				}()

			case kafka.PartitionEOF:
				fmt.Printf("[%v] Reached %v\n", ID, e)
			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Error: %v\n", e)
				break
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("[%v] closing consumer\n", ID)
	k.consumer.Close()

	fmt.Printf("[%v] waiting for all process done \n", ID)
	wgroup.Wait()
	fmt.Printf("[%v] all process done \n", ID)
}

func (k *kafkaconsumer) doSomething(partition int32, offset int64) {
	start := time.Now()
	minDelay := 0
	maxDelay := 5
	randomSleep := int32(minDelay) + rand.Int31n(int32(maxDelay)-int32(minDelay))
	time.Sleep(time.Duration(randomSleep) * time.Second)

	elapsed := time.Since(start)
	fmt.Printf("message from partition: %v, offset: %v finish in : %v second \n", partition, offset, elapsed.Seconds())
}
