package consumerwithpool

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumers struct {
	consumers []*kafkaconsumer
}

type kafkaconsumer struct {
	readedOffsets  int
	workersCh      chan *kafka.Message
	consumer       *kafka.Consumer
	topic          string
	numberOFWorker int
	wg             sync.WaitGroup
}

const (
	commitTryCount           = 3
	commitedMessageCount     = 100
	commitMessageDefaultTime = 5
)

func NewConsumer(broker string, group string, topic string, numOfConsumers int, numOfWorker int) Consumers {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Printf("Alloc: %d kB, ", mem.Alloc/1024)
	fmt.Printf("Total Alloc: %d kB, ", mem.TotalAlloc/1024)
	fmt.Printf("HeapAlloc: %d kB, ", mem.HeapAlloc/1024)
	fmt.Printf("HeapSys: %d kB\n", mem.HeapSys/1024)
	fmt.Printf("Goroutine: %v \n", runtime.NumGoroutine())

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

		if err := c.Subscribe(topic, nil); err != nil {
			println("ERROR")
			os.Exit(1)
		}

		fmt.Printf("NUMBER OF WORKER = %v \n", numOfWorker)
		kafkaCsm.topic = topic
		kafkaCsm.consumer = c
		kafkaCsm.workersCh = make(chan *kafka.Message, numOfWorker)
		kafkaCsm.numberOFWorker = numOfWorker

		consumers.consumers = append(consumers.consumers, kafkaCsm)
	}

	return consumers
}

// commit will commit message to kafka
// if first commit failed, will retry until maxtryCount
// while reach maxtryCount, this function will return an error
func (k *kafkaconsumer) commit(tryCount int) error {
	//c.logger.Info("COMMIT MESSAGE", consumerOperationCategory, nil, nil)
	println("COMMIT MESSAGE")
	if _, err := k.consumer.Commit(); err != nil {
		if tryCount < 3 {
			tryCount++
			return k.commit(tryCount)
		}

		k.readedOffsets = 0
		return err
	}

	k.readedOffsets = 0

	return nil
}

func (k *kafkaconsumer) bgProcess() (chan bool, chan bool) {
	addChan := make(chan bool)
	quitChan := make(chan bool)

	go func() {
		for {
			select {
			case <-addChan:
				//c.logger.Info("add message to count", consumerOperationCategory, nil, nil)
				k.readedOffsets++
				if k.readedOffsets == 100 {
					//c.logger.Info("count exceeded, message will commit", consumerOperationCategory, nil, nil)
					// commit message after finish by handler
					if err := k.commit(0); err != nil {
						println("error while commiting message")
					}
				}
			case <-time.After(5 * time.Second):
				//c.logger.Info("check message to commit", consumerOperationCategory, nil, nil)
				k.commitIfHaveMessage()
			case <-quitChan:
				k.commitIfHaveMessage()
				return
			}
		}
	}()

	return addChan, quitChan
}

func (k *kafkaconsumer) commitIfHaveMessage() {
	if k.readedOffsets > 0 {
		//c.logger.Info("message will commit because time is exceeded", consumerOperationCategory, nil, nil)
		// commit message after finish by handler
		if err := k.commit(0); err != nil {
			println("error while commiting message")
		}
	}
}

// Run wil run kafka consumer
func (c *Consumers) Run(sigchan chan os.Signal) error {
	var waitters sync.WaitGroup
	consumerCtx, cancel := context.WithCancel(context.Background())

	for key, consumer := range c.consumers {
		waitters.Add(1)
		go consumer.MessageProcessor(consumerCtx, key, &waitters, sigchan)
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

func (k *kafkaconsumer) MessageProcessor(ctx context.Context, ID int, wg *sync.WaitGroup, sigchan chan os.Signal) {
	defer wg.Done()
	k.spawnMessager(ctx, ID, k.workersCh)

	addChan, quitChan := k.bgProcess()

	fmt.Printf("Goroutine: %v \n", runtime.NumGoroutine())
kafkaConsumer:
	for {
		select {
		case <-ctx.Done():
			break kafkaConsumer
		default:
			ev := k.consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:

				//fmt.Printf("[%v]Consume message from partition : %v, offset : %v \n", ID, e.TopicPartition.Partition, e.TopicPartition.Offset)

				k.wg.Add(1)
				k.workersCh <- e

				addChan <- true

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

	fmt.Printf("Goroutine: %v \n", runtime.NumGoroutine())

	fmt.Printf("[%v] waiting for all process done \n", ID)
	k.wg.Wait()
	fmt.Printf("[%v] close channel and commit message \n", ID)
	quitChan <- true
	fmt.Printf("[%v] closing consumer \n", ID)
	k.consumer.Close()
	fmt.Printf("[%v] shutting down all worker \n", ID)
	close(k.workersCh)
	fmt.Printf("[%v] all process done \n", ID)

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Printf("Alloc: %d kB, ", mem.Alloc/1024)
	fmt.Printf("Total Alloc: %d kB, ", mem.TotalAlloc/1024)
	fmt.Printf("HeapAlloc: %d kB, ", mem.HeapAlloc/1024)
	fmt.Printf("HeapSys: %d kB\n\n", mem.HeapSys/1024)
	fmt.Printf("Goroutine: %v \n", runtime.NumGoroutine())
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

func (k *kafkaconsumer) spawnMessager(ctx context.Context, id int, messages <-chan *kafka.Message) {
	for i := 0; i < k.numberOFWorker; i++ {
		go k.messager(ctx, id, messages)
		fmt.Printf("worker [%v] created \n", i)
	}
}

func (k *kafkaconsumer) messager(ctx context.Context, id int, messages <-chan *kafka.Message) {
	for msg := range messages {
		println("MESSAGE IN")
		k.doSomething(msg.TopicPartition.Partition, int64(msg.TopicPartition.Offset))
		k.wg.Done()
	}
}
