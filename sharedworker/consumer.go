package sharedworker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"bitbucket.org/kudoindonesia/frontier_biller_sdk/log"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaConsumer is an interface implementation to run kafka consumer
type KafkaConsumer interface {
	Run(signalChannel chan bool)
}

const (
	// sessionTimeoutDefault must within group.max.session.timeout.ms(default 30s)
	// and group.min.session.timeout.ms(default 6s)
	sessionTimeoutDefault           = 6000
	heartbeatIntervalDefault        = 1000
	logConnectionClose              = false
	defaultAutoCommit               = false
	socketKeepAlive                 = true
	defaultCommitedMessageCount     = 1
	defaultCommitMessageDefaultTime = 5 * time.Second

	consumerOperationCategory = "KafkaConsumer"
)

var messageCH chan Message
var terminationCh chan bool

// MessageHandler is a contract of consumer handler that will be ran by SDK
type MessageHandler func([]byte)

type consumers struct {
	logger         log.Logger
	numberOfWorker int
	consumers      []*consumer
	handler        MessageHandler
}

// Consumer will keep all information about Kafka Consumer like topic and it's friend
type consumer struct {
	logger                log.Logger
	consumer              *kafka.Consumer
	commitMessageDuration time.Duration
	readedOffsets         int
	commitMessageCount    int
	tryCount              int
}

// NewConsumer instance initialize of consumer
func NewConsumer(config ConsumerConfig) (KafkaConsumer, error) {
	if config.NumberOfConsumer <= 0 {
		config.NumberOfConsumer = 1
	}

	if config.NumberOfWorker <= 0 {
		config.NumberOfWorker = 1
	}

	if config.MaxCommitTryCount <= 0 {
		config.MaxCommitTryCount = 3
	}

	if config.NumberOfMessageToCommit <= 0 {
		config.NumberOfMessageToCommit = defaultCommitedMessageCount
	}

	if config.CommitMessageDuration <= 0 {
		config.CommitMessageDuration = defaultCommitMessageDefaultTime
	}

	consumers := &consumers{
		logger:         config.Logger,
		numberOfWorker: config.NumberOfWorker,
		handler:        config.Handler,
	}

	for i := 0; i < config.NumberOfConsumer; i++ {
		subscribedConsumer, err := openBroker(config.Server, config.Topic)
		if err != nil {
			config.Logger.Error("failed to create kafka consumer", consumerOperationCategory, nil, err)
			return nil, err
		}

		kafkaConsumer := &consumer{
			logger:                config.Logger,
			consumer:              subscribedConsumer,
			tryCount:              config.MaxCommitTryCount,
			commitMessageCount:    config.NumberOfMessageToCommit,
			commitMessageDuration: config.CommitMessageDuration,
		}

		consumers.consumers = append(consumers.consumers, kafkaConsumer)
	}

	messageCH = make(chan Message, config.NumberOfWorker)
	terminationCh = make(chan bool)

	return consumers, nil
}

// openBroker will return new instance of Kafka Consumer
func openBroker(server string, topic string) (*kafka.Consumer, error) {
	// create new kafka consumer
	cons, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       server,
		"group.id":                fmt.Sprintf("cgr.%s", topic),
		"session.timeout.ms":      sessionTimeoutDefault,
		"heartbeat.interval.ms":   heartbeatIntervalDefault,
		"socket.keepalive.enable": socketKeepAlive,
		"enable.auto.commit":      defaultAutoCommit,
		"default.topic.config":    kafka.ConfigMap{"auto.offset.reset": "earliest"},
		"log.connection.close":    logConnectionClose,
	})

	if err != nil {
		return nil, err
	}

	// subscribe to spesific topic
	if err = cons.Subscribe(topic, nil); err != nil {
		return nil, err
	}

	return cons, err
}

// commit will commit message to kafka
// if first commit failed, will retry until maxtryCount
// while reach maxtryCount, this function will return an error
func (c *consumer) commit(tryCount int) error {
	if _, err := c.consumer.Commit(); err != nil {
		if tryCount < c.tryCount {
			tryCount++
			return c.commit(tryCount)
		}

		c.readedOffsets = 0
		return err
	}

	c.readedOffsets = 0

	return nil
}

func (c *consumer) commitbg() (chan bool, chan bool) {
	addChan := make(chan bool)
	quitChan := make(chan bool)

	go func() {
		for {
			select {
			case <-addChan:
				c.readedOffsets++
				if c.readedOffsets == c.commitMessageCount {
					// commit message after finish by handler
					if err := c.commit(0); err != nil {
						c.logger.Error(fmt.Sprintf("error while commiting message"), consumerOperationCategory, nil, err)
					}
				}
			case <-time.After(c.commitMessageDuration):
				c.commitIfHaveMessage()
			case <-quitChan:
				c.commitIfHaveMessage()
				return
			}
		}
	}()

	return addChan, quitChan
}

func (c *consumer) commitIfHaveMessage() {
	if c.readedOffsets > 0 {
		// commit message after finish by handler
		if err := c.commit(0); err != nil {
			c.logger.Error(fmt.Sprintf("error while commiting message"), consumerOperationCategory, nil, err)
		}
	}
}

// Run will run kafka consumer
func (cs *consumers) Run(signalChannel chan bool) {
	var consumerWaiter sync.WaitGroup
	var workerWaiter sync.WaitGroup
	consumerCtx, cancel := context.WithCancel(context.Background())

	workerWaiter.Add(1)
	cs.startWorker(consumerCtx, &workerWaiter)

	for key, consumer := range cs.consumers {
		consumerWaiter.Add(1)
		go consumer.messageProcessor(consumerCtx, key, &consumerWaiter)
	}

	go func() {
		<-signalChannel
		cs.logger.Warning("cought terminating signal - waiting for all process done", consumerOperationCategory, nil, nil)
		cancel()
	}()

	consumerWaiter.Wait()
	terminationCh <- true
	workerWaiter.Wait()
	cs.logger.Info("all process done", consumerOperationCategory, nil, nil)
}

func (cs *consumers) startWorker(ctx context.Context, wg *sync.WaitGroup) {
	workerConfig := Config{
		Context:   ctx,
		NumWorker: cs.numberOfWorker,
		Handler:   cs.handler,
	}

	messagerWorker := ConsumerWorker{
		Config: workerConfig,
		Logger: cs.logger,
	}
	go messagerWorker.Start(wg)
}

// messageProcessor will handle message from kafka
// and sent message to handler to process the message
func (c *consumer) messageProcessor(ctx context.Context, ID int, wg *sync.WaitGroup) {
	defer wg.Done()

	addChan, quitChan := c.commitbg()
kafkaConsumer:
	for {
		select {
		case <-ctx.Done():
			quitChan <- true
			break kafkaConsumer

		default:
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				messageCH <- Message{e.Value}
				addChan <- true

			case kafka.PartitionEOF:
				fmt.Printf("[Consumer-%v] - reached %v", ID, e)

			case kafka.Error:
				message := fmt.Sprintf("[Consumer-%v] - error %v", ID, e)
				c.logger.Error(message, consumerOperationCategory, nil, e)
				break kafkaConsumer
			default:
				message := fmt.Sprintf("[Consumer-%v] - %v", ID, e)
				c.logger.Warning(message, consumerOperationCategory, nil, nil)
			}
		}
	}

	err := c.consumer.Close()
	if err != nil {
		c.logger.Warning(fmt.Sprintf("[Consumer-%v] -  failed to close current consumer", ID), consumerOperationCategory, nil, nil)
	}
	message := fmt.Sprintf("[Consumer-%v] -  successfully terminated", ID)
	c.logger.Warning(message, consumerOperationCategory, nil, nil)
}
