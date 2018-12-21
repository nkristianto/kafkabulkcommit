package sharedworker

import (
	"time"

	"bitbucket.org/kudoindonesia/frontier_biller_sdk/log"
)

// KafkaLog - this struct will be used for logging only
type KafkaLog struct {
	Topic     string `json:"topic,omitempty"`
	Partition string `json:"partition,omitempty"`
	Offset    string `json:"offset,omitempty"`
}

// ConsumerConfig is properties to running the consumer
type ConsumerConfig struct {
	// This ONLY support one topic for one consumer
	// for multiple topic, please create another consumer instead
	Topic string

	// Fill with kafka server address
	// for multiple server , use format like this :
	// "server1:9091,server2:9091"
	Server string

	// fill with logger to write log for consumer
	// todo: replace with koollog
	Logger log.Logger

	// Number of active consumer to running
	// default is 1
	NumberOfConsumer int

	// Number of concurrent worker running per consumer
	// worker act like a pipe,
	// if pipe is full, next message will waiting until
	// one of the pipe is empty
	// default is 1
	NumberOfWorker int

	// MaxCommitTryCount is number of maximum try count to commit message
	// if commiting process is faild, consumer will retry unil reach
	// default is 3
	MaxCommitTryCount int

	// NumberOfMessageToCommit will make consumer commit message after reach the number
	// i.g. if u set the number is 10, consumer will commit message to kafka after reach 10 message
	// default is 1
	NumberOfMessageToCommit int

	// CommitMessageDuration is duration to commit your message
	// consumer will commit message after reach NumberOfMessageToCommit
	// and CommitMessageDuration will commit message if it doesn't reach the number of NumberOfMessageToCommit
	// after some period
	CommitMessageDuration time.Duration

	// Handler is message handler to do message processing
	// every message will sent to this handler to process
	Handler MessageHandler
}
