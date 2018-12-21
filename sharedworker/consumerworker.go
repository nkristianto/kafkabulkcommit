package sharedworker

import (
	"context"
	"fmt"
	"sync"

	"bitbucket.org/kudoindonesia/frontier_biller_sdk/log"
)

// Config will be used as a contract in declaring cwpahore configuration
// This Consumercwaphore will be using your channel
// Please close your channel when it won't be used anymore, for instance when your program is going to be shutting down
type Config struct {
	Context   context.Context
	NumWorker int
	Handler   func([]byte)
}

// Message is struct for message to process with worker
type Message struct {
	Value []byte
}

// ConsumerWorker will handle
type ConsumerWorker struct {
	Config
	Logger log.Logger
}

func (cw *ConsumerWorker) terminationMonitor() {
	<-terminationCh
	cw.Stop()
}

// Start will be giving a command to a dispatcher lead, and dispatcher lead will dispatcher team member
func (cw *ConsumerWorker) Start(wg *sync.WaitGroup) {
	go cw.Dispatcher(wg)
	cw.terminationMonitor()
	wg.Wait()
}

// Dispatcher will spawn number of worker that will work for
func (cw *ConsumerWorker) Dispatcher(wg *sync.WaitGroup) {
	var waitWorker sync.WaitGroup
	for id := 1; id <= cw.NumWorker; id++ {
		waitWorker.Add(1)
		go cw.Messenger(id, &waitWorker)
	}
	waitWorker.Wait()
	wg.Done()
}

// Messenger is a listener that will continuously work for a broadcasted task
func (cw *ConsumerWorker) Messenger(ID int, wg *sync.WaitGroup) {
	var handlerWg sync.WaitGroup
	for message := range messageCH {
		handlerWg.Add(1)
		cw.handleMessage(message)
		handlerWg.Done()
	}
	handlerWg.Wait()
	wg.Done()
}

func (cw *ConsumerWorker) handleMessage(msg Message) {
	defer cw.recoverProcess()
	cw.Handler(msg.Value)
}

func (cw *ConsumerWorker) recoverProcess() {
	if r := recover(); r != nil {
		cw.Logger.Error(fmt.Sprintf("recover message from %v", r), consumerOperationCategory, nil, fmt.Errorf("got panic"))
	}
}

// Stop will close a messaging pipe that we listen together
func (cw *ConsumerWorker) Stop() {
	close(messageCH)
}
