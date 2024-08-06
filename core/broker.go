package core

import (
	"fmt"
	log "jupiterpa/fin/core/log"
	"sync"
)

type IBroker interface {
	CreateTopic(topic string) chan string
	CreateProducer(topic string) chan string
	SubscribeTopic(topic string) (chan string, error)
	Start()
	Send(topic string, value string) error
	Close()
}
type Broker struct {
	topics    map[string]*topicInfo
	producers []string
	mutex     sync.Mutex
}

type topicInfo struct {
	id     string
	input  chan string
	output []chan string
}

var broker IBroker

func GetBroker() IBroker {
	if broker == nil {
		newBroker := Broker{}
		newBroker.topics = make(map[string]*topicInfo)
		broker = &newBroker
	}
	return broker
}

func (b *Broker) CreateTopic(topic string) chan string {
	channel := make(chan string)

	info := topicInfo{id: topic, input: channel}

	b.mutex.Lock()
	b.topics[topic] = &info
	b.mutex.Unlock()

	log.Info(log.Setup, "ADD "+topic)

	return channel
}

func (b *Broker) CreateProducer(topic string) chan string {
	b.producers = append(b.producers, topic)
	return b.CreateTopic(topic)
}

func (b *Broker) SubscribeTopic(topic string) (chan string, error) {
	log.Info(log.Setup, "ASB "+topic)

	topicInfo, ok := b.topics[topic]
	if !ok {
		return nil, &Error{"Topic does not exist"}
	}

	channel := make(chan string)
	topicInfo.output = append(b.topics[topic].output, channel)

	return channel, nil
}

func (b *Broker) Start() {
	for _, topicInfo := range b.topics {
		go distribute(topicInfo.id, topicInfo.input, topicInfo.output)
	}
}

func (b *Broker) Send(topic string, value string) error {
	topicInfo, ok := b.topics[topic]
	if !ok {
		return &Error{"Topic does not exist"}
	}
	log.Info(log.Process, fmt.Sprintf("SND %-10s <- %s", topic, value))
	topicInfo.input <- value
	return nil
}

func (b *Broker) Close() {
	for _, producer := range b.producers {
		log.Info(log.StartStop, "EPR "+producer)
		close(b.topics[producer].input)
	}
	waitForNodesToEnd()
}

func distribute(topic string, inChannel chan string, outChannels []chan string) {
	log.Info(log.StartStop, "SDS "+topic)
	for {
		value, ok := <-inChannel
		if !ok {
			log.Info(log.StartStop, "EDS "+topic)
			for _, outChannel := range outChannels {
				close(outChannel)
			}
			return
		}
		for _, outChannel := range outChannels {
			outChannel <- value
			log.Info(log.Process, fmt.Sprintf("DIS %-10s <> %s", topic, value))
		}
	}
}
