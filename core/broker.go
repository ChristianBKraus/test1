package core

import (
	"log"
	"sync"
)

type IBroker interface {
	CreateTopic(topic string) chan string
	CreateProducer(topic string) chan string
	SubscribeTopic(topic string) (chan string, error)
	Send(topic string, value string) error
	Close()
}
type Broker struct {
	topics    map[string]chan string
	producers []string
	mutex     sync.Mutex
}

var broker IBroker

func GetBroker() IBroker {
	if broker == nil {
		newBroker := Broker{}
		newBroker.topics = make(map[string]chan string)
		broker = &newBroker
	}
	return broker
}

func (b *Broker) CreateTopic(topic string) chan string {
	channel := make(chan string)

	b.mutex.Lock()
	b.topics[topic] = channel
	b.mutex.Unlock()

	log.Println("ADD " + topic)

	return channel
}

func (b *Broker) CreateProducer(topic string) chan string {
	b.producers = append(b.producers, topic)
	return b.CreateTopic(topic)
}

func (b *Broker) SubscribeTopic(topic string) (chan string, error) {
	log.Println("SUB " + topic)

	if b.topics[topic] == nil {
		return nil, &Error{"Topic does not exist"}
	}

	return b.topics[topic], nil
}

func (b *Broker) Send(topic string, value string) error {
	channel := b.topics[topic]
	if channel == nil {
		return &Error{"Topic does not exist"}
	}
	log.Println("SND " + topic + " <- " + value)
	channel <- value
	return nil
}

func (b *Broker) Close() {
	for _, producer := range b.producers {
		close(b.topics[producer])
	}
	waitForNodesToEnd()
}
