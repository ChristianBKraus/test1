package main

import (
	"log"
	"sync"
)

type IBroker interface {
	createTopic(topic string) chan string
	createProducer(topic string) chan string
	subscribeTopic(topic string) (chan string, error)
	send(topic string, value string) error
	close()
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

func (b *Broker) createTopic(topic string) chan string {
	channel := make(chan string)

	b.mutex.Lock()
	b.topics[topic] = channel
	b.mutex.Unlock()

	log.Println("ADD " + topic)

	return channel
}

func (b *Broker) createProducer(topic string) chan string {
	b.producers = append(b.producers, topic)
	return b.createTopic(topic)
}

func (b *Broker) subscribeTopic(topic string) (chan string, error) {
	log.Println("SUB " + topic)

	if b.topics[topic] == nil {
		return nil, &Error{"Topic does not exist"}
	}

	return b.topics[topic], nil
}

func (b *Broker) send(topic string, value string) error {
	channel := b.topics[topic]
	if channel == nil {
		return &Error{"Topic does not exist"}
	}
	channel <- value
	return nil
}

func (b *Broker) close() {
	for _, producer := range b.producers {
		close(b.topics[producer])
	}
	waitForNodesToEnd()
}
