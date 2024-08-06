package broker

import (
	"fmt"
	log "jupiterpa/fin/core/log"
	utility "jupiterpa/fin/core/utility"
	"sync"
)

type Broker interface {
	CreateTopic(topic string) chan string
	CreateProducer(topic string) chan string
	SubscribeTopic(topic string) (chan string, error)
	Start()
	Send(topic string, value string) error
	Close()
}

func Get() Broker {
	if instance == nil {
		newBroker := broker{}
		newBroker.topics = make(map[string]*topicInfo)
		instance = &newBroker
	}
	return instance
}

type broker struct {
	topics    map[string]*topicInfo
	producers []string
	mutex     sync.Mutex
}

type topicInfo struct {
	id     string
	input  chan string
	output []chan string
}

var instance Broker

func (b *broker) CreateTopic(topic string) chan string {
	channel := make(chan string)

	info := topicInfo{id: topic, input: channel}

	b.mutex.Lock()
	b.topics[topic] = &info
	b.mutex.Unlock()

	log.Info(log.Setup, "ADD "+topic)

	return channel
}

func (b *broker) CreateProducer(topic string) chan string {
	b.producers = append(b.producers, topic)
	return b.CreateTopic(topic)
}

func (b *broker) SubscribeTopic(topic string) (chan string, error) {
	log.Info(log.Setup, "ASB "+topic)

	topicInfo, ok := b.topics[topic]
	if !ok {
		return nil, utility.NewError("Topic does not exist")
	}

	channel := make(chan string)
	topicInfo.output = append(b.topics[topic].output, channel)

	return channel, nil
}

func (b *broker) Start() {
	for _, topicInfo := range b.topics {
		go distribute(topicInfo.id, topicInfo.input, topicInfo.output)
	}
}

func (b *broker) Send(topic string, value string) error {
	topicInfo, ok := b.topics[topic]
	if !ok {
		return utility.NewError("Topic does not exist")
	}
	log.Info(log.Process, fmt.Sprintf("SND %-10s <- %s", topic, value))
	topicInfo.input <- value
	return nil
}

func (b *broker) Close() {
	for _, producer := range b.producers {
		log.Info(log.StartStop, "EPR "+producer)
		close(b.topics[producer].input)
	}
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
