package broker

import (
	"fmt"
	data "jupiterpa/fin/core/data"
	log "jupiterpa/fin/core/log"
	utility "jupiterpa/fin/core/utility"
	"sync"
)

type Broker interface {
	CreateTopic(topic string) chan data.Message
	CreateProducer(topic string) chan data.Message
	SubscribeTopic(topic string) (chan data.Message, error)
	Start()
	Send(topic string, value data.Message) error
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

// -----------------------------------------------------------------------------------------
type broker struct {
	topics    map[string]*topicInfo
	producers []string
	mutex     sync.Mutex
}

type topicInfo struct {
	id     string
	input  chan data.Message
	output []chan data.Message
}

var instance Broker

func (b *broker) CreateTopic(topic string) chan data.Message {
	channel := make(chan data.Message)

	info := topicInfo{id: topic, input: channel}

	b.mutex.Lock()
	b.topics[topic] = &info
	b.mutex.Unlock()

	log.Info(log.Setup, "ADD "+topic)

	return channel
}

func (b *broker) CreateProducer(topic string) chan data.Message {
	b.producers = append(b.producers, topic)
	return b.CreateTopic(topic)
}

func errorTopicNotExists(category log.LogCategory, topic string) *utility.Error {
	message := "Topic " + topic + " does not exist"
	log.Get().Log(category, message, log.Error)
	err := utility.NewError(message)
	return err
}

func (b *broker) SubscribeTopic(topic string) (chan data.Message, error) {
	log.Info(log.Setup, "ASB "+topic)

	topicInfo, ok := b.topics[topic]
	if !ok {
		return nil, errorTopicNotExists(log.Setup, topic)
	}

	channel := make(chan data.Message)
	topicInfo.output = append(b.topics[topic].output, channel)

	return channel, nil
}

func (b *broker) Start() {
	for _, topicInfo := range b.topics {
		go distribute(topicInfo.id, topicInfo.input, topicInfo.output)
	}
}

func (b *broker) Send(topic string, value data.Message) error {
	topicInfo, ok := b.topics[topic]
	if !ok {
		return errorTopicNotExists(log.Process, topic)
	}
	log.Info(log.Process, fmt.Sprintf("SND %-10s <- %s", topic, value))
	log.Info(log.Message, fmt.Sprintf("SND %-10s <- %s", topic, value))
	topicInfo.input <- value
	return nil
}

func (b *broker) Close() {
	for _, producer := range b.producers {
		log.Info(log.StartStop, "EPR "+producer)
		close(b.topics[producer].input)
	}
}

func distribute(topic string, inChannel chan data.Message, outChannels []chan data.Message) {
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
