package node

import (
	"fmt"
	broker "jupiterpa/fin/core/broker"
	log "jupiterpa/fin/core/log"
	"sync"
)

type Node interface {
	Add(in string, out string, transform func(string) string) error
	AddReceiver(in string, receive func(string)) error
	Start()
}

func Create(id string) Node {
	newNode := node{id: id}
	instances = append(instances, &newNode)
	return &newNode
}

func Start() {
	for _, entry := range instances {
		entry.Start()
	}
}

func WaitForClose() {
	waitGroup.Wait()
}

type node struct {
	id              string
	transformations []transformationInfo
	receivers       []receiveInfo
}

var instances []*node
var waitGroup sync.WaitGroup

func (node *node) Subscribe(topic string) (chan string, error) {
	inChannel, err := broker.Get().SubscribeTopic(topic)
	if err != nil {
		return nil, err
	}
	return inChannel, nil
}

func (node *node) Add(in string, out string, transform func(string) string) error {
	inChannel, err := node.Subscribe(in)
	if err != nil {
		return err
	}

	outChannel := broker.Get().CreateTopic(out)
	t := transformationInfo{in + "-" + out, inChannel, outChannel, transform}

	node.transformations = append(node.transformations, t)

	log.Info(log.Setup, "ADT "+node.id+": "+in+"-"+out)

	return nil
}

func (node *node) AddReceiver(topic string, receive func(string)) error {
	channel, error := node.Subscribe(topic)
	if error != nil {
		return error
	}

	r := receiveInfo{topic, channel, receive}
	node.receivers = append(node.receivers, r)

	log.Info(log.Setup, "ADR "+node.id+": "+topic)

	return nil
}

func (node *node) Start() {
	log.Info(log.StartStop, "STN "+node.id)
	for _, t := range node.transformations {
		waitGroup.Add(1)
		go doTransformation(node.id, t)
	}
	for _, r := range node.receivers {
		waitGroup.Add(1)
		go doReceiver(node.id, r)
	}
}

type transformationInfo struct {
	id        string
	in        chan string
	out       chan string
	transform func(string) string
}

type receiveInfo struct {
	id       string
	in       chan string
	function func(string)
}

func doTransformation(nodeId string, t transformationInfo) {
	defer waitGroup.Done()
	log.Info(log.StartStop, "STT "+nodeId+": "+t.id)
	for {
		in, ok := <-t.in
		if !ok {
			log.Info(log.StartStop, "ENT "+nodeId+": "+t.id)
			close(t.out)
			return
		}
		out := t.transform(in)
		log.Info(log.Process, fmt.Sprintf("MAP %-10s -- %s -> %s", nodeId, in, out))
		t.out <- out
	}
}

func doReceiver(nodeId string, r receiveInfo) {
	defer waitGroup.Done()
	log.Info(log.StartStop, "STR "+nodeId+": "+r.id)
	for {
		in, ok := <-r.in
		if !ok {
			log.Info(log.StartStop, "ENR "+nodeId+": "+r.id)
			return
		}
		log.Info(log.Process, fmt.Sprintf("REC %-10s -> %s", nodeId, in))
		r.function(in)
	}
}
