package main

import (
	"log"
	"sync"
)

type INode interface {
	add(in string, out string, transform func(string) string) error
	addReceiver(in string, receive func(string)) error
	start()
}

var nodes []*Node

func CreateNode() INode {
	node := Node{}
	nodes = append(nodes, &node)
	return &node
}

func StartNodes() {
	for _, node := range nodes {
		node.start()
	}
}

func waitForNodesToEnd() {
	waitGroup.Wait()
}

type Node struct {
	transformations []transformationInfo
	receivers       []receiveInfo
}

var waitGroup sync.WaitGroup

func (node *Node) subscribe(topic string) (chan string, error) {
	inChannel, err := broker.subscribeTopic(topic)
	if err != nil {
		log.Println("Topic " + topic + " does not exist: " + err.Error())
		return nil, err
	}
	return inChannel, nil
}

func (node *Node) add(in string, out string, transform func(string) string) error {
	inChannel, err := node.subscribe(in)
	if err != nil {
		return err
	}

	outChannel := broker.createTopic(out)
	t := transformationInfo{in + "-" + out, inChannel, outChannel, transform}

	node.transformations = append(node.transformations, t)

	return nil
}

func (node *Node) addReceiver(topic string, receive func(string)) error {
	channel, error := node.subscribe(topic)
	if error != nil {
		return error
	}

	r := receiveInfo{topic, channel, receive}
	node.receivers = append(node.receivers, r)

	return nil
}

func (node *Node) start() {
	log.Println("BEG")
	for _, t := range node.transformations {
		waitGroup.Add(1)
		go doTransformation(t)
	}
	for _, r := range node.receivers {
		waitGroup.Add(1)
		go doReceiver(r)
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

func doTransformation(t transformationInfo) {
	defer waitGroup.Done()
	log.Println("STT " + t.id)
	for {
		in, ok := <-t.in
		if !ok {
			log.Println("ENT " + t.id)
			close(t.out)
			return
		}
		out := t.transform(in)
		log.Println("MAP " + in + " -> " + out)
		t.out <- out
	}
}

func doReceiver(r receiveInfo) {
	defer waitGroup.Done()
	log.Println("STR " + r.id)
	for {
		in, ok := <-r.in
		if !ok {
			log.Println("ENR " + r.id)
			return
		}
		log.Println("REC " + in)
		r.function(in)
	}
}
