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

type Node struct {
	id              string
	transformations []transformationInfo
	receivers       []receiveInfo
}

var nodes []*Node

func CreateNode(id string) INode {
	node := Node{id: id}
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

	log.Println("ADT " + node.id + ": " + in + "-" + out)

	return nil
}

func (node *Node) addReceiver(topic string, receive func(string)) error {
	channel, error := node.subscribe(topic)
	if error != nil {
		return error
	}

	r := receiveInfo{topic, channel, receive}
	node.receivers = append(node.receivers, r)

	log.Println("ADR " + node.id + ": " + topic)

	return nil
}

func (node *Node) start() {
	log.Println("BEG " + node.id)
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
	log.Println("STT " + nodeId + ": " + t.id)
	for {
		in, ok := <-t.in
		if !ok {
			log.Println("ENT " + nodeId + ": " + t.id)
			close(t.out)
			return
		}
		out := t.transform(in)
		log.Println("MAP " + nodeId + ": " + in + " -> " + out)
		t.out <- out
	}
}

func doReceiver(nodeId string, r receiveInfo) {
	defer waitGroup.Done()
	log.Println("STR " + nodeId + ": " + r.id)
	for {
		in, ok := <-r.in
		if !ok {
			log.Println("ENR " + nodeId + ": " + r.id)
			return
		}
		log.Println("REC " + nodeId + ": " + in)
		r.function(in)
	}
}
