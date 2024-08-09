package main

import (
	broker "jupiterpa/fin/core/broker"
	data "jupiterpa/fin/core/data"
	log "jupiterpa/fin/core/log"
	node "jupiterpa/fin/core/node"

	node1 "jupiterpa/fin/node1"
	node2 "jupiterpa/fin/node2"
	restNode "jupiterpa/fin/rest"
)

func setup() {
	log.Info(log.Setup, "Start Setup")

	restNode.Setup()
	node1.Setup()
	node2.Setup()

	broker := broker.Get()
	broker.Start()
	node.Start()

	log.Info(log.Setup, "Finish Setup")
	log.Info(log.Setup, "")

}

func send() {
	broker := broker.Get()

	msg_1 := data.Message{
		Header: data.GetHeader(restNode.TYP_A),
		Body: data.MessageBody{
			Payload: "Test1"},
	}
	msg_2 := data.Message{
		Header: data.GetHeader(restNode.TYP_A),
		Body: data.MessageBody{
			Payload: "Test2"},
	}

	broker.Send(restNode.TOPIC_1, msg_1)
	broker.Send(restNode.TOPIC_1, msg_2)
}

func close() {
	broker.Get().Close()
	node.WaitForClose()

	// Pause main routine to allow for test of REST server
	node.WaitUnlimited()
}

func main() {
	//log.Get().Activate(log.Setup, log.Information)
	//log.Get().Activate(log.StartStop, log.Information)
	log.Get().Activate(log.Process, log.Information)

	setup()

	send()

	close()

}
