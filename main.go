package main

import (
	broker "jupiterpa/fin/core/broker"
	log "jupiterpa/fin/core/log"
	node "jupiterpa/fin/core/node"
)

func topic1_2_topic2(in string) string {
	return in + "."
}

func topic2_rec(in string) {

}

const TOPIC_1 = "Topic_1"
const TOPIC_2 = "Topic_2"

const NODE_1 = "Node 1"
const NODE_2 = "Node 2"

func setup() broker.Broker {
	log.Info(log.Setup, "Start Setup")
	broker := broker.Get()

	broker.CreateProducer(TOPIC_1)

	node1 := node.Create(NODE_1)
	node1.Add(TOPIC_1, TOPIC_2, topic1_2_topic2)

	node2 := node.Create(NODE_2)
	node2.AddReceiver(TOPIC_2, topic2_rec)

	broker.Start()
	node.Start()

	log.Info(log.Setup, "Finish Setup")
	log.Info(log.Setup, "")

	return broker
}

func main() {
	//log.Get().Activate(log.Setup, log.Information)
	//log.Get().Activate(log.StartStop, log.Information)
	log.Get().Activate(log.Process, log.Information)

	broker := setup()

	broker.Send(TOPIC_1, "Test 1")
	broker.Send(TOPIC_1, "Test 2")

	broker.Close()
	node.WaitForClose()
}
