package main

import (
	"jupiterpa/fin/core"
	"log"
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

func setup() core.IBroker {
	log.Println("Start Setup")
	broker := core.GetBroker()

	broker.CreateProducer(TOPIC_1)

	node1 := core.CreateNode(NODE_1)
	node1.Add(TOPIC_1, TOPIC_2, topic1_2_topic2)

	node2 := core.CreateNode(NODE_2)
	node2.AddReceiver(TOPIC_2, topic2_rec)

	core.StartNodes()

	log.Println("Finsh Setup")
	log.Println()

	return broker
}

func main() {

	broker := setup()

	broker.Send(TOPIC_1, "Test 1")
	broker.Send(TOPIC_1, "Test 2")

	broker.Close()
}
