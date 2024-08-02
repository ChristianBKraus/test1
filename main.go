package main

import "log"

func topic1_2_topic2(in string) string {
	return in
}

func topic2_rec(in string) {

}

const TOPIC_1 = "Topic_1"
const TOPIC_2 = "Topic_2"

const NODE_1 = "Node 1"
const NODE_2 = "Node 2"

func setup() IBroker {
	log.Println("Start Setup")
	broker := GetBroker()

	broker.createProducer(TOPIC_1)

	node1 := CreateNode(NODE_1)
	node1.add(TOPIC_1, TOPIC_2, topic1_2_topic2)

	node2 := CreateNode(NODE_2)
	node2.addReceiver(TOPIC_2, topic2_rec)

	StartNodes()

	log.Println("Finsh Setup")
	log.Println()

	return broker
}

func main() {
	broker := setup()

	broker.send(TOPIC_1, "Test 1")
	broker.send(TOPIC_1, "Test 2")

	broker.close()
}
