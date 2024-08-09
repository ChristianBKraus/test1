package node1

import (
	data "jupiterpa/fin/core/data"
	node "jupiterpa/fin/core/node"
	restNode "jupiterpa/fin/rest"
)

const NODE_1 = "Node_1"
const TYP_B = "B"
const TOPIC_2 = "Topic_2"

func Setup() node.Node {
	node1 := node.Create(NODE_1)
	node1.Add(restNode.TOPIC_1, TOPIC_2, topic1_2_topic2)
	return node1
}

func topic1_2_topic2(in data.Message) data.Message {
	in.Header.Typ = TYP_B
	in.Body.Payload += "."
	return in
}
