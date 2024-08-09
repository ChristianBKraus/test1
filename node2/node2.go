package node2

import (
	data "jupiterpa/fin/core/data"
	node "jupiterpa/fin/core/node"
	node1 "jupiterpa/fin/node1"
)

const NODE_2 = "Node_2"
const TYP_C = "C"

func Setup() node.Node {
	node2 := node.Create(NODE_2)
	node2.AddReceiver(node1.TOPIC_2, topic2_rec)
	return node2
}

func topic2_rec(in data.Message) {
	in.Header.Typ = TYP_C
	in.Body.Payload += "."
}
