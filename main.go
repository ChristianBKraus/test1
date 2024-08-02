package main

func topic1_2_topic2(in string) string {
	return in
}

func topic2_rec(in string) {

}

const TOPIC1 = "Topic_1"
const TOPIC2 = "Topic_2"

func main() {
	// setup
	broker := GetBroker()

	producer := broker.createTopic(TOPIC1)

	node1 := CreateNode()
	node1.add(TOPIC1, TOPIC2, topic1_2_topic2)

	node2 := CreateNode()
	node2.addReceiver(TOPIC2, topic2_rec)

	StartNodes()

	// run
	producer <- "Test"

	// terminate
	close(producer)
	waitForNodesToEnd()
}
