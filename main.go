package main

import (
	broker "jupiterpa/fin/core/broker"
	data "jupiterpa/fin/core/data"
	log "jupiterpa/fin/core/log"
	node "jupiterpa/fin/core/node"
	rest "jupiterpa/fin/core/rest"

	gin "github.com/gin-gonic/gin"
)

func topic1_2_topic2(in data.Message) data.Message {
	in.Header.Typ = TYP_B
	in.Body.Payload += "."
	return in
}

func topic2_rec(in data.Message) {
	in.Header.Typ = TYP_C
	in.Body.Payload += "."
}

const TOPIC_1 = "Topic_1"
const TOPIC_2 = "Topic_2"

const TYP_A = "A"
const TYP_B = "B"
const TYP_C = "C"

const NODE_1 = "Node_1"
const NODE_2 = "Node_2"

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

func hello(context *gin.Context) {
	log.Info(log.Process, context.Request.Method)
}
func helloId(context *gin.Context) {
	log.Info(log.Process, context.Request.RequestURI+": "+context.Param("id"))
}

type Content struct {
	Id1 string
	Id2 string
}

func (c Content) String() string {
	return c.Id1 + "/" + c.Id2
}

func helloPost(context *gin.Context) {
	var content Content

	if err := context.BindJSON(&content); err != nil {
		return
	}

	log.Info(log.Process, content.String())
}

func main() {
	//log.Get().Activate(log.Setup, log.Information)
	//log.Get().Activate(log.StartStop, log.Information)
	log.Get().Activate(log.Process, log.Information)

	// Start REST Server in seperate task
	server := rest.Get()
	server.AddGet("/hello", hello)
	server.AddGet("/hello/:id", helloId)
	server.AddPost("/hellopost", helloPost)
	go server.Start()

	// Test Messaging
	broker := setup()

	msg_1 := data.Message{
		Header: data.GetHeader(TYP_A),
		Body: data.MessageBody{
			Payload: "Test1"},
	}
	msg_2 := data.Message{
		Header: data.GetHeader(TYP_A),
		Body: data.MessageBody{
			Payload: "Test2"},
	}

	broker.Send(TOPIC_1, msg_1)
	broker.Send(TOPIC_1, msg_2)

	broker.Close()
	node.WaitForClose()

	// Pause main routine to allow for test of REST server
	node.WaitUnlimited()
}
