package rest

import (
	"fmt"
	broker "jupiterpa/fin/core/broker"
	data "jupiterpa/fin/core/data"
	log "jupiterpa/fin/core/log"
	rest "jupiterpa/fin/core/rest"

	gin "github.com/gin-gonic/gin"
)

const TOPIC_1 = "Topic_1"

const TYP_A = "A"

func Setup() {
	setupRestServer()
	setupProducer()
}

// --------------------------------------------------------

func setupProducer() {
	broker.Get().CreateProducer(TOPIC_1)
}

func setupRestServer() {
	server := rest.Get()
	addHello(server)
	addMessage(server)
	go server.Start()
}

// ------------------ Content ---------------------
type Content struct {
	Id1 string
	Id2 string
}

func (c Content) String() string {
	return c.Id1 + "/" + c.Id2
}

// ------------------- Message ------------------

func addMessage(server rest.HttpNode) {
	server.AddPost("/message/a", handleA)
}

func handleA(context *gin.Context) {
	var content Content
	if err := context.BindJSON(&content); err != nil {
		return
	}
	message := data.Message{
		Header: data.GetHeader(TYP_A),
		Body: data.MessageBody{
			Payload: fmt.Sprintf("%s", content)},
	}

	log.Info(log.Process, fmt.Sprintf("HTTP %s", message))

	broker.Get().Send(TOPIC_1, message)
}

// ------------------- Hello --------------------

func addHello(server rest.HttpNode) {
	server.AddGet("/hello", hello)
	server.AddGet("/hello/:id", helloId)
	server.AddPost("/hellopost", helloPost)
}

func hello(context *gin.Context) {
	log.Info(log.Process, context.Request.Method)
}
func helloId(context *gin.Context) {
	log.Info(log.Process, context.Request.RequestURI+": "+context.Param("id"))
}

func helloPost(context *gin.Context) {
	var content Content

	if err := context.BindJSON(&content); err != nil {
		return
	}

	log.Info(log.Process, content.String())
}
