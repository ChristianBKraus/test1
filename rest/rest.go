package rest

import (
	broker "jupiterpa/fin/core/broker"
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
	server.AddGet("/hello", hello)
	server.AddGet("/hello/:id", helloId)
	server.AddPost("/hellopost", helloPost)
	go server.Start()
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
