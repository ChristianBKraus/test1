package rest

import (
	json "encoding/json"
	"fmt"
	broker "jupiterpa/fin/core/broker"
	data "jupiterpa/fin/core/data"
	log "jupiterpa/fin/core/log"

	gin "github.com/gin-gonic/gin"
)

type HttpNode interface {
	AddGet(path string, handler func(*gin.Context))
	AddPost(path string, handler func(*gin.Context))
	AddEndpoint(messageType string, structure any, topic string)
	Start()
	GetRegistry(typ string) Registry
}

func Get() HttpNode {
	if instance == nil {
		instance = &httpNode{
			Mapping: make(map[string]Registry),
		}
	}
	return instance
}

// -------------------------------------------------

var instance HttpNode

type httpNode struct {
	Routes  []route
	Mapping map[string]Registry
}

type route struct {
	Path    string
	Typ     string
	Handler func(*gin.Context)
}

type Registry struct {
	Structure any
	Topic     string
}

func (h *httpNode) GetRegistry(typ string) Registry {
	return h.Mapping[typ]
}

func (h *httpNode) AddGet(path string, handler func(*gin.Context)) {
	entry := route{Path: path, Typ: "get", Handler: handler}
	h.Routes = append(h.Routes, entry)
}
func (h *httpNode) AddPost(path string, handler func(*gin.Context)) {
	entry := route{Path: path, Typ: "post", Handler: handler}
	h.Routes = append(h.Routes, entry)
}

func (h *httpNode) AddEndpoint(messageType string, message any, topic string) {
	h.AddPost("/message/"+messageType, handleEndpoint)
	h.Mapping[messageType] = Registry{Structure: message, Topic: topic}
}

func handleEndpoint(context *gin.Context) {

	var input data.Message
	if err := context.BindJSON(&input); err != nil {
		return
	}
	messageType := input.Header.Typ
	registry := instance.GetRegistry(messageType)

	payload := input.Body.Payload
	if err := json.Unmarshal([]byte(payload), registry.Structure); err != nil {
		return
	}
	input.Body.Object = &(registry.Structure)

	log.Info(log.Process, fmt.Sprintf("HTTP %s", input))

	broker.Get().Send(registry.Topic, input)
}

func (h *httpNode) Start() {
	log.Info(log.Setup, "Rest Server is starting")
	router := gin.Default()
	for _, entry := range h.Routes {
		switch entry.Typ {
		case "get":
			router.GET(entry.Path, entry.Handler)
		case "post":
			router.POST(entry.Path, entry.Handler)
		}
	}

	router.Run("localhost:8080")
	log.Info(log.Setup, "Rest Server stopped")
}
