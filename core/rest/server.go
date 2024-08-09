package rest

import (
	log "jupiterpa/fin/core/log"

	gin "github.com/gin-gonic/gin"
)

type HttpNode interface {
	AddGet(path string, handler func(*gin.Context))
	AddPost(path string, handler func(*gin.Context))
	Start()
}

func Get() HttpNode {
	if instance == nil {
		instance = &httpNode{}
	}
	return instance
}

// -------------------------------------------------

var instance HttpNode

type httpNode struct {
	routes []route
}

type route struct {
	Path    string
	Typ     string
	Handler func(*gin.Context)
}

func (h *httpNode) AddGet(path string, handler func(*gin.Context)) {
	entry := route{Path: path, Typ: "get", Handler: handler}
	h.routes = append(h.routes, entry)
}
func (h *httpNode) AddPost(path string, handler func(*gin.Context)) {
	entry := route{Path: path, Typ: "post", Handler: handler}
	h.routes = append(h.routes, entry)
}

func (h *httpNode) Start() {
	log.Info(log.Setup, "Rest Server is starting")
	router := gin.Default()
	for _, entry := range h.routes {
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
