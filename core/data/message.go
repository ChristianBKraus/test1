package data

import "fmt"

type Message struct {
	Header MessageHeader
	Body   MessageBody
}

type MessageHeader struct {
	Typ string
	Id  string
}

type MessageBody struct {
	Payload string
	Object  *any
}

// ----------------------------------------------------------------

func GetHeader(typ string) MessageHeader {
	var id string
	counter++
	id = fmt.Sprintf("%d", counter)
	return MessageHeader{
		Typ: typ,
		Id:  id,
	}
}

var counter int = 0

func (h MessageHeader) String() string {
	return "<" + h.Typ + "-" + h.Id + ">"
}
func (h Message) String() string {
	return "{ " + h.Header.String() + "/" + h.Body.Payload + " }"
}
