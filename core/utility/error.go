package utility

import log "jupiterpa/fin/core/log"

type Error struct {
	text string
}

func NewError(text string) *Error {
	log.Get().Log(log.Process, text, log.Error)
	return &Error{text: text}
}

func (error *Error) Error() string {
	return error.text
}
