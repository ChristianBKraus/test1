package main

type Error struct {
	text string
}

func (error *Error) Error() string {
	return error.text
}
