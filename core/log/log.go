package log

import "log"

type LogLevel int
type LogCategory int

const (
	Error LogLevel = iota + 1
	Warning
	Success
	Information
)

const (
	Setup LogCategory = iota + 1
	StartStop
	Process
)

type ILogger interface {
	Activate(category LogCategory, level LogLevel)
	Log(category LogCategory, message string, level LogLevel)
	Info(category LogCategory, message string)
	GetMessages(category LogCategory) []string
	GetAllMessages() []string
}

func Activate(category LogCategory, level LogLevel) {
	Get().Activate(category, level)
}
func Log(category LogCategory, message string, level LogLevel) {
	Get().Log(category, message, level)
}
func Info(category LogCategory, message string) {
	Get().Info(category, message)
}

func Get() ILogger {
	if logger == nil {
		newLogger := Logger{
			categories: make(map[LogCategory]LogLevel),
			logger:     log.Default(),
			messages:   make(map[LogCategory][]string),
		}
		logger = &newLogger
	}
	return logger
}

type Logger struct {
	categories map[LogCategory]LogLevel
	logger     *log.Logger
	messages   map[LogCategory][]string
}

var logger ILogger

func (l *Logger) Activate(category LogCategory, level LogLevel) {
	if level == 0 {
		level = Information
	}
	l.categories[category] = level
}
func (l *Logger) Log(category LogCategory, message string, level LogLevel) {
	if level == 0 {
		level = Information
	}
	if l.messages[category] == nil {
		l.messages[category] = make([]string, 0)
	}
	l.messages[category] = append(l.messages[category], message)
	loggingLevel, ok := l.categories[category]
	if ok && loggingLevel >= level {
		l.logger.Println(message)
	}
}
func (l *Logger) Info(category LogCategory, message string) {
	l.Log(category, message, Information)
}
func (l *Logger) GetMessages(category LogCategory) []string {
	return l.messages[category]
}
func (l *Logger) GetAllMessages() []string {
	var result []string
	for _, msgs := range l.messages {
		for _, msg := range msgs {
			result = append(result, msg)
		}
	}
	return result
}
