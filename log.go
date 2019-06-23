package cogman

import (
	"log"
)

type LogLevel uint8

func (l LogLevel) String() string {
	switch l {
	case LogLevelError:
		return "error"
	case LogLevelWarn:
		return "warn"
	case LogLevelInfo:
		return "info"
	case LogLevelDebug:
		return "debug"
	case LogLevelTrace:
		return "trace"
	}
	return ""
}

const (
	LogLevelError LogLevel = iota
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
	LogLevelTrace
)

type Logger interface {
	Log(LogLevel, ...interface{})
}

type noOpLogger struct{}

func (noOpLogger) Log(LogLevel, ...interface{}) {}

var NoOpLogger = noOpLogger{}

type stdLogger struct {
	lgr *log.Logger
}

func (l stdLogger) Log(level LogLevel, v ...interface{}) {
	l.lgr.Println(append([]interface{}{level}, v...)...)
}

var StdLogger = stdLogger{lgr: &log.Logger{}}
