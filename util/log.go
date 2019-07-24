package util

import (
	"log"
	"os"
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
	Error(msg string, err error, objects ...Object)
	Debug(msg string, objects ...Object)
	Info(msg string, objects ...Object)
	Warn(msg string, objects ...Object)
}

type noOpLogger struct{}

func (noOpLogger) Log(LogLevel, ...interface{}) {}

var NoOpLogger = noOpLogger{}

type StdLogger struct {
	lgr *log.Logger
}

func NewLogger() StdLogger {
	return StdLogger{
		lgr: log.New(os.Stdout, "", log.LstdFlags),
	}
}

type Object struct {
	Key string
	Val interface{}
}

type entry map[string]interface{}

func prepareEntry(msg string, err error, objects ...Object) entry {
	v := map[string]interface{}{
		"message": msg,
	}
	if err != nil {
		v["error"] = err
	}
	for _, o := range objects {
		v[o.Key] = o.Val
	}
	return v
}

func (l StdLogger) log(level LogLevel, v ...interface{}) {
	l.lgr.Println(append([]interface{}{level}, v...)...)
}

func (s StdLogger) Error(msg string, err error, objects ...Object) {
	s.log(LogLevelError, prepareEntry(msg, err, objects...))
}

func (s StdLogger) Warn(msg string, objects ...Object) {
	s.log(LogLevelWarn, prepareEntry(msg, nil, objects...))
}

func (s StdLogger) Info(msg string, objects ...Object) {
	s.log(LogLevelInfo, prepareEntry(msg, nil, objects...))
}

func (s StdLogger) Debug(msg string, objects ...Object) {
	s.log(LogLevelDebug, prepareEntry(msg, nil, objects...))
}
