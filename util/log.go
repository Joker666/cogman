package util

import (
	"log"
	"os"
)

// LogLevel holds log level values
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

// LogLevel is levels of logs
const (
	LogLevelError LogLevel = iota
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
	LogLevelTrace
)

// Logger is the logs interface
type Logger interface {
	Error(msg string, err error, objects ...Object)
	Debug(msg string, objects ...Object)
	Info(msg string, objects ...Object)
	Warn(msg string, objects ...Object)
}

type noOpLogger struct{}

func (noOpLogger) Log(LogLevel, ...interface{}) {}

// NoOpLogger is no operation logger
var NoOpLogger = noOpLogger{}

// StdLogger holds a log
type StdLogger struct {
	lgr *log.Logger
}

// NewLogger returns a new standard logger
func NewLogger() StdLogger {
	return StdLogger{
		lgr: log.New(os.Stdout, "", log.LstdFlags),
	}
}

// Object is a basic key value object
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

// Error prints error logs
func (l StdLogger) Error(msg string, err error, objects ...Object) {
	l.log(LogLevelError, prepareEntry(msg, err, objects...))
}

// Warn prints warning logs
func (l StdLogger) Warn(msg string, objects ...Object) {
	l.log(LogLevelWarn, prepareEntry(msg, nil, objects...))
}

// Info prints info logs
func (l StdLogger) Info(msg string, objects ...Object) {
	l.log(LogLevelInfo, prepareEntry(msg, nil, objects...))
}

// Debug prints debug logs
func (l StdLogger) Debug(msg string, objects ...Object) {
	l.log(LogLevelDebug, prepareEntry(msg, nil, objects...))
}
