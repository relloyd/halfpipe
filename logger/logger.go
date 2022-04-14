package logger

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"

	log "github.com/sirupsen/logrus"
)

// Logger type is interface for available logging methods.
type Logger interface {
	Trace(...interface{})
	Debug(...interface{})
	Info(...interface{})
	Warn(...interface{})
	Error(...interface{})
	Panic(...interface{})
	Fatal(...interface{})
}

// LoggerImpl is a struct that extends sirupsen/logrus.
type LoggerImpl struct {
	Logger         *log.Entry
	Service        string
	LogLevelStr    string
	PrintStackDump bool
}

// NewLogger will create a new logger implementation.
func NewWebLogger(serviceName string, level string, stackDumpOnPanic bool, exitHandlerFn func()) *LoggerImpl {
	log.SetOutput(os.Stderr)
	log.RegisterExitHandler(exitHandlerFn)
	logLevel, err := log.ParseLevel(level)
	if err == nil {
		log.SetLevel(logLevel)
	} else {
		fmt.Println("Error setting up logging: ", err)
		os.Exit(1)
	}
	logger := log.WithFields(log.Fields{
		"service": serviceName,
	})
	return &LoggerImpl{Logger: logger, Service: serviceName, LogLevelStr: level, PrintStackDump: stackDumpOnPanic}
}

// NewLogger will create a new logger implementation.
func NewLogger(serviceName string, level string, stackDumpOnPanic bool) *LoggerImpl {
	log.SetOutput(os.Stderr)
	// log.SetFormatter(&log.JSONFormatter{})  // don't use JSON when running on CLI.
	logLevel, err := log.ParseLevel(level)
	if err == nil {
		log.SetLevel(logLevel)
	} else {
		fmt.Println("Error setting up logging: ", err)
		os.Exit(1)
	}
	logger := log.WithFields(log.Fields{
		"service": serviceName,
	})
	return &LoggerImpl{Logger: logger, Service: serviceName, LogLevelStr: level, PrintStackDump: stackDumpOnPanic}
}

// Debug log.
func (l *LoggerImpl) Trace(message ...interface{}) {
	l.Logger.Trace(message...)
}

// Debug log.
func (l *LoggerImpl) Debug(message ...interface{}) {
	l.Logger.Debug(message...)
}

// Info log.
func (l *LoggerImpl) Info(message ...interface{}) {
	l.Logger.Info(message...)
}

// Warn log.
func (l *LoggerImpl) Warn(message ...interface{}) {
	l.Logger.Warn(message...)
}

// Error (with stack trace in debug mode).
func (l *LoggerImpl) Error(message ...interface{}) {
	if l.LogLevelStr == "trace" || l.PrintStackDump {
		if l.PrintStackDump {
			l.Logger.WithField("stackTrace", fmt.Sprintf("%s", debug.Stack())).Error(message...)
		} else {
			l.Logger.Error(message)
		}
	} else {
		l.Logger.Error(message...)
	}
}

// Panic (with stack trace in debug mode, or if user explicitely sets PrintStackDump).
func (l *LoggerImpl) Panic(message ...interface{}) {
	if l.LogLevelStr == "debug" || l.LogLevelStr == "trace" {
		if l.PrintStackDump {
			l.Logger.WithField("stackTrace", fmt.Sprintf("%s", debug.Stack())).Panic(message...)
		} else {
			l.Logger.Fatal(message)
		}
	} else {
		if l.PrintStackDump { // if the user wants a stack dump without having to use debug|trace levels...
			l.Logger.Panic(message...)
		} else { // else log the message and quit without a stack dump...
			l.Logger.Fatal(message...)
		}
	}
}

// Fatal (with stack trace in debug mode).
// This causes exit(1) without a stack dump by default.
// Call Panic() to get a stack dump instead.
func (l *LoggerImpl) Fatal(message ...interface{}) {
	if l.LogLevelStr == "debug" || l.LogLevelStr == "trace" {
		l.Logger.WithField("stackTrace", fmt.Sprintf("%s", debug.Stack())).Fatal(message...)
	} else {
		l.Logger.Fatal(message...)
	}
}

// SetOutput will set the log output to the Writer supplied.
func (l *LoggerImpl) SetOutput(writer io.Writer) {
	log.SetOutput(writer)
}
