/*
 * Copyright 2019, Automatic Inc.
 * All rights reserved.
 *
 * Author: Aniruddha Maru
 */

package hedwig

import (
	"context"
	"log"
)

type LoggingFields map[string]interface{}

// Logger represents an logging interface that this library expects
type Logger interface {
	// Error logs an error with a message. `fields` can be used as additional metadata for structured logging.
	// You can generally expect one of these fields to be available: message_sqs_id, message_sns_id.
	// By default fields are logged as a map using fmt.Sprintf
	Error(err error, message string, fields LoggingFields)

	// Warn logs a warn level log with a message. `fields` param works the same as `Error`.
	Warn(err error, message string, fields LoggingFields)

	// Info logs a debug level log with a message. `fields` param works the same as `Error`.
	Info(message string, fields LoggingFields)

	// Debug logs a debug level log with a message. `fields` param works the same as `Error`.
	Debug(message string, fields LoggingFields)
}

// fieldLogger represents an interface that logrus library's loggers satisfies
// TODO once Go modules are able to separate test dependencies from others, there should be a test that imports logrus
// Logger so our interface doesn't mismatch in the future.
type fieldLogger interface {
	// Add an error as single field (using the key defined in ErrorKey) to the Entry.
	WithError(err error) fieldLogger

	// Add a map of fields to the Entry.
	WithFields(fields LoggingFields) fieldLogger

	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
}

type logrusLogger struct {
	fieldLogger
}

func (l *logrusLogger) Error(err error, message string, fields LoggingFields) {
	l.WithError(err).WithFields(fields).Error(message)
}

func (l *logrusLogger) Warn(err error, message string, fields LoggingFields) {
	l.WithError(err).WithFields(fields).Warn(message)
}

func (l *logrusLogger) Info(message string, fields LoggingFields) {
	l.WithFields(fields).Info(message)
}

func (l *logrusLogger) Debug(message string, fields LoggingFields) {
	l.WithFields(fields).Debug(message)
}

func LogrusGetLoggerFunc(fn func(ctx context.Context) fieldLogger) GetLoggerFunc {
	return func(ctx context.Context) Logger {
		return &logrusLogger{fn(ctx)}
	}
}

type stdLogger struct{}

func (s *stdLogger) Error(err error, message string, fields LoggingFields) {
	log.Printf("[ERROR] %s [error: %+v][fields: %+v]\n", message, err, fields)
}

func (s *stdLogger) Warn(err error, message string, fields LoggingFields) {
	log.Printf("[WARN] %s [error: %+v][fields: %+v]\n", message, err, fields)
}

func (s *stdLogger) Info(message string, fields LoggingFields) {
	log.Printf("[INFO] %s [fields: %+v]\n", message, fields)
}

func (s *stdLogger) Debug(message string, fields LoggingFields) {
	log.Printf("[DEBUG] %s [fields: %+v]\n", message, fields)
}
