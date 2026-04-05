// Module: Logger
// Phase: Foundation
// Purpose: Provides a small structured logger for core startup and runtime logs.
// Extended in later phases by richer fields, sinks, and tracing correlation.
package logger

import (
	"log/slog"
	"os"
	"strings"
)

// Logger wraps slog.Logger so node modules can share one logging API.
type Logger struct {
	base *slog.Logger
}

// New creates a logger configured with the given log level string.
func New(level string) *Logger {
	logLevel := parseLevel(level)
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	return &Logger{base: slog.New(handler)}
}

// Info logs informational events.
func (l *Logger) Info(msg string, args ...any) {
	l.base.Info(msg, args...)
}

// Error logs error events.
func (l *Logger) Error(msg string, args ...any) {
	l.base.Error(msg, args...)
}

// Warn logs warning events.
func (l *Logger) Warn(msg string, args ...any) {
	l.base.Warn(msg, args...)
}

// Debug logs debug events.
func (l *Logger) Debug(msg string, args ...any) {
	l.base.Debug(msg, args...)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
