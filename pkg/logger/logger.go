// =============================================================================
// Package: Logger
// File: pkg/logger/logger.go
// Responsible Member: All Members (Shared Utility)
// Purpose: Provides a structured logger for the distributed messaging system.
//          Each node creates a logger tagged with its ID so log output can
//          be easily filtered by node.
//
// Connections:
//   - Used by ALL modules for logging (consensus, replication, fault, etc.).
//   - Created in internal/node/node.go during initialization.
//   - Passed through the Node struct for consistent logging.
//
// Usage example:
//   log := logger.New("node1")
//   log.Info("starting election", "term", 5)
//   log.Error("replication failed", "peer", "node2", "error", err)
// =============================================================================
package logger

import (
	"log/slog"
	"os"
)

// Logger wraps the standard library's structured logger with node context.
type Logger struct {
	base *slog.Logger
}

// New creates a new structured logger tagged with the given node ID.
// All log entries from this logger will include a "node" field.
func New(nodeID string) *Logger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	base := slog.New(handler).With("node", nodeID)
	return &Logger{base: base}
}

// Info logs an informational message with optional key-value pairs.
func (l *Logger) Info(msg string, args ...any) {
	l.base.Info(msg, args...)
}

// Error logs an error message with optional key-value pairs.
func (l *Logger) Error(msg string, args ...any) {
	l.base.Error(msg, args...)
}

// Debug logs a debug message with optional key-value pairs.
func (l *Logger) Debug(msg string, args ...any) {
	l.base.Debug(msg, args...)
}

// Warn logs a warning message with optional key-value pairs.
func (l *Logger) Warn(msg string, args ...any) {
	l.base.Warn(msg, args...)
}
