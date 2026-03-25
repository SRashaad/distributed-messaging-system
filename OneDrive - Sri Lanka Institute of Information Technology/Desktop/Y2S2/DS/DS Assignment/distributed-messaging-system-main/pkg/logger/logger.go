// =============================================================================
// Package: Logger
// File: pkg/logger/logger.go
// Responsible Member: All Members (Shared Utility)
// Purpose: Provides a structured logger for the distributed messaging system.
//          Each node creates a logger tagged with its ID so log output can
//          be easily filtered by node. Uses Go's standard log/slog package
//          for structured key-value logging.
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

// Logger wraps the standard library's structured logger with node context.
type Logger struct {
	// TODO: Embed *slog.Logger for structured logging.
}

// New creates a new structured logger tagged with the given node ID.
// All log entries from this logger will include a "node" field.
//
// TODO: Implement:
//   1. Create a slog.TextHandler writing to os.Stdout with Debug level.
//   2. Create a slog.Logger with the handler.
//   3. Add a "node" attribute with the given nodeID.
//   4. Return the wrapped Logger.
func New(nodeID string) *Logger {
	return nil
}

// Info logs an informational message with optional key-value pairs.
//
// TODO: Delegate to the embedded slog.Logger.Info().
func (l *Logger) Info(msg string, args ...any) {
	// TODO: implement
}

// Error logs an error message with optional key-value pairs.
//
// TODO: Delegate to the embedded slog.Logger.Error().
func (l *Logger) Error(msg string, args ...any) {
	// TODO: implement
}

// Debug logs a debug message with optional key-value pairs.
//
// TODO: Delegate to the embedded slog.Logger.Debug().
func (l *Logger) Debug(msg string, args ...any) {
	// TODO: implement
}

// Warn logs a warning message with optional key-value pairs.
//
// TODO: Delegate to the embedded slog.Logger.Warn().
func (l *Logger) Warn(msg string, args ...any) {
	// TODO: implement
}
