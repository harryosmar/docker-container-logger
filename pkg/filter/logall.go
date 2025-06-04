package filter

import (
	"context"

	"go.uber.org/zap"
)

// LogAllFilter is a filter that accepts all log entries without modification
type LogAllFilter struct {
	logger *zap.Logger
}

// NewLogAllFilter creates a new LogAllFilter
func NewLogAllFilter(logger *zap.Logger) *LogAllFilter {
	return &LogAllFilter{
		logger: logger,
	}
}

// Process implements the Filter interface
// Always returns the original entry and true (accept)
func (f *LogAllFilter) Process(ctx context.Context, entry LogEntry) (LogEntry, bool, error) {
	// Simply pass through all log entries
	return entry, true, nil
}

// Initialize implements the Filter interface
func (f *LogAllFilter) Initialize(ctx context.Context, config map[string]interface{}) error {
	f.logger.Info("Initialized LogAllFilter")
	return nil
}

// GetSchema implements the Filter interface
// Returns the default schema for LogAllFilter
func (f *LogAllFilter) GetSchema() []string {
	return DefaultSchema
}
