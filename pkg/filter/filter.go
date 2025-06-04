package filter

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

// LogEntry represents a log entry to be filtered or transformed
type LogEntry struct {
	Content []byte
	Source  string
}

// Filter defines the interface for log filters and transformers
type Filter interface {
	// Process filters or transforms a log entry
	// Returns the processed log entry and a boolean indicating whether to write it
	// If the boolean is false, the log entry will be dropped
	Process(ctx context.Context, entry LogEntry) (LogEntry, bool, error)
	// Initialize sets up the filter
	Initialize(ctx context.Context, config map[string]interface{}) error
	// GetSchema returns the schema for this filter
	GetSchema() []string
}

// DefaultSchema is the default schema used by the LogAllFilter
var DefaultSchema = []string{"container_id", "container_name", "timestamp", "source", "line", "labels"}


// ErrUnsupportedFilterType is returned when an unsupported filter type is requested
type ErrUnsupportedFilterType string

func (e ErrUnsupportedFilterType) Error() string {
	return fmt.Sprintf("unsupported filter type: %s", string(e))
}

// CreateFilter creates a filter based on the given type and config
func CreateFilter(ctx context.Context, filterType string, config map[string]interface{}, logger *zap.Logger) (Filter, error) {
	switch filterType {
	case "", "logall":
		provider := NewLogAllFilter(logger)
		err := provider.Initialize(ctx, config)
		return provider, err
	case "wadugs":
		provider := NewLogWadugsFilter(logger)
		err := provider.Initialize(ctx, config)
		return provider, err
	// Add more filter types here as they are implemented
	default:
		return nil, ErrUnsupportedFilterType(filterType)
	}
}
