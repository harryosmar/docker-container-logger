package processor

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/harryosmar/docker-container-logger/pkg/logging"
	"github.com/harryosmar/docker-container-logger/pkg/metrics"
	"github.com/harryosmar/docker-container-logger/pkg/models"
	"go.uber.org/zap"
)

// logEntryPool is a pool of LogEntry objects to reduce GC pressure
var logEntryPool = sync.Pool{
	New: func() interface{} { return &models.LogEntry{} },
}

// LogProcessor processes container log entries
type LogProcessor struct {
	logger     *zap.Logger
	metrics    *metrics.Metrics
	fileLogger *logging.FileLogger
	format     string
	dropOnFull bool
}

// NewLogProcessor creates a new log processor
func NewLogProcessor(
	logger *zap.Logger,
	metrics *metrics.Metrics,
	fileLogger *logging.FileLogger,
	format string,
	dropOnFull bool,
) *LogProcessor {
	return &LogProcessor{
		logger:     logger,
		metrics:    metrics,
		fileLogger: fileLogger,
		format:     format,
		dropOnFull: dropOnFull,
	}
}

// ProcessLine processes a log line from a container
func (p *LogProcessor) ProcessLine(raw string, meta models.ContainerMeta) {
	entry := p.marshalEntry(raw, meta)
	success := p.fileLogger.Write(entry, p.dropOnFull)
	
	if success {
		p.metrics.IncLinesProcessed()
	} else {
		p.metrics.IncLinesDropped()
	}
	
	// Update queue length metric
	p.metrics.UpdateQueueLength(p.fileLogger.GetQueueLength())
}

// marshalEntry converts a raw log line to a structured log entry
func (p *LogProcessor) marshalEntry(raw string, meta models.ContainerMeta) []byte {
	// Skip empty lines
	if len(strings.TrimSpace(raw)) == 0 {
		p.logger.Debug("Skipping empty log line")
		return nil
	}

	// Parse timestamp and line
	parts := strings.SplitN(raw, " ", 2)
	ts, line := parts[0], raw
	if len(parts) == 2 {
		line = parts[1]
	}
	
	// Determine log source (stdout/stderr)
	source := "stdout"
	if strings.Contains(raw, " stderr ") {
		source = "stderr"
	}
	
	// Get a LogEntry from the pool
	logEntryObj := logEntryPool.Get()
	entry := logEntryObj.(*models.LogEntry)
	
	// Reset the LogEntry to avoid data leakage
	*entry = models.LogEntry{}
	
	// Set the entry fields
	entry.ContainerID = meta.ID
	entry.ContainerName = meta.Name
	entry.Labels = meta.Labels
	entry.Timestamp = ts
	entry.Source = source
	entry.Line = line
	
	var result []byte
	var err error
	
	if p.format == "json" {
		// For schema-based format, just marshal the log entry itself
		// The FileLogger will handle adding it to the schema document
		result, err = json.Marshal(entry)
		if err != nil {
			p.logger.Error("Failed to marshal log entry", zap.Error(err))
			// Return the entry to the pool
			logEntryPool.Put(logEntryObj)
			// Return a fallback simple JSON with error info
			fallback, _ := json.Marshal(map[string]string{
				"error":    "marshal_failed",
				"raw_line": raw,
			})
			return fallback
		}
	} else {
		// Plain text format
		result = []byte(fmt.Sprintf("%s [%s] [%s] %s", 
			ts, meta.Name, source, line))
	}
	
	// Return the entry to the pool
	logEntryPool.Put(logEntryObj)
	
	return result
}
