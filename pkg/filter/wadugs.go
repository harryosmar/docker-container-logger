package filter

import (
	"context"
	"encoding/json"
	"go.uber.org/zap"
)

// LogWadugsFilter filters log entries for Wadugs API logs
// It ensures that logs have required fields: request_id, level, and time
type LogWadugsFilter struct {
	logger *zap.Logger
}

// WadugsSchema defines the schema for Wadugs API logs
var WadugsSchema = []string{"service_name", "source", "level", "time", "request_id", "session_id", "ip_address", "user_id", "environment", "version", "raw"}

// Process filters log entries based on required fields
// Returns the original entry and true if it should be kept, false if it should be dropped
func (l LogWadugsFilter) Process(ctx context.Context, entry LogEntry) (LogEntry, bool, error) {
	// Skip if entry is empty
	if len(entry.Content) == 0 {
		l.logger.Debug("Skipping empty log entry")
		return entry, false, nil
	}

	// Parse the log entry as an object
	var logEntryObj map[string]interface{}
	err := json.Unmarshal(entry.Content, &logEntryObj)
	if err != nil {
		l.logger.Debug("Failed to parse log entry as object", zap.Error(err), zap.String("content", string(entry.Content)))
		return entry, false, nil
	}

	// Check if the 'line' field exists and contains the JSON message
	line, hasLine := logEntryObj["line"]
	if !hasLine || line == nil {
		l.logger.Debug("Log entry missing 'line' field")
		return entry, false, nil
	}

	// Convert line to string if it's not already
	lineStr, ok := line.(string)
	if !ok {
		l.logger.Debug("'line' field is not a string", zap.Any("line_type", line))
		return entry, false, nil
	}

	// Parse the JSON from the line field
	var lineData map[string]interface{}
	err = json.Unmarshal([]byte(lineStr), &lineData)
	if err != nil {
		l.logger.Debug("Failed to parse JSON from 'line' field",
			zap.String("line", lineStr),
			zap.Error(err))
		return entry, false, nil
	}

	// Check for required fields in the line JSON
	requestID, hasRequestID := lineData["request_id"]
	sessionID, hasSessionID := lineData["session_id"]
	ipAddress, hasIpAddress := lineData["ip_address"]
	userID, hasUserID := lineData["user_id"]
	level, hasLevel := lineData["level"]
	time, hasTime := lineData["time"]
	environment, hasEnvironment := lineData["environment"]
	version, hasVersion := lineData["version"]
	//_, hasTobeLog := lineData["tobe_log"]
	_, _ = lineData["tobe_log"]

	// Filter out entries that don't have all required fields
	//if !hasRequestID || !hasLevel || !hasTime || !hasTobeLog {
	if !hasRequestID || !hasLevel || !hasTime {
		l.logger.Debug("Log entry missing required fields in line JSON",
			zap.Bool("has_request_id", hasRequestID),
			zap.Bool("has_level", hasLevel),
			zap.Bool("has_time", hasTime))
		return entry, false, nil
	}

	// Get service name from labels
	serviceName := ""
	if labels, hasLabels := logEntryObj["labels"]; hasLabels && labels != nil {
		if labelsMap, ok := labels.(map[string]interface{}); ok {
			if svcName, hasSvcName := labelsMap["com.docker.swarm.service.name"]; hasSvcName && svcName != nil {
				if svcNameStr, ok := svcName.(string); ok {
					serviceName = svcNameStr
				}
			}
		}
	}

	// Get source field
	source := ""
	if src, hasSource := logEntryObj["source"]; hasSource && src != nil {
		if srcStr, ok := src.(string); ok {
			source = srcStr
		}
	}

	// Convert requestID, level, and time to strings
	requestIDStr := ""
	if requestIDVal, ok := requestID.(string); ok {
		requestIDStr = requestIDVal
	}

	levelStr := ""
	if levelVal, ok := level.(string); ok {
		levelStr = levelVal
	}

	timeStr := ""
	if timeVal, ok := time.(string); ok {
		timeStr = timeVal
	}

	sessionIDStr := ""
	if hasSessionID {
		if sessionIDVal, ok := sessionID.(string); ok {
			sessionIDStr = sessionIDVal
		}
	}

	ipAddressStr := ""
	if hasIpAddress {
		if ipAddressVal, ok := ipAddress.(string); ok {
			ipAddressStr = ipAddressVal
		}
	}

	userIDStr := ""
	if hasUserID {
		if userIDVal, ok := userID.(string); ok {
			userIDStr = userIDVal
		}
	}

	environmentStr := ""
	if hasEnvironment {
		if environmentVal, ok := environment.(string); ok {
			environmentStr = environmentVal
		}
	}

	versionStr := ""
	if hasVersion {
		if versionVal, ok := version.(string); ok {
			versionStr = versionVal
		}
	}

	// Create the array format output
	outputArray := []interface{}{
		serviceName,    // service_name
		source,         // source
		levelStr,       // level
		timeStr,        // timestamp
		requestIDStr,   // request_id
		sessionIDStr,   // session_id
		ipAddressStr,   // ip_address
		userIDStr,      // user_id
		environmentStr, // env
		versionStr,     // version
		lineStr,        // raw JSON line
	}

	// Marshal the array to JSON
	outputJSON, err := json.Marshal(outputArray)
	if err != nil {
		l.logger.Error("Failed to marshal output array to JSON", zap.Error(err))
		return entry, false, err
	}

	// Create a new entry with the transformed content
	transformedEntry := LogEntry{
		Content: outputJSON,
		Source:  entry.Source,
	}

	// Entry has all required fields, keep it with the transformed format
	return transformedEntry, true, nil
}

// Initialize sets up the filter with configuration
func (l LogWadugsFilter) Initialize(ctx context.Context, config map[string]interface{}) error {
	l.logger.Info("Initializing Wadugs log filter")
	return nil
}

// GetSchema implements the Filter interface
// Returns the schema for Wadugs API logs
func (l LogWadugsFilter) GetSchema() []string {
	return WadugsSchema
}

// NewLogWadugsFilter creates a new Wadugs log filter
func NewLogWadugsFilter(logger *zap.Logger) *LogWadugsFilter {
	return &LogWadugsFilter{
		logger: logger,
	}
}
