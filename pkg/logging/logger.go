package logging

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/harryosmar/docker-container-logger/pkg/filter"
	"github.com/harryosmar/docker-container-logger/pkg/models"
	"github.com/harryosmar/docker-container-logger/pkg/storage"
	"go.uber.org/zap"
)

// logEntryPool is a pool of LogEntry objects to reduce GC pressure
var logEntryPool = sync.Pool{
	New: func() interface{} { return &models.LogEntry{} },
}

// FileLogger manages log file operations
type FileLogger struct {
	currentFile     *os.File
	currentWriter   *bufio.Writer
	currentDate     string
	fileNamePattern string
	logDir          string
	queue           chan []byte
	shutdownChan    chan struct{}
	fileMutex       sync.Mutex
	logger          *zap.Logger

	// Batch processing configuration
	minBatchSize            int           // Minimum batch size
	maxBatchSize            int           // Maximum batch size
	currentBatchSize        int           // Current adaptive batch size
	batchSizeAdjustInterval time.Duration // How often to adjust batch size
	lastProcessingTime      time.Duration // Time taken to process last batch

	// Worker pool configuration
	workerPool chan struct{} // Semaphore for limiting concurrent workers
	maxWorkers int           // Maximum number of concurrent workers

	// Compression configuration
	compressLogs     bool // Whether to compress logs before upload
	compressionLevel int  // Compression level (1-9, higher = better compression)

	storageProvider storage.Provider
	logFilter       filter.Filter
	rolloverMinutes int
}

// NewFileLogger creates a new file logger
func NewFileLogger(writeChanSize int, storageProvider storage.Provider, logFilter filter.Filter, logger *zap.Logger, rolloverMinutes int, fileNamePattern string) *FileLogger {
	// Default to daily rollover if not specified
	if rolloverMinutes <= 0 {
		rolloverMinutes = 24 * 60 // daily
	}

	// Default to daily file pattern if not specified
	if fileNamePattern == "" {
		fileNamePattern = "2006-01-02" // daily
	}

	// Default batch processing settings
	minBatchSize := 50
	maxBatchSize := 500
	currentBatchSize := 100 // Start with a moderate batch size
	batchSizeAdjustInterval := 1 * time.Minute

	// Default worker pool settings
	maxWorkers := 4 // Default to 4 workers

	// Default compression settings
	compressLogs := false
	compressionLevel := 6 // Default gzip compression level

	return &FileLogger{
		queue:                   make(chan []byte, writeChanSize),
		shutdownChan:            make(chan struct{}),
		minBatchSize:            minBatchSize,
		maxBatchSize:            maxBatchSize,
		currentBatchSize:        currentBatchSize,
		batchSizeAdjustInterval: batchSizeAdjustInterval,
		maxWorkers:              maxWorkers,
		workerPool:              make(chan struct{}, maxWorkers),
		compressLogs:            compressLogs,
		compressionLevel:        compressionLevel,
		storageProvider:         storageProvider,
		logFilter:               logFilter,
		logger:                  logger,
		rolloverMinutes:         rolloverMinutes,
		fileNamePattern:         fileNamePattern,
	}
}

// Start initializes the file logger and starts the write loop
func (f *FileLogger) Start() error {
	err := f.initLogFile()
	if err != nil {
		return err
	}

	go f.writeLoop()
	go f.scheduleRollover()

	return nil
}

// Shutdown gracefully shuts down the file logger
func (f *FileLogger) Shutdown() {
	close(f.shutdownChan)

	// Flush and close writer
	f.fileMutex.Lock()
	f.currentWriter.Flush()
	f.currentFile.Close()
	f.fileMutex.Unlock()

	// Upload final log
	f.uploadToStorage(f.currentDate)
}

// Write writes a log entry to the file
func (f *FileLogger) Write(entry []byte, dropOnFull bool) bool {
	select {
	case f.queue <- entry:
		return true
	default:
		if !dropOnFull {
			// Block until we can write
			f.queue <- entry
			return true
		}
		return false
	}
}

// writeLoop processes log entries from the queue and writes them to the file
func (f *FileLogger) writeLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Create a ticker for periodic flushing of buffered entries
	flushInterval := time.NewTicker(5 * time.Second)
	defer flushInterval.Stop()

	// Create a ticker for adjusting batch size based on performance
	batchSizeAdjustTicker := time.NewTicker(f.batchSizeAdjustInterval)
	defer batchSizeAdjustTicker.Stop()

	// Preallocate the buffer with capacity for max batch size to reduce reallocations
	logBuffer := make([][]byte, 0, f.maxBatchSize)

	// Track batch processing metrics
	var batchStartTime time.Time
	var entriesProcessed int64
	var processingDuration time.Duration

	for {
		select {
		case entry := <-f.queue:
			processedEntry := entry
			shouldWrite := true

			// Apply filter if configured
			if f.logFilter != nil {
				logEntry := filter.LogEntry{Content: entry, Source: ""}
				processedLogEntry, shouldWrite, err := f.logFilter.Process(context.Background(), logEntry)
				if err != nil {
					f.logger.Error("Error processing log entry", zap.Error(err))
					shouldWrite = false
				} else if shouldWrite {
					processedEntry = processedLogEntry.Content
				}
			} else {
				processedEntry = entry
				shouldWrite = true
			}

			// Only add to buffer if the filter says we should
			if shouldWrite {
				logBuffer = append(logBuffer, processedEntry)

				// If buffer reaches current batch size, flush it
				if len(logBuffer) >= f.currentBatchSize {
					batchStartTime = time.Now()
					f.flushLogEntries(logBuffer)
					processingDuration = time.Since(batchStartTime)
					f.lastProcessingTime = processingDuration
					entriesProcessed += int64(len(logBuffer))

					f.logger.Debug("Flushed batch",
						zap.Int("size", len(logBuffer)),
						zap.Duration("duration", processingDuration),
						zap.Int("current_batch_size", f.currentBatchSize))

					logBuffer = logBuffer[:0] // Clear the buffer
				}
			} else {
				f.logger.Debug("Log entry filtered out", zap.Int("size", len(entry)))
			}
		case <-flushInterval.C:
			// Periodically flush buffered entries
			if len(logBuffer) > 0 {
				batchStartTime = time.Now()
				f.flushLogEntries(logBuffer)
				processingDuration = time.Since(batchStartTime)
				f.lastProcessingTime = processingDuration
				entriesProcessed += int64(len(logBuffer))
				logBuffer = logBuffer[:0] // Clear the buffer
			}
		case <-batchSizeAdjustTicker.C:
			// Adjust batch size based on performance metrics
			if entriesProcessed > 0 {
				// Calculate average processing time per entry
				avgProcessingTime := f.lastProcessingTime.Nanoseconds() / entriesProcessed

				// If processing is fast, increase batch size
				if avgProcessingTime < 500000 { // Less than 0.5ms per entry
					newBatchSize := f.currentBatchSize + 50
					if newBatchSize <= f.maxBatchSize {
						f.currentBatchSize = newBatchSize
						f.logger.Info("Increased batch size",
							zap.Int("new_size", f.currentBatchSize),
							zap.Int64("avg_processing_ns", avgProcessingTime))
					}
				} else if avgProcessingTime > 2000000 { // More than 2ms per entry
					// If processing is slow, decrease batch size
					newBatchSize := f.currentBatchSize - 25
					if newBatchSize >= f.minBatchSize {
						f.currentBatchSize = newBatchSize
						f.logger.Info("Decreased batch size",
							zap.Int("new_size", f.currentBatchSize),
							zap.Int64("avg_processing_ns", avgProcessingTime))
					}
				}

				// Reset counters
				entriesProcessed = 0
			}
		case <-ticker.C:
			f.fileMutex.Lock()
			f.currentWriter.Flush()
			f.fileMutex.Unlock()
		case <-f.shutdownChan:
			// Flush any remaining entries and exit
			if len(logBuffer) > 0 {
				f.flushLogEntries(logBuffer)
			}
			f.fileMutex.Lock()
			f.currentWriter.Flush()
			f.fileMutex.Unlock()
			return
		}
	}
}

// flushLogEntries writes a batch of log entries to the current log file
func (f *FileLogger) flushLogEntries(entries [][]byte) {
	if len(entries) == 0 {
		return
	}

	f.fileMutex.Lock()
	defer f.fileMutex.Unlock()

	// Ensure file is open
	if f.currentFile == nil || f.currentWriter == nil {
		f.logger.Error("Log file is not open")
		return
	}

	// For schema-based JSON format, we need to parse each entry and add to the schema document
	filePath := f.currentFile.Name()

	// Close the current file to ensure all data is flushed
	f.currentWriter.Flush()
	f.currentFile.Close()

	// Reopen the file for reading
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		f.logger.Error("Failed to reopen log file for reading", zap.Error(err))
		return
	}

	// Read the existing schema document
	var doc models.SchemaLogDocument
	fileInfo, err := file.Stat()
	if err != nil {
		f.logger.Error("Failed to get file info", zap.Error(err))
		file.Close()
		return
	}

	// Read and parse the existing document if the file has content
	if fileInfo.Size() > 0 {
		fileContent, err := io.ReadAll(file)
		if err != nil {
			f.logger.Error("Failed to read log file", zap.Error(err))
			file.Close()
			return
		}

		err = json.Unmarshal(fileContent, &doc)
		if err != nil {
			f.logger.Error("Failed to parse schema document", zap.Error(err))
			// If we can't parse the document, create a new one
			doc = models.SchemaLogDocument{
				Schema: f.logFilter.GetSchema(),
				Rows:   [][]interface{}{},
			}
		}
	} else {
		// Create a new schema document if the file is empty
		doc = models.SchemaLogDocument{
			Schema: f.logFilter.GetSchema(),
			Rows:   [][]interface{}{},
		}
	}

	// Close the file after reading
	file.Close()

	// Use a worker pool for parallel processing of entries
	var wg sync.WaitGroup
	processedEntriesChan := make(chan models.LogEntry, len(entries))
	arrayEntriesChan := make(chan []interface{}, len(entries))

	// Process entries in parallel using worker pool
	for _, entryBytes := range entries {
		if len(entryBytes) == 0 {
			continue
		}

		// Try to acquire a worker from the pool
		f.workerPool <- struct{}{} // Blocks if pool is full

		wg.Add(1)
		go func(data []byte) {
			defer wg.Done()
			defer func() { <-f.workerPool }() // Release worker back to pool

			// Try to determine if this is an array or object format
			var isArray bool
			for _, b := range data {
				// Skip whitespace
				if b == ' ' || b == '	' || b == '\n' || b == '\r' {
					continue
				}
				// Check if it starts with '['
				isArray = b == '['
				break
			}

			if isArray {
				// Handle array format (from LogWadugsFilter)
				// The array is already in the format expected by the schema document
				var arrayData []interface{}
				err := json.Unmarshal(data, &arrayData)
				if err != nil {
					f.logger.Error("Failed to parse array log entry", zap.Error(err))
					return
				}

				// Send the array to the array entries channel
				arrayEntriesChan <- arrayData
			} else {
				// Handle object format (from LogAllFilter)
				// Get a LogEntry from the pool
				logEntryObj := logEntryPool.Get()
				entry := logEntryObj.(*models.LogEntry)

				// Reset the LogEntry to avoid data leakage
				*entry = models.LogEntry{}

				// Parse the entry
				err := json.Unmarshal(data, entry)
				if err != nil {
					f.logger.Error("Failed to parse object log entry", zap.Error(err))
					logEntryPool.Put(logEntryObj) // Return to pool
					return
				}

				// Skip entries with missing critical fields
				if entry.ContainerID == "" || entry.ContainerName == "" || entry.Timestamp == "" {
					f.logger.Warn("Skipping log entry with missing fields",
						zap.String("container_id", entry.ContainerID),
						zap.String("container_name", entry.ContainerName),
						zap.String("timestamp", entry.Timestamp))
					logEntryPool.Put(logEntryObj) // Return to pool
					return
				}

				// Send valid entry to the channel
				processedEntriesChan <- *entry

				// Return the entry to the pool
				logEntryPool.Put(logEntryObj)
			}
		}(entryBytes)
	}

	// Close the channels when all workers are done
	go func() {
		wg.Wait()
		close(processedEntriesChan)
		close(arrayEntriesChan)
	}()

	// Collect processed entries and add them to the schema document
	for entry := range processedEntriesChan {
		doc.AddLogEntry(entry)
	}

	// Add array entries directly to the rows
	for arrayEntry := range arrayEntriesChan {
		doc.Rows = append(doc.Rows, arrayEntry)
	}

	// Marshal the document to JSON
	updatedContent, err := json.Marshal(doc)
	if err != nil {
		f.logger.Error("Failed to marshal schema document", zap.Error(err))
		return
	}

	// Open the file for writing (truncate it)
	f.currentFile, err = os.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		f.logger.Error("Failed to open file for writing", zap.Error(err))
		return
	}

	// Determine if we should compress the content
	if f.compressLogs {
		// Create a gzip writer with the configured compression level
		gzipWriter, err := gzip.NewWriterLevel(f.currentFile, f.compressionLevel)
		if err != nil {
			f.logger.Error("Failed to create gzip writer", zap.Error(err))
			f.currentFile.Close()
			return
		}
		defer gzipWriter.Close()

		// Write the compressed content
		_, err = gzipWriter.Write(updatedContent)
		if err != nil {
			f.logger.Error("Failed to write compressed schema document", zap.Error(err))
			return
		}

		// Flush and close the gzip writer
		if err = gzipWriter.Flush(); err != nil {
			f.logger.Error("Failed to flush gzip writer", zap.Error(err))
			return
		}
		if err = gzipWriter.Close(); err != nil {
			f.logger.Error("Failed to close gzip writer", zap.Error(err))
			return
		}
	} else {
		// Write the uncompressed content
		_, err = f.currentFile.Write(updatedContent)
		if err != nil {
			f.logger.Error("Failed to write schema document", zap.Error(err))
			return
		}
	}

	// Close the file after writing
	f.currentFile.Close()

	// Reopen the file for appending
	f.currentFile, err = os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		f.logger.Error("Failed to reopen log file for appending", zap.Error(err))
		return
	}
	f.currentWriter = bufio.NewWriter(f.currentFile)
	f.logger.Debug("Flushed log entries to file", zap.Int("count", len(entries)), zap.Int("valid_rows", len(doc.Rows)))
}

// initLogFile initializes the log file based on the configured pattern
func (f *FileLogger) initLogFile() error {
	now := time.Now()
	f.currentDate = now.Format(f.fileNamePattern)
	fileName := fmt.Sprintf("%s.json", f.currentDate)

	// Check if file exists and has content
	fileInfo, err := os.Stat(fileName)
	fileExists := err == nil && fileInfo.Size() > 0

	// Open file with appropriate flags
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	f.currentFile = file
	f.currentWriter = bufio.NewWriter(file)

	// If the file is new or empty, initialize it with a schema-based log document
	if !fileExists {
		// Create an empty schema log document with the schema from the filter
		emptyDoc := &models.SchemaLogDocument{
			Schema: f.logFilter.GetSchema(),
			Rows:   [][]interface{}{},
		}
		emptyContent, err := json.Marshal(emptyDoc)
		if err != nil {
			f.logger.Error("Failed to marshal empty schema log document", zap.Error(err))
			return err
		}

		// Write the empty document to the file
		_, err = f.currentWriter.Write(emptyContent)
		if err != nil {
			f.logger.Error("Failed to write empty schema log document", zap.Error(err))
			return err
		}
		f.currentWriter.Flush()
	}

	f.logger.Info("Initialized log file with schema-based format", zap.String("file", fileName), zap.String("pattern", f.fileNamePattern))
	return nil
}

// scheduleRollover schedules log file rollovers based on configuration
func (f *FileLogger) scheduleRollover() {
	for {
		now := time.Now()
		var nextRollover time.Time

		if f.rolloverMinutes >= 24*60 && f.rolloverMinutes%(24*60) == 0 {
			// Daily or multiple days rollover at midnight
			nextRollover = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
		} else {
			// Calculate next rollover based on minutes
			currentMinutes := now.Hour()*60 + now.Minute()
			nextRolloverMinutes := ((currentMinutes / f.rolloverMinutes) + 1) * f.rolloverMinutes

			// If next rollover is tomorrow, roll over at midnight
			if nextRolloverMinutes >= 24*60 {
				nextRollover = time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
			} else {
				nextHour := nextRolloverMinutes / 60
				nextMinute := nextRolloverMinutes % 60
				nextRollover = time.Date(now.Year(), now.Month(), now.Day(), nextHour, nextMinute, 0, 0, now.Location())
			}
		}

		duration := nextRollover.Sub(now)
		f.logger.Info("Scheduled next log rollover", zap.Time("at", nextRollover), zap.Duration("in", duration))

		timer := time.NewTimer(duration)
		select {
		case <-timer.C:
			f.rollover()
		case <-f.shutdownChan:
			timer.Stop()
			return
		}
	}
}

// rollover performs a log file rollover
func (f *FileLogger) rollover() {
	f.fileMutex.Lock()
	oldDate := f.currentDate

	f.currentWriter.Flush()
	f.currentFile.Close()
	err := f.initLogFile()
	f.fileMutex.Unlock()

	if err != nil {
		f.logger.Error("Failed to initialize new log file during rollover", zap.Error(err))
		return
	}

	// Upload in a separate goroutine to avoid blocking
	go func() {
		// Upload the file to storage
		f.uploadToStorage(oldDate)

		// Clear the local file after successful upload
		localPath := fmt.Sprintf("%s.json", oldDate)
		// Open the file with truncate flag to clear its contents
		file, err := os.OpenFile(localPath, os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			f.logger.Error("Failed to clear local file after upload", zap.Error(err), zap.String("path", localPath))
			return
		}
		// Initialize the file with an empty schema log document
		emptyDoc := f.logFilter.GetSchema()
		emptyContent, err := json.Marshal(emptyDoc)
		if err != nil {
			f.logger.Error("Failed to marshal empty schema log document", zap.Error(err))
		} else {
			_, err = file.Write(emptyContent)
			if err != nil {
				f.logger.Error("Failed to initialize cleared file with empty schema log document", zap.Error(err))
			}
		}
		file.Close()
		f.logger.Info("Cleared local file and initialized with empty schema log document", zap.String("path", localPath))
	}()
}

// uploadToStorage uploads a log file to cloud storage
func (f *FileLogger) uploadToStorage(date string) {
	localPath := fmt.Sprintf("%s.json", date)

	// Use the file name pattern for remote path, but ensure we're using the correct date format
	// This allows for aggregating logs into daily/monthly files based on the pattern
	remotePath := fmt.Sprintf("%s.json", date)

	f.logger.Info("Uploading log file to storage",
		zap.String("local", localPath),
		zap.String("remote", remotePath))

	// Add timeout to upload
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Use the filter's schema when uploading to storage
	err := f.storageProvider.UploadWithFilter(ctx, localPath, remotePath, f.logFilter)
	if err != nil {
		f.logger.Error("Failed to upload log file to storage",
			zap.Error(err),
			zap.String("local", localPath),
			zap.String("remote", remotePath))
		return
	}

	f.logger.Info("Successfully uploaded log file to storage",
		zap.String("local", localPath),
		zap.String("remote", remotePath))
}

// GetQueueLength returns the current length of the write queue
func (f *FileLogger) GetQueueLength() int {
	return len(f.queue)
}
