package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/harryosmar/docker-container-logger/pkg/filter"
	"github.com/harryosmar/docker-container-logger/pkg/models"
	"github.com/harryosmar/docker-container-logger/pkg/storage/aliyun"
	"github.com/harryosmar/docker-container-logger/pkg/storage/gcs"
	s3provider "github.com/harryosmar/docker-container-logger/pkg/storage/s3"
	"go.uber.org/zap"
)

// Reader defines the interface for reading logs
type Reader interface {
	// ReadLogs reads logs for a specific date
	ReadLogs(ctx context.Context, date string, logFilter filter.Filter) (*models.SchemaLogDocument, error)
}

// GetReader returns a reader for the given provider
func GetReader(provider Provider, logger *zap.Logger) Reader {
	// Check if provider implements Reader interface
	if reader, ok := provider.(Reader); ok {
		return reader
	}

	// Return a storage-specific reader based on the provider type
	switch p := provider.(type) {
	case *s3provider.Provider:
		return &S3Reader{provider: p, logger: logger}
	case *gcs.Provider:
		return &GCSReader{provider: p, logger: logger}
	case *aliyun.Provider:
		return &AliyunReader{provider: p, logger: logger}
	default:
		// Default to local file system reader for dummy provider
		return &DefaultReader{
			provider:  provider,
			logger:    logger,
			uploadDir: "uploads", // Default upload directory
		}
	}
}

// DefaultReader provides a default implementation for reading logs from local filesystem
type DefaultReader struct {
	provider  Provider
	logger    *zap.Logger
	uploadDir string
}

// ReadLogs reads logs for a specific date from the upload directory
func (r *DefaultReader) ReadLogs(ctx context.Context, date string, logFilter filter.Filter) (*models.SchemaLogDocument, error) {
	// Construct the file path based on the date
	filePath := filepath.Join(r.uploadDir, fmt.Sprintf("%s.json", date))

	r.logger.Info("Reading logs from local filesystem", zap.String("date", date), zap.String("path", filePath))

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		r.logger.Warn("Log file not found", zap.String("path", filePath))
		// Use the filter's schema instead of default schema
		return models.NewSchemaLogDocumentWithSchema(logFilter.GetSchema()), nil
	}

	// Read file content
	content, err := os.ReadFile(filePath)
	if err != nil {
		r.logger.Error("Failed to read log file", zap.Error(err), zap.String("path", filePath))
		return nil, fmt.Errorf("failed to read log file: %w", err)
	}

	// Parse content as schema-based log document
	var doc models.SchemaLogDocument
	err = json.Unmarshal(content, &doc)
	if err != nil {
		r.logger.Error("Failed to parse log file", zap.Error(err), zap.String("path", filePath))
		return nil, fmt.Errorf("failed to parse log file: %w", err)
	}

	return &doc, nil
}

// S3Reader provides implementation for reading logs from AWS S3
type S3Reader struct {
	provider *s3provider.Provider
	logger   *zap.Logger
}

// ReadLogs reads logs for a specific date from S3
func (r *S3Reader) ReadLogs(ctx context.Context, date string, logFilter filter.Filter) (*models.SchemaLogDocument, error) {
	// Get bucket name and folder path from provider
	bucketName := r.provider.GetBucketName()
	folderPath := r.provider.GetFolderPath()

	// Construct the remote path based on the date
	remotePath := fmt.Sprintf("%s.json", date)
	if folderPath != "" {
		remotePath = fmt.Sprintf("%s/%s", folderPath, date)
	}

	r.logger.Info("Reading logs from S3",
		zap.String("date", date),
		zap.String("bucket", bucketName),
		zap.String("key", remotePath))

	// Get S3 client
	client := r.provider.GetClient()
	if client == nil {
		return nil, fmt.Errorf("S3 client not initialized")
	}

	// Check if the object exists
	_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucketName,
		Key:    &remotePath,
	})

	if err != nil {
		// If object doesn't exist, return empty document with filter's schema
		if strings.Contains(err.Error(), "NotFound") {
			r.logger.Warn("Log file not found in S3",
				zap.String("bucket", bucketName),
				zap.String("key", remotePath))
			return models.NewSchemaLogDocumentWithSchema(logFilter.GetSchema()), nil
		}

		// Other error
		r.logger.Error("Failed to check if S3 object exists",
			zap.Error(err),
			zap.String("bucket", bucketName),
			zap.String("key", remotePath))
		return nil, fmt.Errorf("failed to check if S3 object exists: %w", err)
	}

	// Download the object
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucketName,
		Key:    &remotePath,
	})

	if err != nil {
		r.logger.Error("Failed to download S3 object",
			zap.Error(err),
			zap.String("bucket", bucketName),
			zap.String("key", remotePath))
		return nil, fmt.Errorf("failed to download S3 object: %w", err)
	}
	defer result.Body.Close()

	// Read all content
	content, err := io.ReadAll(result.Body)
	if err != nil {
		r.logger.Error("Failed to read S3 object content", zap.Error(err))
		return nil, fmt.Errorf("failed to read S3 object content: %w", err)
	}

	// Parse content as schema-based log document
	var doc models.SchemaLogDocument
	err = json.Unmarshal(content, &doc)
	if err != nil {
		r.logger.Error("Failed to parse S3 object as schema log document", zap.Error(err))
		return nil, fmt.Errorf("failed to parse S3 object as schema log document: %w", err)
	}

	return &doc, nil
}

// GCSReader provides implementation for reading logs from Google Cloud Storage
type GCSReader struct {
	provider *gcs.Provider
	logger   *zap.Logger
}

// ReadLogs reads logs for a specific date from GCS
func (r *GCSReader) ReadLogs(ctx context.Context, date string, logFilter filter.Filter) (*models.SchemaLogDocument, error) {
	// Get bucket name and folder path from provider
	bucketName := r.provider.GetBucketName()
	folderPath := r.provider.GetFolderPath()

	// Construct the remote path based on the date
	remotePath := date
	if folderPath != "" {
		remotePath = fmt.Sprintf("%s/%s", folderPath, date)
	}

	r.logger.Info("Reading logs from GCS",
		zap.String("date", date),
		zap.String("bucket", bucketName),
		zap.String("object", remotePath))

	// Get GCS client
	client := r.provider.GetClient()
	if client == nil {
		return nil, fmt.Errorf("GCS client not initialized")
	}

	// Get the object
	reader, err := client.Bucket(bucketName).Object(remotePath).NewReader(ctx)
	if err != nil {
		// If object doesn't exist, return empty document with filter's schema
		if strings.Contains(err.Error(), "storage: object doesn't exist") {
			r.logger.Warn("Log file not found in GCS",
				zap.String("bucket", bucketName),
				zap.String("object", remotePath))
			return models.NewSchemaLogDocumentWithSchema(logFilter.GetSchema()), nil
		}

		// Other error
		r.logger.Error("Failed to get GCS object",
			zap.Error(err),
			zap.String("bucket", bucketName),
			zap.String("object", remotePath))
		return nil, fmt.Errorf("failed to get GCS object: %w", err)
	}
	defer reader.Close()

	// Read all content
	content, err := io.ReadAll(reader)
	if err != nil {
		r.logger.Error("Failed to read GCS object content", zap.Error(err))
		return nil, fmt.Errorf("failed to read GCS object content: %w", err)
	}

	// Parse content as schema-based log document
	var doc models.SchemaLogDocument
	err = json.Unmarshal(content, &doc)
	if err != nil {
		r.logger.Error("Failed to parse GCS object as schema log document", zap.Error(err))
		return nil, fmt.Errorf("failed to parse GCS object as schema log document: %w", err)
	}

	return &doc, nil
}

// AliyunReader provides implementation for reading logs from Aliyun OSS
type AliyunReader struct {
	provider *aliyun.Provider
	logger   *zap.Logger
}

// ReadLogs reads logs for a specific date from Aliyun OSS
func (r *AliyunReader) ReadLogs(ctx context.Context, date string, logFilter filter.Filter) (*models.SchemaLogDocument, error) {
	// Get bucket name from provider
	bucketName := r.provider.GetBucketName()

	r.logger.Info("Reading logs from Aliyun OSS",
		zap.String("date", date),
		zap.String("bucket", bucketName),
		zap.String("object", date))

	// Get bucket from provider
	bucket := r.provider.GetBucket()
	if bucket == nil {
		return nil, fmt.Errorf("Aliyun OSS bucket not initialized")
	}

	// Check if the object exists
	exists, err := bucket.IsObjectExist(date)
	if err != nil {
		r.logger.Error("Failed to check if Aliyun OSS object exists",
			zap.Error(err),
			zap.String("bucket", bucketName),
			zap.String("object", date))
		return nil, fmt.Errorf("failed to check if Aliyun OSS object exists: %w", err)
	}

	if !exists {
		r.logger.Warn("Log file not found in Aliyun OSS",
			zap.String("bucket", bucketName),
			zap.String("object", date))
		return models.NewSchemaLogDocumentWithSchema(logFilter.GetSchema()), nil
	}

	// Download the object
	result, err := bucket.GetObject(date)
	if err != nil {
		r.logger.Error("Failed to download Aliyun OSS object",
			zap.Error(err),
			zap.String("bucket", bucketName),
			zap.String("object", date))
		return nil, fmt.Errorf("failed to download Aliyun OSS object: %w", err)
	}
	defer result.Close()

	// Read all content
	content, err := io.ReadAll(result)
	if err != nil {
		r.logger.Error("Failed to read Aliyun OSS object content", zap.Error(err))
		return nil, fmt.Errorf("failed to read Aliyun OSS object content: %w", err)
	}

	// Parse content as schema-based log document
	var doc models.SchemaLogDocument
	err = json.Unmarshal(content, &doc)
	if err != nil {
		r.logger.Error("Failed to parse Aliyun OSS object as schema log document", zap.Error(err))
		return nil, fmt.Errorf("failed to parse Aliyun OSS object as schema log document: %w", err)
	}

	return &doc, nil
}
