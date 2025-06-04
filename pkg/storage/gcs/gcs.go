package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/storage"
	"github.com/harryosmar/docker-container-logger/pkg/filter"
	"github.com/harryosmar/docker-container-logger/pkg/models"
	"go.uber.org/zap"
)

// Provider implements the storage.Provider interface for Google Cloud Storage
type Provider struct {
	client     *storage.Client
	bucket     *storage.BucketHandle
	bucketName string
	logger     *zap.Logger
}

// NewProvider creates a new GCS storage provider
func NewProvider(logger *zap.Logger) *Provider {
	return &Provider{
		logger: logger,
	}
}

// Upload implements the storage.Provider interface for GCS
// It uses the default filter schema
func (p *Provider) Upload(ctx context.Context, localPath string, remotePath string) error {
	// Create a default filter that returns the default schema
	defaultFilter := &defaultSchemaFilter{}
	return p.UploadWithFilter(ctx, localPath, remotePath, defaultFilter)
}

// defaultSchemaFilter is a simple filter that returns the default schema
type defaultSchemaFilter struct{}

func (f *defaultSchemaFilter) Process(ctx context.Context, entry filter.LogEntry) (filter.LogEntry, bool, error) {
	return entry, true, nil
}

func (f *defaultSchemaFilter) Initialize(ctx context.Context, config map[string]interface{}) error {
	return nil
}

func (f *defaultSchemaFilter) GetSchema() []string {
	return filter.DefaultSchema
}

// UploadWithFilter implements the storage.Provider interface for GCS with custom filter schema
func (p *Provider) UploadWithFilter(ctx context.Context, localPath string, remotePath string, logFilter filter.Filter) error {
	// Check if the remote file already exists
	obj := p.bucket.Object(remotePath)
	_, err := obj.Attrs(ctx)
	exists := err == nil

	// Read the local file content
	localContent, err := os.ReadFile(localPath)
	if err != nil {
		p.logger.Error("Failed to read local file", zap.Error(err), zap.String("path", localPath))
		return err
	}

	// Parse local content as schema-based log document
	var localDoc models.SchemaLogDocument
	err = json.Unmarshal(localContent, &localDoc)
	if err != nil {
		p.logger.Error("Failed to parse local file as schema log document", zap.Error(err), zap.String("path", localPath))
		return err
	}

	// Skip empty documents (no rows)
	if len(localDoc.Rows) == 0 {
		p.logger.Info("Local file contains no logs, skipping upload", zap.String("path", localPath))
		return nil
	}

	var combinedDoc *models.SchemaLogDocument

	if exists {
		// If remote file exists, download it first, merge with local content, then upload
		p.logger.Info("Remote file exists, merging schema documents",
			zap.String("bucket", p.bucketName),
			zap.String("key", remotePath))

		// Download existing content
		reader, err := obj.NewReader(ctx)
		if err != nil {
			p.logger.Error("Failed to download existing file", zap.Error(err))
			return err
		}
		defer reader.Close()

		// Read all content
		existingContent, err := io.ReadAll(reader)
		if err != nil {
			p.logger.Error("Failed to read existing file content", zap.Error(err))
			return err
		}

		// Parse existing content as schema-based log document
		var remoteDoc models.SchemaLogDocument
		err = json.Unmarshal(existingContent, &remoteDoc)
		if err != nil {
			p.logger.Error("Failed to parse remote file as schema log document, will use local document", zap.Error(err))
			// If we can't parse the remote file, we'll just use the local document
			combinedDoc = &localDoc
		} else {
			// Merge the two documents
			combinedDoc, err = models.MergeSchemaLogDocuments(&remoteDoc, &localDoc)
			if err != nil {
				p.logger.Error("Failed to merge schema log documents, will use local document", zap.Error(err))
				combinedDoc = &localDoc
			}
		}

		// Marshal combined document back to JSON
		combinedContent, err := json.Marshal(combinedDoc)
		if err != nil {
			p.logger.Error("Failed to marshal combined schema log document", zap.Error(err))
			return err
		}

		// Upload combined content
		writer := obj.NewWriter(ctx)
		_, err = writer.Write(combinedContent)
		if err != nil {
			p.logger.Error("Failed to write combined content to GCS", zap.Error(err))
			return err
		}
		err = writer.Close()
		if err != nil {
			p.logger.Error("Failed to close GCS writer", zap.Error(err))
			return err
		}

		p.logger.Info("Successfully merged and uploaded schema log document to GCS",
			zap.String("bucket", p.bucketName),
			zap.String("key", remotePath),
			zap.Int("total_rows", len(combinedDoc.Rows)))
	} else {
		// If remote file doesn't exist, simply upload the local document
		p.logger.Info("Uploading new schema log document to GCS",
			zap.String("bucket", p.bucketName),
			zap.String("key", remotePath))

		// Upload the content
		writer := obj.NewWriter(ctx)
		_, err = writer.Write(localContent)
		if err != nil {
			p.logger.Error("Failed to write content to GCS", zap.Error(err))
			return err
		}
		err = writer.Close()
		if err != nil {
			p.logger.Error("Failed to close GCS writer", zap.Error(err))
			return err
		}

		p.logger.Info("Successfully uploaded new schema log document to GCS",
			zap.String("bucket", p.bucketName),
			zap.String("key", remotePath),
			zap.Int("rows", len(localDoc.Rows)))
	}

	// Clean up local file after successful upload (truncate it)
	file, err := os.OpenFile(localPath, os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		p.logger.Error("Failed to truncate local file after upload", zap.Error(err))
		return nil // Don't return error here, the upload was successful
	}
	defer file.Close()

	// Initialize with schema log document using the filter's schema
	emptyDoc := models.NewSchemaLogDocumentWithSchema(logFilter.GetSchema())
	emptyContent, err := json.Marshal(emptyDoc)
	if err != nil {
		p.logger.Error("Failed to marshal empty schema log document", zap.Error(err))
	} else {
		_, err = file.Write(emptyContent)
		if err != nil {
			p.logger.Error("Failed to initialize truncated file with empty schema log document", zap.Error(err))
		}
	}

	p.logger.Info("Cleared local file after upload", zap.String("path", localPath))

	return nil
}

// Initialize implements the storage.Provider interface for GCS
func (p *Provider) Initialize(ctx context.Context, config map[string]interface{}) error {
	// Extract bucket name from config
	bucketName, ok := config["bucket_name"].(string)
	if !ok || bucketName == "" {
		return fmt.Errorf("bucket_name is required for GCS storage")
	}
	p.bucketName = bucketName

	// Initialize GCS client
	client, err := storage.NewClient(ctx)
	if err != nil {
		p.logger.Error("Failed to create GCS client", zap.Error(err))
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	p.client = client
	p.bucket = client.Bucket(p.bucketName)

	// Verify that the bucket exists by attempting to get its attributes
	_, err = p.bucket.Attrs(ctx)
	if err != nil {
		p.logger.Error("Failed to get bucket attributes", zap.Error(err), zap.String("bucket", p.bucketName))
		return fmt.Errorf("bucket %s not found or not accessible: %w", p.bucketName, err)
	}

	p.logger.Info("GCS storage provider initialized", zap.String("bucket", p.bucketName))
	return nil
}

// GetBucketName returns the bucket name
func (p *Provider) GetBucketName() string {
	return p.bucketName
}

// GetClient returns the GCS client
func (p *Provider) GetClient() *storage.Client {
	return p.client
}

// GetBucket returns the GCS bucket handle
func (p *Provider) GetBucket() *storage.BucketHandle {
	return p.bucket
}

// GetFolderPath returns the folder path (empty for GCS implementation)
func (p *Provider) GetFolderPath() string {
	return ""
}
