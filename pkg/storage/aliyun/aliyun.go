package aliyun

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/harryosmar/docker-container-logger/pkg/filter"
	"github.com/harryosmar/docker-container-logger/pkg/models"
	"go.uber.org/zap"
)

// Provider implements the storage.Provider interface for Aliyun OSS
type Provider struct {
	client     *oss.Client
	bucket     *oss.Bucket
	bucketName string
	logger     *zap.Logger
}

// NewProvider creates a new Aliyun OSS storage provider
func NewProvider(logger *zap.Logger) *Provider {
	return &Provider{
		logger: logger,
	}
}

// Upload implements the storage.Provider interface for Aliyun OSS
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

// UploadWithFilter implements the storage.Provider interface for Aliyun OSS with custom filter schema
func (p *Provider) UploadWithFilter(ctx context.Context, localPath string, remotePath string, logFilter filter.Filter) error {
	// Read the local file content
	localContent, err := os.ReadFile(localPath)
	if err != nil {
		p.logger.Error("Failed to read local file", zap.Error(err), zap.String("path", localPath))
		return err
	}

	// Parse local content as a schema-based log document
	var localDoc models.SchemaLogDocument
	if err := json.Unmarshal(localContent, &localDoc); err != nil {
		p.logger.Error("Failed to parse local file as schema log document", zap.Error(err))
		return err
	}

	// If there are no rows in the local document, nothing to upload
	if len(localDoc.Rows) == 0 {
		p.logger.Info("No new logs to upload, skipping")
		return nil
	}

	// Check if remote object exists
	exists, err := p.bucket.IsObjectExist(remotePath)
	if err != nil {
		p.logger.Error("Error checking if object exists", zap.Error(err), zap.String("path", remotePath))
		return err
	}

	var combinedDoc *models.SchemaLogDocument

	if exists {
		// If remote file exists, download it first, merge with local content, then upload
		p.logger.Info("Remote file exists, merging schema documents",
			zap.String("bucket", p.bucketName),
			zap.String("key", remotePath))

		// Download existing content
		obj, err := p.bucket.GetObject(remotePath)
		if err != nil {
			p.logger.Error("Failed to download existing file", zap.Error(err))
			return err
		}
		defer obj.Close()

		// Read all content
		existingContent, err := io.ReadAll(obj)
		if err != nil {
			p.logger.Error("Failed to read existing file", zap.Error(err))
			return err
		}

		// Parse existing content as a schema-based log document
		var remoteDoc models.SchemaLogDocument
		if err := json.Unmarshal(existingContent, &remoteDoc); err != nil {
			p.logger.Error("Failed to parse remote file as schema log document", zap.Error(err))
			return err
		}

		// Merge the two documents
		combinedDoc, err = models.MergeSchemaLogDocuments(&remoteDoc, &localDoc)
		if err != nil {
			p.logger.Error("Failed to merge schema log documents", zap.Error(err))
			return err
		}
	} else {
		// If remote file doesn't exist, use the local document
		p.logger.Info("Remote file doesn't exist, using local document",
			zap.String("bucket", p.bucketName),
			zap.String("key", remotePath))
		combinedDoc = &localDoc
	}

	// Marshal the combined document
	combinedContent, err := json.Marshal(combinedDoc)
	if err != nil {
		p.logger.Error("Failed to marshal combined schema log document", zap.Error(err))
		return err
	}

	// Upload the combined content
	err = p.bucket.PutObject(remotePath, bytes.NewReader(combinedContent))
	if err != nil {
		p.logger.Error("Failed to upload combined file", zap.Error(err))
		return err
	}

	p.logger.Info("Successfully uploaded schema log document to Aliyun OSS",
		zap.String("bucket", p.bucketName),
		zap.String("key", remotePath),
		zap.Int("total_rows", len(combinedDoc.Rows)))


	// Clean up local file after successful upload (truncate it)
	file, err := os.OpenFile(localPath, os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		p.logger.Error("Failed to clear local file after upload", zap.Error(err), zap.String("path", localPath))
		// Don't return error here, the upload was successful
	} else {
		// Initialize the file with an empty schema-based log document
		emptyDoc := models.NewSchemaLogDocumentWithSchema(logFilter.GetSchema())
		emptyContent, err := json.Marshal(emptyDoc)
		if err != nil {
			p.logger.Error("Failed to marshal empty schema log document", zap.Error(err))
		} else {
			_, err = file.Write(emptyContent)
			if err != nil {
				p.logger.Error("Failed to initialize file with empty schema log document", zap.Error(err))
			}
		}
		file.Close()
		p.logger.Info("Cleared local file and initialized with empty schema log document", zap.String("path", localPath))
	}

	return nil
}

// Initialize implements the storage.Provider interface for Aliyun OSS
func (p *Provider) Initialize(ctx context.Context, config map[string]interface{}) error {
	// Extract bucket name from config
	bucketName, ok := config["bucket_name"].(string)
	if !ok || bucketName == "" {
		return fmt.Errorf("bucket_name is required for Aliyun OSS storage")
	}
	p.bucketName = bucketName

	// Extract required credentials from config
	endpoint, ok := config["endpoint"].(string)
	if !ok || endpoint == "" {
		return fmt.Errorf("endpoint is required for Aliyun OSS storage")
	}

	accessKeyID, ok := config["access_key_id"].(string)
	if !ok || accessKeyID == "" {
		return fmt.Errorf("access_key_id is required for Aliyun OSS storage")
	}

	accessKeySecret, ok := config["access_key_secret"].(string)
	if !ok || accessKeySecret == "" {
		return fmt.Errorf("access_key_secret is required for Aliyun OSS storage")
	}

	// Initialize Aliyun OSS client
	client, err := oss.New(endpoint, accessKeyID, accessKeySecret)
	if err != nil {
		p.logger.Error("Failed to create Aliyun OSS client", zap.Error(err))
		return fmt.Errorf("failed to create Aliyun OSS client: %w", err)
	}
	p.client = client

	// Get bucket handle
	bucket, err := client.Bucket(bucketName)
	if err != nil {
		p.logger.Error("Failed to get bucket", zap.Error(err), zap.String("bucket", bucketName))
		return fmt.Errorf("failed to get bucket %s: %w", bucketName, err)
	}
	p.bucket = bucket

	// Verify bucket exists by listing objects (will fail if bucket doesn't exist)
	_, err = bucket.ListObjects(oss.MaxKeys(1))
	if err != nil {
		p.logger.Error("Failed to list objects in bucket", zap.Error(err), zap.String("bucket", bucketName))
		return fmt.Errorf("failed to verify bucket %s exists: %w", bucketName, err)
	}

	p.logger.Info("Aliyun OSS storage provider initialized", zap.String("bucket", p.bucketName))
	return nil
}

// GetBucketName returns the bucket name
func (p *Provider) GetBucketName() string {
	return p.bucketName
}

// GetClient returns the Aliyun OSS client
func (p *Provider) GetClient() *oss.Client {
	return p.client
}

// GetBucket returns the Aliyun OSS bucket
func (p *Provider) GetBucket() *oss.Bucket {
	return p.bucket
}
