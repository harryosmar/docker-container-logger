package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/harryosmar/docker-container-logger/pkg/filter"
	"github.com/harryosmar/docker-container-logger/pkg/models"
	"go.uber.org/zap"
	"os"
)

// Provider implements the storage.Provider interface for AWS S3
type Provider struct {
	client       *s3.Client
	bucketName   string
	logger       *zap.Logger
	folderPath   string
	region       string
	accessKey    string
	secretKey    string
	sessionToken string
}

// NewProvider creates a new S3 storage provider
func NewProvider(logger *zap.Logger) *Provider {
	return &Provider{
		logger: logger,
	}
}

// Upload implements the storage.Provider interface for S3
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

// UploadWithFilter implements the storage.Provider interface for S3 with custom filter schema
func (p *Provider) UploadWithFilter(ctx context.Context, localPath string, remotePath string, logFilter filter.Filter) error {
	// append folderPath to remotePath
	if p.folderPath != "" {
		remotePath = fmt.Sprintf("%s/%s", p.folderPath, remotePath)
	}

	// Check if the remote file already exists
	exists := false
	try := func() error {
		_, err := p.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &p.bucketName,
			Key:    &remotePath,
		})
		if err == nil {
			exists = true
		}
		return nil // ignore errors, just assume it doesn't exist
	}
	_ = try() // ignore any error

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
		result, err := p.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &p.bucketName,
			Key:    &remotePath,
		})
		if err != nil {
			p.logger.Error("Failed to download existing file", zap.Error(err))
			return err
		}
		defer result.Body.Close()

		// Read all content
		existingContent := make([]byte, 0)
		buf := make([]byte, 1024)
		for {
			n, err := result.Body.Read(buf)
			if n > 0 {
				existingContent = append(existingContent, buf[:n]...)
			}
			if err != nil {
				break // EOF or error
			}
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
		_, err = p.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &p.bucketName,
			Key:         &remotePath,
			Body:        bytes.NewReader(combinedContent),
			ContentType: aws.String("application/json"),
		})
		if err != nil {
			p.logger.Error("Failed to upload combined file", zap.Error(err))
			return err
		}

		p.logger.Info("Successfully merged and uploaded schema log document to S3",
			zap.String("bucket", p.bucketName),
			zap.String("key", remotePath),
			zap.Int("total_rows", len(combinedDoc.Rows)))
	} else {
		// If remote file doesn't exist, simply upload the local document
		p.logger.Info("Uploading new schema log document to S3",
			zap.String("bucket", p.bucketName),
			zap.String("key", remotePath))

		// Upload the content
		_, err = p.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &p.bucketName,
			Key:         &remotePath,
			Body:        bytes.NewReader(localContent),
			ContentType: aws.String("application/json"),
		})
		if err != nil {
			p.logger.Error("Failed to upload new file", zap.Error(err))
			return err
		}

		p.logger.Info("Successfully uploaded new schema log document to S3",
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

	// Initialize with empty schema log document
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

// Initialize implements the storage.Provider interface for S3
func (p *Provider) Initialize(ctx context.Context, config map[string]interface{}) error {
	// Extract bucket name from config
	bucketName, ok := config["bucket_name"].(string)
	if !ok || bucketName == "" {
		return fmt.Errorf("bucket_name is required for S3 storage")
	}
	p.bucketName = bucketName

	// Extract folder path from config
	path, ok := config["folder_path"].(string)
	if !ok || path == "" {
		p.folderPath = ""
	}
	p.folderPath = path

	// Extract region from config
	region, ok := config["region"].(string)
	if !ok || region == "" {
		p.region = ""
	}
	p.region = region

	// Extract region from config
	accessKey, ok := config["access_key"].(string)
	if !ok || accessKey == "" {
		p.accessKey = ""
	}
	p.accessKey = accessKey

	// Extract region from config
	secretKey, ok := config["secret_key"].(string)
	if !ok || secretKey == "" {
		p.secretKey = ""
	}
	p.secretKey = secretKey

	// Extract region from config
	sessionToken, ok := config["session_token"].(string)
	if !ok || sessionToken == "" {
		p.sessionToken = ""
	}
	p.sessionToken = sessionToken

	// Load AWS config
	awsCfg, err := awsConfig.LoadDefaultConfig(
		ctx,
		awsConfig.WithRegion(region),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			p.accessKey,
			p.secretKey,
			p.sessionToken,
		)),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Override region if specified
	if region, ok := config["region"].(string); ok && region != "" {
		awsCfg.Region = region
	}

	p.client = s3.NewFromConfig(awsCfg)
	p.logger.Info("S3 storage provider initialized", zap.String("bucket", p.bucketName))
	return nil
}

// GetBucketName returns the bucket name
func (p *Provider) GetBucketName() string {
	return p.bucketName
}

// GetFolderPath returns the folder path
func (p *Provider) GetFolderPath() string {
	return p.folderPath
}

// GetClient returns the S3 client
func (p *Provider) GetClient() *s3.Client {
	return p.client
}
