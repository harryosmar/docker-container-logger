package dummy

import (
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"os"
	"path/filepath"

	"github.com/harryosmar/docker-container-logger/pkg/filter"
	"github.com/harryosmar/docker-container-logger/pkg/models"
)

type Provider struct {
	logger    *zap.Logger
	uploadDir string
}

func NewProvider(logger *zap.Logger) *Provider {
	return &Provider{
		logger:    logger,
		uploadDir: "uploads",
	}
}

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

func (p *Provider) UploadWithFilter(ctx context.Context, localPath string, remotePath string, logFilter filter.Filter) error {
	// Create uploads directory if it doesn't exist
	err := os.MkdirAll("uploads", 0755)
	if err != nil {
		p.logger.Error("Failed to create uploads directory", zap.Error(err))
		return err
	}

	// Determine the destination path based on the remote path
	// Use the file name pattern from the remote path
	destPath := filepath.Join("uploads", filepath.Base(remotePath))
	p.logger.Info("Dummy provider: Processing upload",
		zap.String("local", localPath),
		zap.String("remote", remotePath),
		zap.String("dest", destPath))

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

	// Check if destination file exists
	if _, err := os.Stat(destPath); os.IsNotExist(err) {
		// If destination doesn't exist, use the local document
		combinedDoc = &localDoc
		p.logger.Info("Creating new schema log document", zap.String("path", destPath))
	} else {
		// If destination exists, read and merge
		remoteContent, err := os.ReadFile(destPath)
		if err != nil {
			p.logger.Error("Failed to read remote file", zap.Error(err), zap.String("path", destPath))
			return err
		}

		// Parse remote content as schema-based log document
		var remoteDoc models.SchemaLogDocument
		err = json.Unmarshal(remoteContent, &remoteDoc)
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
		p.logger.Info("Merging with existing schema log document", zap.String("path", destPath))
	}

	// Marshal combined document back to JSON
	combinedContent, err := json.Marshal(combinedDoc)
	if err != nil {
		p.logger.Error("Failed to marshal combined schema log document", zap.Error(err))
		return err
	}

	// Write the combined content to the destination file
	err = os.WriteFile(destPath, combinedContent, 0644)
	if err != nil {
		p.logger.Error("Failed to write to destination file", zap.Error(err), zap.String("path", destPath))
		return err
	}

	p.logger.Info("Dummy provider: Upload successful",
		zap.String("dest", destPath),
		zap.Int("rows", len(combinedDoc.Rows)))

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

	return nil
}

// GetUploadDir returns the upload directory
func (p *Provider) GetUploadDir() string {
	return p.uploadDir
}

func (p *Provider) Initialize(ctx context.Context, config map[string]interface{}) error {
	p.logger.Info("Initializing Dummy Provider")
	p.logger.Info(fmt.Sprintf("Config: %+v", config))

	uploadDir, ok := config["upload_dir"].(string)
	if ok && uploadDir != "" {
		p.uploadDir = uploadDir
	}

	// Create uploads directory
	err := os.MkdirAll(p.uploadDir, 0755)
	if err != nil {
		p.logger.Error("Failed to create uploads directory", zap.Error(err))
		return err
	}

	p.logger.Info("Dummy Provider Initialized", zap.String("uploadDir", p.uploadDir))
	return nil
}
