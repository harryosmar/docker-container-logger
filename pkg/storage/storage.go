package storage

import (
	"context"
	
	"github.com/harryosmar/docker-container-logger/pkg/filter"
)

// Provider defines the interface for cloud storage providers
type Provider interface {
	// Upload uploads a file to cloud storage
	// If the remote file exists, the contents of the local file will be appended to it
	Upload(ctx context.Context, localPath string, remotePath string) error
	// UploadWithFilter uploads a file to cloud storage using the specified filter for schema
	UploadWithFilter(ctx context.Context, localPath string, remotePath string, logFilter filter.Filter) error
	// Initialize sets up the storage provider
	Initialize(ctx context.Context, config map[string]interface{}) error
}
