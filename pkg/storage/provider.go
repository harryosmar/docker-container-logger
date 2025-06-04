package storage

import (
	"context"
	"github.com/harryosmar/docker-container-logger/pkg/storage/dummy"

	"github.com/harryosmar/docker-container-logger/pkg/storage/aliyun"
	"github.com/harryosmar/docker-container-logger/pkg/storage/gcs"
	"github.com/harryosmar/docker-container-logger/pkg/storage/s3"
	"go.uber.org/zap"
)

// S3Provider is an alias for the S3 storage provider
type S3Provider = s3.Provider

// GCSProvider is an alias for the GCS storage provider
type GCSProvider = gcs.Provider

// AliyunProvider is an alias for the Aliyun OSS storage provider
type AliyunProvider = aliyun.Provider

// CreateProvider creates a storage provider based on the given type and config
func CreateProvider(ctx context.Context, storageType string, config map[string]interface{}, logger *zap.Logger) (Provider, error) {
	switch storageType {
	case "s3":
		provider := s3.NewProvider(logger)
		err := provider.Initialize(ctx, config)
		return provider, err
	case "gcs":
		provider := gcs.NewProvider(logger)
		err := provider.Initialize(ctx, config)
		return provider, err
	case "aliyun":
		provider := aliyun.NewProvider(logger)
		err := provider.Initialize(ctx, config)
		return provider, err
	case "dummy":
		provider := dummy.NewProvider(logger)
		err := provider.Initialize(ctx, config)
		return provider, err
	default:
		return nil, ErrUnsupportedStorageType(storageType)
	}
}
