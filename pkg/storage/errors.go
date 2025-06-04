package storage

import "fmt"

// ErrUnsupportedStorageType is returned when an unsupported storage type is requested
func ErrUnsupportedStorageType(storageType string) error {
	return fmt.Errorf("unsupported storage type: %s", storageType)
}
