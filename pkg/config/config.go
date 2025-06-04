package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
)

// StorageConfig holds storage-specific configuration
type StorageConfig struct {
	Type   string                 `json:"type"`
	Config map[string]interface{} `json:"config"`
}

// FilterConfig holds filter-specific configuration
type FilterConfig struct {
	Type   string                 `json:"type"`
	Config map[string]interface{} `json:"config"`
}

// AppConfig holds application configuration
type AppConfig struct {
	FilterLabels       []string      `json:"filter_labels"`
	AllContainers      bool          `json:"all_containers"`
	Format             string        `json:"format"`
	MaxStreams         int           `json:"max_streams"`
	TailBufferSize     int           `json:"tail_buffer_size"`
	WriteBufferSize    int           `json:"write_buffer_size"`
	SinceWindowSeconds int           `json:"since_window_seconds"`
	GCPercent          int           `json:"gc_percent"`
	DropOnFull         bool          `json:"drop_on_full"`
	MetricsAddr        string        `json:"metrics_addr"`
	RolloverMinutes    int           `json:"rollover_minutes"`
	FileNamePattern    string        `json:"file_name_pattern"`
	Storage            StorageConfig `json:"storage"`
	Filter             FilterConfig  `json:"filter"`
	ConfigPath         string        `json:"-"` // Path to config file, not stored in JSON
}

// LoadConfig loads configuration from a JSON file
func LoadConfig(path string) (*AppConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var c AppConfig
	if err := json.NewDecoder(f).Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

// WatchConfig watches for changes in the config file and calls the provided callback
func WatchConfig(path string, logger *zap.Logger, callback func(*AppConfig)) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Warn("Watch fail", zap.Error(err))
		return
	}
	defer w.Close()
	w.Add(path)
	for ev := range w.Events {
		if ev.Op&fsnotify.Write == fsnotify.Write {
			if c, err := LoadConfig(path); err == nil {
				callback(c)
			}
		}
	}
}

// GetSinceWindow returns the since window as a time.Duration
func (c *AppConfig) GetSinceWindow() time.Duration {
	return time.Duration(c.SinceWindowSeconds) * time.Second
}

// String returns a string representation of the config
func (c *AppConfig) String() string {
	b, _ := json.MarshalIndent(c, "", "  ")
	return fmt.Sprintf("Config: %s", string(b))
}
