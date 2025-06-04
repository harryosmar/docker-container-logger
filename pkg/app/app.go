package app

import (
	"context"
	"github.com/harryosmar/docker-container-logger/pkg/config"
	"github.com/harryosmar/docker-container-logger/pkg/docker"
	"github.com/harryosmar/docker-container-logger/pkg/filter"
	"github.com/harryosmar/docker-container-logger/pkg/logging"
	"github.com/harryosmar/docker-container-logger/pkg/metrics"
	"github.com/harryosmar/docker-container-logger/pkg/processor"
	"github.com/harryosmar/docker-container-logger/pkg/storage"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
)

// App represents the main application
type App struct {
	config          *config.AppConfig
	configPath      string
	logger          *zap.Logger
	metrics         *metrics.Metrics
	fileLogger      *logging.FileLogger
	containerMgr    *docker.ContainerManager
	logProcessor    *processor.LogProcessor
	storageProvider storage.Provider
	shutdownCh      chan struct{}
}

// NewApp creates a new application instance
func NewApp(configPath string) (*App, error) {
	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	// Load configuration
	appConfig, err := config.LoadConfig(configPath)
	if err != nil {
		return nil, err
	}

	// Store config path for later use
	appConfig.ConfigPath = configPath

	// Initialize storage provider
	ctx := context.Background()
	storageProvider, err := storage.CreateProvider(
		ctx,
		appConfig.Storage.Type,
		appConfig.Storage.Config,
		logger,
	)
	if err != nil {
		return nil, err
	}

	// Initialize log filter
	logFilter, err := filter.CreateFilter(
		ctx,
		appConfig.Filter.Type,
		appConfig.Filter.Config,
		logger,
	)
	if err != nil {
		return nil, err
	}

	// Initialize metrics
	metricsService := metrics.NewMetrics(logger, logFilter)

	// Set storage provider in metrics service for dashboard access
	metricsService.SetStorageProvider(storageProvider)

	// Initialize file logger
	fileLogger := logging.NewFileLogger(
		appConfig.WriteBufferSize,
		storageProvider,
		logFilter,
		logger,
		appConfig.RolloverMinutes,
		appConfig.FileNamePattern,
	)

	// Initialize log processor
	logProcessor := processor.NewLogProcessor(
		logger,
		metricsService,
		fileLogger,
		appConfig.Format,
		appConfig.DropOnFull,
	)

	// Initialize container manager
	containerMgr, err := docker.NewContainerManager(
		logger,
		appConfig.MaxStreams,
		appConfig.GetSinceWindow(),
		appConfig.TailBufferSize,
		appConfig.DropOnFull,
		appConfig.FilterLabels,
		appConfig.AllContainers,
		logProcessor.ProcessLine,
	)
	if err != nil {
		return nil, err
	}

	return &App{
		config:          appConfig,
		configPath:      configPath,
		logger:          logger,
		metrics:         metricsService,
		fileLogger:      fileLogger,
		containerMgr:    containerMgr,
		logProcessor:    logProcessor,
		storageProvider: storageProvider,
		shutdownCh:      make(chan struct{}),
	}, nil
}

// Start starts the application
func (a *App) Start() error {
	// Start metrics server
	a.metrics.ServeMetrics(a.config.MetricsAddr)

	// Start file logger
	err := a.fileLogger.Start()
	if err != nil {
		return err
	}

	// Setup signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start container monitoring
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = a.containerMgr.StartMonitoring(ctx)
	if err != nil {
		return err
	}

	// Watch for configuration changes
	go a.watchConfig()

	// Wait for shutdown signal
	select {
	case <-sigCh:
		a.logger.Info("Received shutdown signal")
		cancel() // Cancel container monitoring context
		a.Shutdown()
	case <-a.shutdownCh:
		a.logger.Info("Shutdown requested")
		cancel() // Cancel container monitoring context
	}

	return nil
}

// Shutdown gracefully shuts down the application
func (a *App) Shutdown() {
	a.logger.Info("Shutting down application")

	// Shutdown file logger
	a.fileLogger.Shutdown()

	// Close container manager
	a.containerMgr.Close()

	// Sync logger
	a.logger.Sync()

	// Signal shutdown complete
	close(a.shutdownCh)
}

// watchConfig watches for configuration changes
func (a *App) watchConfig() {
	// Define the callback function for config changes
	callback := func(newConfig *config.AppConfig) {
		a.logger.Info("Configuration updated")

		// Update app config
		a.config = newConfig

		// Restart application with new config
		// In a real implementation, we would update components individually
		// but for simplicity, we'll just signal a restart
		a.Shutdown()
	}

	// Start watching for config changes
	config.WatchConfig(a.configPath, a.logger, callback)
}
