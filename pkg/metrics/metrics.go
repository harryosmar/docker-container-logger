package metrics

import (
	"net/http"

	"github.com/harryosmar/docker-container-logger/pkg/filter"
	"github.com/harryosmar/docker-container-logger/pkg/storage"
	"github.com/harryosmar/docker-container-logger/pkg/web"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Metrics holds all Prometheus metrics
type Metrics struct {
	StreamsActive    prometheus.Gauge
	LinesProcessed   prometheus.Counter
	Reconnects       prometheus.Counter
	LinesDropped     prometheus.Counter
	QueueLengthGauge prometheus.Gauge
	logger           *zap.Logger
	storageProvider  storage.Provider
	logFilter        filter.Filter
}

// NewMetrics creates a new metrics instance
func NewMetrics(logger *zap.Logger, logFilter filter.Filter) *Metrics {
	m := &Metrics{
		StreamsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "logger_active_streams",
			Help: "Active container streams",
		}),
		LinesProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "logger_lines_total",
			Help: "Total log lines processed",
		}),
		Reconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "logger_reconnects_total",
			Help: "Reconnect attempts",
		}),
		LinesDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "logger_lines_dropped_total",
			Help: "Dropped log lines due to backpressure",
		}),
		QueueLengthGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "logger_write_queue_length",
			Help: "Current write queue length",
		}),
		logger:    logger,
		logFilter: logFilter,
	}

	// Register metrics
	prometheus.MustRegister(
		m.StreamsActive,
		m.LinesProcessed,
		m.Reconnects,
		m.LinesDropped,
		m.QueueLengthGauge,
	)

	return m
}

// SetStorageProvider sets the storage provider for the metrics server
func (m *Metrics) SetStorageProvider(provider storage.Provider) {
	m.storageProvider = provider
}

// ServeMetrics starts the metrics HTTP server
func (m *Metrics) ServeMetrics(addr string) {
	// Create a new ServeMux to avoid conflicts with existing handlers
	mux := http.NewServeMux()
	
	// Register metrics endpoint
	mux.Handle("/metrics", promhttp.Handler())
	
	// Register health check endpoint
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	
	// Register dashboard handler if storage provider is available
	if m.storageProvider != nil {
		dashboardHandler := web.NewDashboardHandler(m.logger, m.storageProvider, m.logFilter)
		// Mount the dashboard handler at the root path
		mux.Handle("/", dashboardHandler.Handler())
	}

	m.logger.Info("Metrics server started", zap.String("addr", addr))
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			m.logger.Error("Metrics server failed", zap.Error(err))
		}
	}()
}

// UpdateQueueLength updates the queue length gauge
func (m *Metrics) UpdateQueueLength(length int) {
	m.QueueLengthGauge.Set(float64(length))
}

// IncStreamsActive increments the active streams gauge
func (m *Metrics) IncStreamsActive() {
	m.StreamsActive.Inc()
}

// DecStreamsActive decrements the active streams gauge
func (m *Metrics) DecStreamsActive() {
	m.StreamsActive.Dec()
}

// IncLinesProcessed increments the lines processed counter
func (m *Metrics) IncLinesProcessed() {
	m.LinesProcessed.Inc()
}

// IncReconnects increments the reconnects counter
func (m *Metrics) IncReconnects() {
	m.Reconnects.Inc()
}

// IncLinesDropped increments the lines dropped counter
func (m *Metrics) IncLinesDropped() {
	m.LinesDropped.Inc()
}
