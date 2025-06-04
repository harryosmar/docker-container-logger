package web

import (
	"context"
	"embed"
	"encoding/json"
	"html/template"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/harryosmar/docker-container-logger/pkg/filter"
	"github.com/harryosmar/docker-container-logger/pkg/storage"
	"go.uber.org/zap"
)

//go:embed templates/index.html
var dashboardHTML embed.FS

// DashboardHandler handles the dashboard routes
type DashboardHandler struct {
	logger *zap.Logger
	reader storage.Reader
	router *mux.Router
	tmpl   *template.Template
	filter filter.Filter
}

// NewDashboardHandler creates a new dashboard handler
func NewDashboardHandler(logger *zap.Logger, provider storage.Provider, logFilter filter.Filter) *DashboardHandler {
	reader := storage.GetReader(provider, logger)
	router := mux.NewRouter()

	// Parse the embedded template
	tmpl, err := template.ParseFS(dashboardHTML, "templates/index.html")
	if err != nil {
		logger.Fatal("Failed to parse dashboard template", zap.Error(err))
	}

	handler := &DashboardHandler{
		logger: logger,
		reader: reader,
		router: router,
		tmpl:   tmpl,
		filter: logFilter,
	}

	// Register routes
	router.HandleFunc("/dashboard", handler.handleDashboard).Methods("GET")
	router.HandleFunc("/api/schema/{date}", handler.handleSchema).Methods("GET")
	router.HandleFunc("/api/data/{date}", handler.handleData).Methods("GET")

	return handler
}

// Handler returns the HTTP handler
func (h *DashboardHandler) Handler() http.Handler {
	return h.router
}

// handleDashboard renders the dashboard page
func (h *DashboardHandler) handleDashboard(w http.ResponseWriter, r *http.Request) {
	h.logger.Info("Dashboard request received")

	// Get current date in the format used for file names
	currentDate := time.Now().Format("2006-01-02")

	// Render the template
	data := map[string]interface{}{
		"CurrentDate": currentDate,
	}

	w.Header().Set("Content-Type", "text/html")
	if err := h.tmpl.Execute(w, data); err != nil {
		h.logger.Error("Failed to render dashboard template", zap.Error(err))
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleSchema handles the API endpoint for fetching schema information
//func (h *DashboardHandler) handleSchema(w http.ResponseWriter, r *http.Request) {
//	// Return just the schema as JSON
//	w.Header().Set("Content-Type", "application/json")
//	response := map[string]interface{}{
//		"schema": h.filter.GetSchema(),
//	}
//
//	if err := json.NewEncoder(w).Encode(response); err != nil {
//		h.logger.Error("Failed to encode schema", zap.Error(err))
//		http.Error(w, "Failed to encode schema", http.StatusInternalServerError)
//		return
//	}
//}

// handleSchema handles the API endpoint for fetching schema information
func (h *DashboardHandler) handleSchema(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	date := vars["date"]

	h.logger.Info("Schema request received", zap.String("date", date))

	// Read logs for the specified date
	ctx := context.Background()
	logs, err := h.reader.ReadLogs(ctx, date, h.filter)
	if err != nil {
		h.logger.Error("Failed to read logs", zap.Error(err), zap.String("date", date))
		http.Error(w, "Failed to read logs", http.StatusInternalServerError)
		return
	}

	// Return just the schema as JSON
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"schema": logs.Schema,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode schema", zap.Error(err))
		http.Error(w, "Failed to encode schema", http.StatusInternalServerError)
		return
	}
}

// handleData handles the API endpoint for fetching log data
func (h *DashboardHandler) handleData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	date := vars["date"]

	h.logger.Info("Data request received", zap.String("date", date))

	// Read logs for the specified date
	ctx := context.Background()
	logs, err := h.reader.ReadLogs(ctx, date, h.filter)
	if err != nil {
		h.logger.Error("Failed to read logs", zap.Error(err), zap.String("date", date))
		http.Error(w, "Failed to read logs", http.StatusInternalServerError)
		return
	}

	// Return just the data as JSON
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"data": logs.Rows,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode data", zap.Error(err))
		http.Error(w, "Failed to encode data", http.StatusInternalServerError)
		return
	}
}
