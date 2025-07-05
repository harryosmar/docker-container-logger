package main

import (
	"flag"
	"log"
	"os"

	"github.com/harryosmar/docker-container-logger/pkg/app"
)

func main() {
	configFilePath := os.Getenv("DOCKER_CONTAINER_LOGGER_CONFIG_PATH")
	if configFilePath == "" {
		configFilePath = "config.json"
	}
	configPath := flag.String("config", configFilePath, "Path to config file")
	flag.Parse()

	// Create and start the application
	application, err := app.NewApp(*configPath)
	if err != nil {
		log.Fatalf("Failed to initialize application: %v", err)
	}

	if err := application.Start(); err != nil {
		log.Fatalf("Application failed: %v", err)
	}

	os.Exit(0)
}
