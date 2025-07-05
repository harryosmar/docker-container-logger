package docker

import (
	"bufio"
	"context"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types/events"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/harryosmar/docker-container-logger/pkg/models"
	"go.uber.org/zap"
)

// ContainerManager handles Docker container operations
type ContainerManager struct {
	client        *client.Client
	logger        *zap.Logger
	maxStreams    int
	sinceWindow   time.Duration
	bufferSize    int
	dropOnFull    bool
	processLine   func(string, models.ContainerMeta)
	filterLabels  []string
	allContainers bool
}

// NewContainerManager creates a new container manager
func NewContainerManager(
	logger *zap.Logger,
	maxStreams int,
	sinceWindow time.Duration,
	bufferSize int,
	dropOnFull bool,
	filterLabels []string,
	allContainers bool,
	processLine func(string, models.ContainerMeta),
) (*ContainerManager, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}

	return &ContainerManager{
		client:        cli,
		logger:        logger,
		maxStreams:    maxStreams,
		sinceWindow:   sinceWindow,
		bufferSize:    bufferSize,
		dropOnFull:    dropOnFull,
		processLine:   processLine,
		filterLabels:  filterLabels,
		allContainers: allContainers,
	}, nil
}

// ListContainers lists containers based on configured filters
func (cm *ContainerManager) ListContainers(ctx context.Context) ([]types.Container, error) {
	if cm.allContainers {
		// If allContainers is true, don't apply any filters
		return cm.client.ContainerList(ctx, container.ListOptions{})
	}

	if len(cm.filterLabels) == 0 {
		// Default filter if no labels specified
		f := filters.NewArgs()
		f.Add("label", "com.docker.swarm.service.name")
		return cm.client.ContainerList(ctx, container.ListOptions{Filters: f})
	}

	// Get all containers with at least some label
	f := filters.NewArgs()
	// We still need some basic filter to avoid getting all containers
	f.Add("label", "") // This gets containers with any label

	allContainers, err := cm.client.ContainerList(ctx, container.ListOptions{Filters: f})
	if err != nil {
		return nil, err
	}

	// Filter containers based on our filter labels
	var filteredContainers []types.Container
	seenContainers := make(map[string]bool)

	for _, c := range allContainers {
		// Check if this container matches any of our filter labels
		for _, filterLabel := range cm.filterLabels {
			// Parse the filter label
			parts := strings.SplitN(filterLabel, "=", 2)
			
			if len(parts) == 1 {
				// Just checking for label existence
				if _, exists := c.Labels[parts[0]]; exists {
					if !seenContainers[c.ID] {
						filteredContainers = append(filteredContainers, c)
						seenContainers[c.ID] = true
					}
					break // Found a match, no need to check other filters
				}
			} else {
				// Checking for label with specific value
				if value, exists := c.Labels[parts[0]]; exists && value == parts[1] {
					if !seenContainers[c.ID] {
						filteredContainers = append(filteredContainers, c)
						seenContainers[c.ID] = true
					}
					break // Found a match, no need to check other filters
				}
			}
		}
	}

	return filteredContainers, nil
}

// StartMonitoring starts monitoring containers
func (cm *ContainerManager) StartMonitoring(ctx context.Context) error {
	sem := make(chan struct{}, cm.maxStreams)
	cancelMap := sync.Map{}

	evsFilter := filters.NewArgs()
	evsFilter.Add("type", "container")
	evsFilter.Add("event", "start")
	evsFilter.Add("event", "die")
	eventsCh, errsCh := cm.client.Events(ctx, events.ListOptions{Filters: evsFilter})

	containers, err := cm.ListContainers(ctx)
	if err != nil {
		return err
	}

	for _, c := range containers {
		cm.startTail(ctx, sem, &cancelMap, c.ID)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-eventsCh:
				if !ok {
					return
				}
				id := ev.Actor.ID
				switch ev.Action {
				case "start":
					cm.startTail(ctx, sem, &cancelMap, id)
				case "die":
					if v, ok := cancelMap.Load(id); ok {
						v.(context.CancelFunc)()
						cancelMap.Delete(id)
					}
				}
			case err, ok := <-errsCh:
				if !ok {
					return
				}
				cm.logger.Warn("Events error, reconnecting", zap.Error(err))
				time.Sleep(time.Second)
				eventsCh, errsCh = cm.client.Events(ctx, events.ListOptions{Filters: evsFilter})
			}
		}
	}()

	return nil
}

// startTail starts tailing logs for a container
func (cm *ContainerManager) startTail(ctx context.Context, sem chan struct{}, cancelMap *sync.Map, cid string) {
	inspect, err := cm.client.ContainerInspect(ctx, cid)
	if err != nil {
		cm.logger.Warn("Inspect failed", zap.String("id", cid), zap.Error(err))
		return
	}

	meta := models.ContainerMeta{
		ID:     cid,
		Name:   strings.TrimPrefix(inspect.Name, "/"),
		Labels: inspect.Config.Labels,
	}

	sem <- struct{}{}
	cctx, cancel := context.WithCancel(ctx)
	cancelMap.Store(cid, cancel)

	go func() {
		defer func() { <-sem }()
		cm.tailLogs(cctx, meta)
	}()
}

// tailLogs tails logs for a container
func (cm *ContainerManager) tailLogs(ctx context.Context, meta models.ContainerMeta) {
	delay := time.Second
	buffer := make(chan string, cm.bufferSize)

	// Create a separate context that can be cancelled when the buffer is closed
	bufferCtx, bufferCancel := context.WithCancel(context.Background())
	defer bufferCancel()

	go func() {
		for line := range buffer {
			select {
			case <-bufferCtx.Done():
				return
			default:
				cm.processLine(line, meta)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// Parent context cancelled, exit gracefully
			close(buffer)
			return
		default:
			since := time.Now().Add(-cm.sinceWindow).Format(time.RFC3339)
			r, err := cm.client.ContainerLogs(
				ctx,
				meta.ID,
				container.LogsOptions{
					ShowStdout: true,
					ShowStderr: true,
					Follow:     true,
					Timestamps: true,
					Since:      since,
				},
			)
			if err != nil {
				cm.logger.Warn("Fetch failed", zap.String("id", meta.ID), zap.Error(err), zap.Duration("delay", delay))

				// Check if context is done before sleeping
				select {
				case <-ctx.Done():
					close(buffer)
					return
				case <-time.After(delay):
					// Continue with backoff
				}

				delay *= 2
				if delay > 30*time.Second {
					delay = 30 * time.Second
				}
				continue
			}
			delay = time.Second

			s := bufio.NewScanner(r)
			s.Buffer(make([]byte, 64*1024), 1024*1024) // Increase scanner buffer to handle large log lines

			for s.Scan() {
				select {
				case <-ctx.Done():
					r.Close()
					close(buffer)
					return
				case buffer <- s.Text():
					// Successfully sent to buffer
				default:
					// Buffer full, drop or block based on configuration
					if cm.dropOnFull {
						// Drop the line
						continue
					} else {
						// Block until we can write
						buffer <- s.Text()
					}
				}
			}

			if err := s.Err(); err != nil {
				cm.logger.Warn("Scanner error", zap.String("id", meta.ID), zap.Error(err))
			}

			r.Close()

			// Check if context is done before continuing
			select {
			case <-ctx.Done():
				close(buffer)
				return
			default:
				// Continue with reconnect
			}
		}
	}
}

// Close closes the Docker client
func (cm *ContainerManager) Close() error {
	return cm.client.Close()
}
