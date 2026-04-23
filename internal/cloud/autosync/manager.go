package autosync

import (
	"context"
	"time"

	"github.com/Gentleman-Programming/engram/internal/store"
)

type Config struct {
	TargetKey string
}

func DefaultConfig() Config {
	return Config{TargetKey: store.DefaultSyncTargetKey}
}

type Status struct {
	Phase               string
	LastError           string
	ConsecutiveFailures int
	BackoffUntil        *time.Time
	LastSyncAt          *time.Time
	ReasonCode          string
	ReasonMessage       string
}

type Manager struct {
	status Status
}

func New(_ any, _ any, _ Config) *Manager {
	return &Manager{status: Status{
		Phase:         "disabled",
		ReasonCode:    "autosync_unavailable",
		ReasonMessage: "autosync is not available in this release",
	}}
}

func (m *Manager) Run(ctx context.Context) {
	<-ctx.Done()
}

func (m *Manager) NotifyDirty() {}

func (m *Manager) Status() Status { return m.status }
