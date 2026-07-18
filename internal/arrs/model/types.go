package model

import (
	"context"
	"fmt"

	"github.com/javi11/altmount/internal/config"
)

var (
	ErrPathMatchFailed         = fmt.Errorf("path match failed")
	ErrEpisodeAlreadySatisfied = fmt.Errorf("item already satisfied by another file in ARR")
	ErrInstanceNotFound        = fmt.Errorf("instance not found")
)

// ConfigInstance represents an arrs instance from configuration
type ConfigInstance struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // "radarr", "sonarr", "lidarr", "readarr", "whisparr", or "sportarr"
	URL      string `json:"url"`
	APIKey   string `json:"api_key"`
	Category string `json:"category"`
	Enabled  bool   `json:"enabled"`
}

// ConfigManager interface defines methods needed for configuration management
type ConfigManager interface {
	Snapshot() (config.ConfigSnapshot, error)
	CompareAndSwap(context.Context, uint64, *config.Config) (config.ConfigSnapshot, error)
}
