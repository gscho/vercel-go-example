package config

import (
	"errors"
	"os"
)

type Config struct {
	DatabaseURL string
	Port        string
	QueueRegion string
}

func Load() (Config, error) {
	cfg := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		Port:        envOr("PORT", "3001"),
		QueueRegion: envOr("QUEUE_REGION", "iad1"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
