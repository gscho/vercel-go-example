// Package app holds the dependencies shared by the API and the worker.
package app

import (
	"github.com/rs/zerolog"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/gscho/vercel-go-example/api/internal/config"
	"github.com/gscho/vercel-go-example/api/internal/database"
	"github.com/gscho/vercel-go-example/api/internal/logging"
	"github.com/gscho/vercel-go-example/api/internal/queue"
	"github.com/gscho/vercel-go-example/api/internal/store"
)

var Core = fx.Options(
	fx.Provide(
		config.Load,
		logging.New,
		database.New,
		store.New,
		func(cfg config.Config) *queue.Client { return queue.NewClient(cfg.QueueRegion) },
	),
	fx.WithLogger(func(log zerolog.Logger) fxevent.Logger { return logging.FxLogger{Log: log} }),
)
