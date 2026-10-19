package database

import (
	"context"
	stdlog "log"
	"time"

	"github.com/rs/zerolog"
	"go.uber.org/fx"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/gscho/vercel-go-example/api/internal/config"
)

func New(lc fx.Lifecycle, cfg config.Config, log zerolog.Logger) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN: cfg.DatabaseURL,
		// Neon's pooled connection string goes through PgBouncer in transaction mode,
		// which doesn't keep pgx's cached prepared statements between transactions.
		PreferSimpleProtocol: true,
	}), &gorm.Config{Logger: newGormLogger(log)})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(10)
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error { return sqlDB.Close() },
	})
	return db, nil
}

func newGormLogger(log zerolog.Logger) gormlogger.Interface {
	writer := stdlog.New(log.With().Str("component", "gorm").Logger(), "", 0)
	return gormlogger.New(writer, gormlogger.Config{
		SlowThreshold:             500 * time.Millisecond,
		LogLevel:                  gormlogger.Warn,
		IgnoreRecordNotFoundError: true,
	})
}
