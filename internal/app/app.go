package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/import-service/internal/authorization"
	"github.com/lihongjie0209/import-service/internal/cache"
	"github.com/lihongjie0209/import-service/internal/config"
	"github.com/lihongjie0209/import-service/internal/database"
	"github.com/lihongjie0209/import-service/internal/eventbus"
	"github.com/lihongjie0209/import-service/internal/idempotency"
	"github.com/lihongjie0209/import-service/internal/importjob"
	"github.com/lihongjie0209/import-service/internal/logging"
	"github.com/lihongjie0209/import-service/internal/migration"
	"github.com/lihongjie0209/import-service/internal/objectstorage"
	"github.com/lihongjie0209/import-service/internal/observability"
	"github.com/lihongjie0209/import-service/internal/outbound"
	"github.com/lihongjie0209/import-service/internal/scheduler"
	grpctransport "github.com/lihongjie0209/import-service/internal/transport/grpc"
	httptransport "github.com/lihongjie0209/import-service/internal/transport/http"
	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

func New(cfg config.Config) *fx.App {
	return fx.New(
		fx.Supply(cfg),
		fx.Provide(newLogger),
		fx.Provide(observability.NewTracing),
		fx.WithLogger(func(logger *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: logger} }),
		MigrationModule,
		DatabaseModule,
		CacheModule,
		eventbus.Module,
		fx.Provide(idempotency.New),
		fx.Provide(observability.NewMetrics),
		outbound.Module,
		fx.Provide(newApplicationVerifier),
		fx.Provide(authorization.New),
		ImportModule,
		ImportEventModule,
		scheduler.Module,
		grpctransport.Module,
		httptransport.Module,
		fx.StartTimeout(cfg.App.ShutdownTimeout),
		fx.StopTimeout(cfg.App.ShutdownTimeout),
	)
}

func runStartupMigration(cfg config.Config, logger *slog.Logger) error {
	if !cfg.Migration.AutoUp {
		return nil
	}
	started := time.Now()
	logger.Info("running startup database migration", "path", cfg.Migration.Path)
	if err := migration.Run(cfg.Migration, "up", 0); err != nil {
		return fmt.Errorf("startup database migration: %w", err)
	}
	logger.Info("startup database migration completed", "duration", time.Since(started))
	return nil
}

func newLogger(lc fx.Lifecycle, cfg config.Config) (*slog.Logger, error) {
	logger, closer, err := logging.New(cfg.Log)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.StopHook(func() error { return closer.Close() }))
	return logger.With("service", cfg.App.Name, "environment", cfg.Runtime.ActiveProfile), nil
}

func newDatabase(lc fx.Lifecycle, cfg config.Config) (*sqlx.DB, error) {
	if !cfg.Database.Enabled {
		return nil, nil
	}
	db, err := database.Open(context.Background(), cfg.Database)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.StopHook(func() error { return db.Close() }))
	return db, nil
}

func newRedis(lc fx.Lifecycle, cfg config.Config, tracing *observability.Tracing) (*redis.Client, error) {
	if !cfg.Redis.Enabled {
		return nil, nil
	}
	client, err := cache.Open(context.Background(), cfg.Redis)
	if err != nil {
		return nil, err
	}
	if tracing.Enabled() {
		if err := redisotel.InstrumentTracing(client); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("instrument redis tracing: %w", err)
		}
	}
	lc.Append(fx.StopHook(func() error { return client.Close() }))
	return client, nil
}

func newLocker(client *redis.Client) *cache.Locker {
	if client == nil {
		return nil
	}
	return cache.NewLocker(client)
}

var DatabaseModule = fx.Module("database", fx.Provide(newDatabase, database.NewTransactor), fx.Invoke(func(db *sqlx.DB, logger *slog.Logger) {
	if db == nil {
		logger.Warn("database is disabled")
	}
}))
var MigrationModule = fx.Module("migration", fx.Invoke(runStartupMigration))
var CacheModule = fx.Module("cache", fx.Provide(newRedis, newLocker), fx.Invoke(func(client *redis.Client, logger *slog.Logger) {
	if client == nil {
		logger.Warn("redis is disabled")
	}
}))

func newImportWorker(repository importjob.Repository, transactor *database.Transactor, storage importjob.Storage, provider importjob.Provider, cfg config.Config) *importjob.Worker {
	return importjob.NewWorker(repository, transactor, storage, provider, cfg.Import.BatchSize, cfg.Import.MaxRows, cfg.Import.MaxBytes, cfg.Import.JobTimeout, cfg.Import.ResultTTL)
}

func newImportService(repository importjob.Repository, transactor *database.Transactor, storage importjob.Storage, cfg config.Config, applications appaccess.Verifier, provider importjob.Provider) (*importjob.Service, error) {
	return importjob.NewRuntimeService(repository, transactor, storage, cfg.Import.UploadTTL, applications, provider)
}

var ImportModule = fx.Module("import", fx.Provide(objectstorage.New, importjob.NewRepository, importjob.NewProvider, importjob.NewRuntimeCatalog, newImportService, newImportWorker))
