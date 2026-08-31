package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/lihongjie0209/import-service/internal/config"
	"github.com/lihongjie0209/import-service/internal/importjob"
	"github.com/lihongjie0209/import-service/internal/observability"
	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
)

func New(lc fx.Lifecycle, cfg config.Config, worker *importjob.Worker, metrics *observability.Metrics, logger *slog.Logger) (*cron.Cron, error) {
	location, err := time.LoadLocation(cfg.Cron.Timezone)
	if err != nil {
		return nil, err
	}
	runner := cron.New(cron.WithLocation(location), cron.WithSeconds(), cron.WithChain(cron.Recover(cron.PrintfLogger(slogWriter{logger})), cron.SkipIfStillRunning(cron.PrintfLogger(slogWriter{logger}))))
	if cfg.Cron.ImportCleanupSpec != "" {
		if _, err := runner.AddFunc(cfg.Cron.ImportCleanupSpec, func() {
			started := time.Now()
			status := "success"
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			cleaned, runErr := worker.CleanupExpired(ctx, cfg.Cron.CleanupBatchSize)
			if runErr != nil {
				status = "error"
				logger.ErrorContext(ctx, "expired import cleanup failed", "error", runErr, "cleaned", cleaned)
			} else {
				logger.InfoContext(ctx, "expired import cleanup completed", "cleaned", cleaned)
			}
			metrics.ObserveCron("import_result_cleanup", status, started)
		}); err != nil {
			return nil, err
		}
	}
	lc.Append(fx.Hook{OnStart: func(context.Context) error {
		if cfg.Cron.Enabled {
			runner.Start()
			logger.Info("scheduler started")
		}
		return nil
	}, OnStop: func(ctx context.Context) error {
		stopCtx := runner.Stop()
		select {
		case <-stopCtx.Done():
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	return runner, nil
}

type slogWriter struct{ logger *slog.Logger }

func (w slogWriter) Printf(format string, args ...any) {
	w.logger.Error("scheduler event", "detail", format, "args", args)
}

var Module = fx.Module("scheduler", fx.Provide(New), fx.Invoke(func(*cron.Cron) {}))
