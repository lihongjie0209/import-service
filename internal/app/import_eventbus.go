package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/import-service/internal/config"
	"github.com/lihongjie0209/import-service/internal/eventbus"
	"github.com/lihongjie0209/import-service/internal/importjob"
	platformeventbus "github.com/lihongjie0209/microservice-platform-go/eventbus"
	platformoutbox "github.com/lihongjie0209/microservice-platform-go/outbox"
	importv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/import/v1"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
)

type importProcessor interface {
	Validate(context.Context, string, string, string) error
	Apply(context.Context, string, string, string) error
}
type importEventRuntime struct {
	cfg    config.Config
	store  *platformoutbox.SQLStore
	bus    *eventbus.Bus
	worker importProcessor
	logger *slog.Logger
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newImportOutboxStore(db *sqlx.DB) (*platformoutbox.SQLStore, error) {
	if db == nil {
		return nil, nil
	}
	return platformoutbox.NewSQLStore(db, "import_outbox_events")
}
func newImportEventRuntime(lifecycle fx.Lifecycle, cfg config.Config, store *platformoutbox.SQLStore, bus *eventbus.Bus, worker *importjob.Worker, logger *slog.Logger) *importEventRuntime {
	runtime := &importEventRuntime{cfg: cfg, store: store, bus: bus, worker: worker, logger: logger}
	lifecycle.Append(fx.Hook{OnStart: runtime.start, OnStop: runtime.stop})
	return runtime
}
func (r *importEventRuntime) start(context.Context) error {
	if !r.cfg.EventBus.Enabled {
		return nil
	}
	if r.store == nil || r.bus == nil {
		return errors.New("enabled event bus requires database outbox and JetStream")
	}
	dispatcher, err := platformoutbox.New(r.store, r.bus, platformoutbox.Config{BatchSize: r.cfg.EventBus.DispatchBatchSize, Lease: r.cfg.EventBus.DispatchLease, RetryDelay: r.cfg.EventBus.DispatchRetryDelay})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	cleaner, err := platformoutbox.NewRetentionCleaner(r.store, platformoutbox.RetentionConfig{Retention: r.cfg.EventBus.PublishedRetention, BatchSize: r.cfg.EventBus.CleanupBatchSize})
	if err != nil {
		cancel()
		return err
	}
	r.wg.Go(func() {
		ticker := time.NewTicker(r.cfg.EventBus.DispatchInterval)
		defer ticker.Stop()
		for {
			if _, runErr := dispatcher.RunOnce(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				r.logger.ErrorContext(ctx, "dispatch import outbox failed", "error", runErr)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	r.wg.Go(func() {
		ticker := time.NewTicker(r.cfg.EventBus.CleanupInterval)
		defer ticker.Stop()
		for {
			if deleted, runErr := cleaner.RunOnce(ctx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				r.logger.ErrorContext(ctx, "clean published import outbox events", "error", runErr)
			} else if deleted > 0 {
				r.logger.InfoContext(ctx, "published import outbox events cleaned", "deleted", deleted)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	r.consume(ctx, "import-validation-worker-v1", "platform.import.job.requested.v1", "requested")
	r.consume(ctx, "import-apply-worker-v1", "platform.import.job.apply-requested.v1", "apply-requested")
	return nil
}
func (r *importEventRuntime) consume(ctx context.Context, durable, subject, change string) {
	r.wg.Go(func() {
		err := r.bus.ConsumeWithOptions(ctx, platformeventbus.ConsumerOptions{Durable: durable, FilterSubject: subject, Handler: func(handlerCtx context.Context, envelope *eventbus.Envelope) error {
			return r.handle(handlerCtx, envelope, change)
		}, OnError: func(err error) {
			r.logger.ErrorContext(ctx, "consume import request failed", "subject", subject, "error", err)
		}})
		if err != nil && !errors.Is(err, context.Canceled) {
			r.logger.ErrorContext(ctx, "import consumer stopped", "subject", subject, "error", err)
		}
	})
}
func (r *importEventRuntime) handle(ctx context.Context, envelope *eventbus.Envelope, expected string) error {
	payload := new(importv1.ImportJobChangedEvent)
	if err := proto.Unmarshal(envelope.GetPayload(), payload); err != nil {
		return err
	}
	job := payload.GetJob()
	if payload.GetChangeType() != expected || job == nil {
		return errors.New("invalid import job event")
	}
	if job.GetTenantId() == "" || job.GetApplicationId() == "" || envelope.GetTenantId() != job.GetTenantId() || envelope.GetApplicationId() != job.GetApplicationId() {
		return errors.New("import job event scope mismatch")
	}
	if expected == "requested" {
		return r.worker.Validate(ctx, job.GetTenantId(), job.GetApplicationId(), job.GetId())
	}
	return r.worker.Apply(ctx, job.GetTenantId(), job.GetApplicationId(), job.GetId())
}
func (r *importEventRuntime) stop(context.Context) error {
	if r.cancel != nil {
		r.cancel()
		r.wg.Wait()
	}
	return nil
}

var ImportEventModule = fx.Module("import-event-runtime", fx.Provide(newImportOutboxStore, newImportEventRuntime), fx.Invoke(func(*importEventRuntime) {}))
