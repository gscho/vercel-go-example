package worker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"go.uber.org/fx"

	"github.com/gscho/vercel-go-example/api/internal/checks"
	"github.com/gscho/vercel-go-example/api/internal/queue"
	"github.com/gscho/vercel-go-example/api/internal/store"
)

const (
	scheduleEvery  = 5 * time.Second
	heartbeatEvery = 15 * time.Second
	minIdlePoll    = 1 * time.Second
	maxIdlePoll    = 5 * time.Second
	batchSize      = 10
	// visibilityTimeout must outlast a check (10s timeout) plus recording the result.
	visibilityTimeout = 60 * time.Second
)

// Worker runs three loops: one sends a check job for each monitor that is due, one
// receives jobs from Vercel Queues and runs them, and one writes a heartbeat so the UI
// can show whether a worker is running.
type Worker struct {
	store        *store.Store
	queue        *queue.Client
	checker      *checks.Checker
	log          zerolog.Logger
	instanceID   string
	deploymentID string
	startedAt    time.Time

	cancel context.CancelFunc
	loops  sync.WaitGroup
}

func New(s *store.Store, q *queue.Client, checker *checks.Checker, log zerolog.Logger) *Worker {
	instanceID, err := os.Hostname()
	if err != nil || instanceID == "" {
		instanceID = "worker-" + rand.Text()[:8]
	}
	return &Worker{
		store:        s,
		queue:        q,
		checker:      checker,
		log:          log.With().Str("instance", instanceID).Logger(),
		instanceID:   instanceID,
		deploymentID: os.Getenv("VERCEL_DEPLOYMENT_ID"),
	}
}

func Register(lc fx.Lifecycle, w *Worker) {
	lc.Append(fx.Hook{OnStart: w.Start, OnStop: w.Stop})
}

func (w *Worker) Start(context.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.startedAt = time.Now()

	w.loops.Go(func() { w.heartbeatLoop(ctx) })
	w.loops.Go(func() { w.scheduleLoop(ctx) })
	w.loops.Go(func() { w.consumeLoop(ctx) })
	w.log.Info().Msg("worker started")
	return nil
}

// Stop runs when Vercel sends SIGTERM. The loops stop taking new work, checks already
// running finish and are acknowledged, and then the heartbeat is removed.
func (w *Worker) Stop(ctx context.Context) error {
	w.cancel()

	done := make(chan struct{})
	go func() {
		w.loops.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	w.log.Info().Msg("worker stopped")
	return w.store.DeleteHeartbeat(ctx, w.instanceID)
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(heartbeatEvery)
	defer ticker.Stop()
	for {
		heartbeat := store.WorkerHeartbeat{
			InstanceID:   w.instanceID,
			DeploymentID: w.deploymentID,
			StartedAt:    w.startedAt,
			LastSeenAt:   time.Now(),
		}
		if err := w.store.RecordHeartbeat(ctx, heartbeat); err != nil && ctx.Err() == nil {
			w.log.Error().Err(err).Msg("record heartbeat")
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) scheduleLoop(ctx context.Context) {
	ticker := time.NewTicker(scheduleEvery)
	defer ticker.Stop()
	for {
		w.scheduleDueMonitors(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) scheduleDueMonitors(ctx context.Context) {
	ids, err := w.store.ClaimDueMonitors(ctx, 100)
	if err != nil {
		if ctx.Err() == nil {
			w.log.Error().Err(err).Msg("claim due monitors")
		}
		return
	}

	for _, id := range ids {
		job := checks.Job{MonitorID: id, Trigger: checks.TriggerScheduled}
		if err := checks.Enqueue(ctx, w.queue, job); err != nil {
			w.log.Error().Err(err).Uint("monitor", id).Msg("enqueue scheduled check")
		}
	}
}

// consumeLoop polls the queue, backing off while it is empty because ReceiveMessages
// returns immediately instead of waiting for a message.
func (w *Worker) consumeLoop(ctx context.Context) {
	idle := minIdlePoll
	for {
		messages, err := w.queue.Receive(ctx, checks.Topic, checks.ConsumerGroup, batchSize, visibilityTimeout)
		if err != nil && ctx.Err() == nil {
			w.log.Error().Err(err).Msg("receive checks")
		}

		if len(messages) > 0 {
			w.processBatch(ctx, messages)
			idle = minIdlePoll
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(idle):
		}
		idle = min(idle*2, maxIdlePoll)
	}
}

func (w *Worker) processBatch(ctx context.Context, messages []queue.Message) {
	// Shutdown cancels ctx, but a check that is already running should still finish and
	// be recorded rather than show up as a failure.
	checkCtx := context.WithoutCancel(ctx)

	var batch sync.WaitGroup
	for _, message := range messages {
		batch.Go(func() { w.process(checkCtx, message) })
	}
	batch.Wait()
}

func (w *Worker) process(ctx context.Context, message queue.Message) {
	log := w.log.With().Str("messageId", message.ID).Int("delivery", message.DeliveryCount).Logger()

	var job checks.Job
	if err := json.Unmarshal(message.Body, &job); err != nil {
		log.Error().Err(err).Msg("drop malformed check job")
		w.ack(ctx, message, log)
		return
	}

	monitor, err := w.store.GetMonitor(ctx, job.MonitorID)
	if errors.Is(err, store.ErrNotFound) {
		w.ack(ctx, message, log)
		return
	}
	if err != nil {
		// Leave the message unacknowledged so Vercel Queues redelivers it.
		log.Error().Err(err).Msg("load monitor")
		return
	}

	outcome := w.checker.Check(ctx, monitor.URL)
	result := store.CheckResult{
		MonitorID:  monitor.ID,
		MessageID:  message.ID,
		Trigger:    job.Trigger,
		CheckedAt:  time.Now(),
		OK:         outcome.OK,
		StatusCode: outcome.StatusCode,
		LatencyMs:  int(outcome.Latency.Milliseconds()),
	}
	if outcome.Err != nil {
		errMessage := outcome.Err.Error()
		result.Error = &errMessage
	}
	if err := w.store.RecordCheck(ctx, &result); err != nil {
		log.Error().Err(err).Msg("record check")
		return
	}

	w.ack(ctx, message, log)
	log.Info().
		Uint("monitor", monitor.ID).
		Str("trigger", job.Trigger).
		Bool("ok", result.OK).
		Int("latencyMs", result.LatencyMs).
		Msg("check complete")
}

func (w *Worker) ack(ctx context.Context, message queue.Message, log zerolog.Logger) {
	if err := w.queue.Ack(ctx, checks.Topic, checks.ConsumerGroup, message.ReceiptHandle); err != nil {
		log.Error().Err(err).Msg("ack check job")
	}
}
