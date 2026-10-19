package store

import (
	"context"
	"errors"
	"time"

	"go.uber.org/fx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrNotFound = errors.New("not found")

// migrationLockID is an arbitrary advisory lock key shared by every process that migrates.
const migrationLockID = 727001

type Store struct {
	db *gorm.DB
}

// New registers migrations as a start hook, so the API and the worker both bring the
// schema up to date before they serve requests or process checks.
func New(lc fx.Lifecycle, db *gorm.DB) *Store {
	s := &Store{db: db}
	lc.Append(fx.Hook{OnStart: s.Migrate})
	return s
}

// Migrate holds a transaction-scoped advisory lock so API instances and the worker that
// start at the same time run AutoMigrate one after another instead of racing to create tables.
func (s *Store) Migrate(ctx context.Context) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", migrationLockID).Error; err != nil {
			return err
		}
		return tx.AutoMigrate(&Monitor{}, &CheckResult{}, &WorkerHeartbeat{})
	})
}

func (s *Store) ListMonitors(ctx context.Context) ([]Monitor, error) {
	var monitors []Monitor
	err := s.db.WithContext(ctx).Order("created_at").Find(&monitors).Error
	return monitors, err
}

func (s *Store) GetMonitor(ctx context.Context, id uint) (Monitor, error) {
	var monitor Monitor
	err := s.db.WithContext(ctx).First(&monitor, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Monitor{}, ErrNotFound
	}
	return monitor, err
}

func (s *Store) CreateMonitor(ctx context.Context, monitor *Monitor) error {
	monitor.NextCheckAt = time.Now()
	return s.db.WithContext(ctx).Create(monitor).Error
}

type MonitorUpdate struct {
	Name            *string
	URL             *string
	IntervalSeconds *int
	Paused          *bool
}

func (s *Store) UpdateMonitor(ctx context.Context, id uint, update MonitorUpdate) (Monitor, error) {
	changes := map[string]any{}
	if update.Name != nil {
		changes["name"] = *update.Name
	}
	if update.URL != nil {
		changes["url"] = *update.URL
	}
	if update.IntervalSeconds != nil {
		changes["interval_seconds"] = *update.IntervalSeconds
	}
	if update.Paused != nil {
		changes["paused"] = *update.Paused
		if !*update.Paused {
			changes["next_check_at"] = time.Now()
		}
	}

	if len(changes) > 0 {
		result := s.db.WithContext(ctx).Model(&Monitor{}).Where("id = ?", id).Updates(changes)
		if result.Error != nil {
			return Monitor{}, result.Error
		}
		if result.RowsAffected == 0 {
			return Monitor{}, ErrNotFound
		}
	}
	return s.GetMonitor(ctx, id)
}

func (s *Store) DeleteMonitor(ctx context.Context, id uint) error {
	result := s.db.WithContext(ctx).Delete(&Monitor{}, id)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// ClaimDueMonitors moves each due monitor's next check forward by its interval and returns
// the claimed IDs. SKIP LOCKED lets several worker instances claim at once without
// picking the same monitor.
func (s *Store) ClaimDueMonitors(ctx context.Context, limit int) ([]uint, error) {
	var ids []uint
	err := s.db.WithContext(ctx).Raw(`
		UPDATE monitors
		SET next_check_at = now() + make_interval(secs => interval_seconds)
		WHERE id IN (
			SELECT id FROM monitors
			WHERE NOT paused AND next_check_at <= now()
			ORDER BY next_check_at
			LIMIT ?
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id`, limit).Scan(&ids).Error
	return ids, err
}

func (s *Store) RecordCheck(ctx context.Context, result *CheckResult) error {
	return s.db.WithContext(ctx).
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "message_id"}}, DoNothing: true}).
		Create(result).Error
}

// ChecksSince returns a monitor's checks newer than since, newest first.
func (s *Store) ChecksSince(ctx context.Context, monitorID uint, since time.Time) ([]CheckResult, error) {
	var checks []CheckResult
	err := s.db.WithContext(ctx).
		Where("monitor_id = ? AND checked_at > ?", monitorID, since).
		Order("checked_at DESC").
		Find(&checks).Error
	return checks, err
}

// LatestChecks returns up to perMonitor of each monitor's newest checks since the given
// time, newest first, keyed by monitor ID.
func (s *Store) LatestChecks(ctx context.Context, since time.Time, perMonitor int) (map[uint][]CheckResult, error) {
	var checks []CheckResult
	err := s.db.WithContext(ctx).Raw(`
		SELECT id, monitor_id, message_id, trigger, checked_at, ok, status_code, latency_ms, error
		FROM (
			SELECT *, row_number() OVER (PARTITION BY monitor_id ORDER BY checked_at DESC) AS position
			FROM check_results
			WHERE checked_at > ?
		) ranked
		WHERE position <= ?
		ORDER BY monitor_id, checked_at DESC`, since, perMonitor).Scan(&checks).Error
	if err != nil {
		return nil, err
	}

	byMonitor := make(map[uint][]CheckResult)
	for _, check := range checks {
		byMonitor[check.MonitorID] = append(byMonitor[check.MonitorID], check)
	}
	return byMonitor, nil
}

// UptimeSince returns the share of successful checks for each monitor, from 0 to 1.
func (s *Store) UptimeSince(ctx context.Context, since time.Time) (map[uint]float64, error) {
	var rows []struct {
		MonitorID uint
		Uptime    float64
	}
	err := s.db.WithContext(ctx).Raw(`
		SELECT monitor_id, avg(CASE WHEN ok THEN 1 ELSE 0 END)::float8 AS uptime
		FROM check_results
		WHERE checked_at > ?
		GROUP BY monitor_id`, since).Scan(&rows).Error
	if err != nil {
		return nil, err
	}

	uptime := make(map[uint]float64, len(rows))
	for _, row := range rows {
		uptime[row.MonitorID] = row.Uptime
	}
	return uptime, nil
}

func (s *Store) RecordHeartbeat(ctx context.Context, heartbeat WorkerHeartbeat) error {
	return s.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "instance_id"}},
			DoUpdates: clause.AssignmentColumns([]string{"last_seen_at"}),
		}).
		Create(&heartbeat).Error
}

func (s *Store) DeleteHeartbeat(ctx context.Context, instanceID string) error {
	return s.db.WithContext(ctx).Delete(&WorkerHeartbeat{}, "instance_id = ?", instanceID).Error
}

func (s *Store) ActiveWorkers(ctx context.Context, seenSince time.Time) ([]WorkerHeartbeat, error) {
	var workers []WorkerHeartbeat
	err := s.db.WithContext(ctx).
		Where("last_seen_at > ?", seenSince).
		Order("started_at").
		Find(&workers).Error
	return workers, err
}
