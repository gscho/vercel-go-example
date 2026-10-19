package store

import "time"

type Monitor struct {
	ID              uint          `gorm:"primaryKey" json:"id"`
	Name            string        `gorm:"type:text;not null" json:"name"`
	URL             string        `gorm:"type:text;not null" json:"url"`
	IntervalSeconds int           `gorm:"not null" json:"intervalSeconds"`
	Paused          bool          `gorm:"not null" json:"paused"`
	NextCheckAt     time.Time     `gorm:"not null;index" json:"nextCheckAt"`
	CreatedAt       time.Time     `json:"createdAt"`
	UpdatedAt       time.Time     `json:"updatedAt"`
	Checks          []CheckResult `gorm:"constraint:OnDelete:CASCADE" json:"-"`
}

type CheckResult struct {
	ID        uint `gorm:"primaryKey" json:"id"`
	MonitorID uint `gorm:"not null;index:idx_check_results_monitor_checked_at,priority:1" json:"monitorId"`
	// MessageID is the Vercel Queues message that triggered the check. It is unique so a
	// redelivered message can't record the same check twice.
	MessageID  string    `gorm:"type:text;not null;uniqueIndex" json:"-"`
	Trigger    string    `gorm:"type:text;not null" json:"trigger"`
	CheckedAt  time.Time `gorm:"not null;index:idx_check_results_monitor_checked_at,priority:2" json:"checkedAt"`
	OK         bool      `gorm:"not null" json:"ok"`
	StatusCode *int      `json:"statusCode"`
	LatencyMs  int       `gorm:"not null" json:"latencyMs"`
	Error      *string   `gorm:"type:text" json:"error"`
}

type WorkerHeartbeat struct {
	InstanceID   string    `gorm:"primaryKey;type:text" json:"instanceId"`
	DeploymentID string    `gorm:"type:text;not null" json:"deploymentId"`
	StartedAt    time.Time `gorm:"not null" json:"startedAt"`
	LastSeenAt   time.Time `gorm:"not null;index" json:"lastSeenAt"`
}
