// Package checks defines the check job that the API and the worker exchange through
// Vercel Queues, and the HTTP probe the worker runs for each job.
package checks

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gscho/vercel-go-example/api/internal/queue"
)

const (
	Topic         = "monitor-checks"
	ConsumerGroup = "worker"

	TriggerScheduled = "scheduled"
	TriggerManual    = "manual"
)

type Job struct {
	MonitorID uint   `json:"monitorId"`
	Trigger   string `json:"trigger"`
}

func Enqueue(ctx context.Context, q *queue.Client, job Job) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	_, err = q.Send(ctx, Topic, body)
	return err
}

type Outcome struct {
	OK         bool
	StatusCode *int
	Latency    time.Duration
	Err        error
}

type Checker struct {
	client *http.Client
}

func NewChecker() *Checker {
	// A fresh connection per check measures DNS, TCP, and TLS time the way a new visitor
	// would experience it, instead of reusing a warm connection.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	return &Checker{client: &http.Client{Timeout: 10 * time.Second, Transport: transport}}
}

// Check requests the URL and treats any response below 400 as up. Latency is measured
// until the response headers arrive.
func (c *Checker) Check(ctx context.Context, url string) Outcome {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Outcome{Err: err}
	}
	req.Header.Set("User-Agent", "vercel-go-example-uptime/1.0")

	start := time.Now()
	resp, err := c.client.Do(req)
	latency := time.Since(start)
	if err != nil {
		return Outcome{Latency: latency, Err: err}
	}
	defer resp.Body.Close()

	statusCode := resp.StatusCode
	outcome := Outcome{OK: statusCode < 400, StatusCode: &statusCode, Latency: latency}
	if !outcome.OK {
		outcome.Err = errors.New(resp.Status)
	}
	return outcome
}
