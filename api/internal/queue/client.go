// Package queue is a minimal client for the Vercel Queues HTTP API.
// See https://vercel.com/docs/queues/api.
package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gscho/vercel-go-example/api/internal/oidc"
)

type Client struct {
	baseURL string
	http    *http.Client
}

type Message struct {
	ID            string
	ReceiptHandle string
	DeliveryCount int
	Body          []byte
}

// NewClient talks to the queue in one region. Messages can only be received from the
// region they were sent to.
func NewClient(region string) *Client {
	return &Client{
		baseURL: fmt.Sprintf("https://%s.vercel-queue.com/api/v3", region),
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// Send publishes a JSON message. The returned ID is empty when Vercel accepted the
// message but deferred delivery during a regional failover.
func (c *Client) Send(ctx context.Context, topic string, body []byte) (string, error) {
	req, err := c.newRequest(ctx, http.MethodPost, "/topic/"+url.PathEscape(topic), bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		var payload struct {
			MessageID string `json:"messageId"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return "", fmt.Errorf("decode send response: %w", err)
		}
		return payload.MessageID, nil
	case http.StatusAccepted:
		return "", nil
	default:
		return "", responseError("send", resp)
	}
}

// Receive leases up to maxMessages messages for the consumer group. It returns right
// away, with no messages when the topic is empty.
func (c *Client) Receive(ctx context.Context, topic, consumer string, maxMessages int, visibility time.Duration) ([]Message, error) {
	req, err := c.newRequest(ctx, http.MethodPost, consumerPath(topic, consumer), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "multipart/mixed")
	req.Header.Set("Vqs-Max-Messages", strconv.Itoa(maxMessages))
	req.Header.Set("Vqs-Visibility-Timeout-Seconds", strconv.Itoa(int(visibility.Seconds())))

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, responseError("receive", resp)
	}
	return readMessages(resp)
}

// Ack removes a processed message from the consumer group.
func (c *Client) Ack(ctx context.Context, topic, consumer, receiptHandle string) error {
	path := consumerPath(topic, consumer) + "/lease/" + url.PathEscape(receiptHandle)
	req, err := c.newRequest(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return responseError("ack", resp)
	}
	return nil
}

func (c *Client) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	token, err := oidc.Token(ctx)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return req, nil
}

func consumerPath(topic, consumer string) string {
	return "/topic/" + url.PathEscape(topic) + "/consumer/" + url.PathEscape(consumer)
}

// readMessages parses a multipart/mixed response, where each part is one message and the
// Vqs-* part headers carry its metadata.
func readMessages(resp *http.Response) ([]Message, error) {
	_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return nil, fmt.Errorf("parse receive content type: %w", err)
	}

	reader := multipart.NewReader(resp.Body, params["boundary"])
	var messages []Message
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			return messages, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read message part: %w", err)
		}

		body, err := io.ReadAll(part)
		if err != nil {
			return nil, fmt.Errorf("read message body: %w", err)
		}
		deliveryCount, _ := strconv.Atoi(part.Header.Get("Vqs-Delivery-Count"))
		messages = append(messages, Message{
			ID:            part.Header.Get("Vqs-Message-Id"),
			ReceiptHandle: part.Header.Get("Vqs-Receipt-Handle"),
			DeliveryCount: deliveryCount,
			Body:          body,
		})
	}
}

func responseError(operation string, resp *http.Response) error {
	detail, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return fmt.Errorf("queue %s: %s: %s", operation, resp.Status, bytes.TrimSpace(detail))
}
