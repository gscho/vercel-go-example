package queue

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSendReceiveAck(t *testing.T) {
	t.Setenv("VERCEL_OIDC_TOKEN_FILE", "")
	t.Setenv("VERCEL_OIDC_TOKEN", "test-token")

	const boundary = "vqs-boundary-test"
	mux := http.NewServeMux()
	mux.HandleFunc("POST /topic/checks", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q", got)
		}
		if body, _ := io.ReadAll(r.Body); string(body) != `{"monitorId":1}` {
			t.Errorf("body = %s", body)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"messageId":"msg_1"}`)
	})
	mux.HandleFunc("POST /topic/checks/consumer/worker", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Vqs-Max-Messages"); got != "10" {
			t.Errorf("Vqs-Max-Messages = %q", got)
		}
		w.Header().Set("Content-Type", "multipart/mixed; boundary="+boundary)
		_, _ = io.WriteString(w, "--"+boundary+"\r\n"+
			"Content-Type: application/json\r\n"+
			"Vqs-Message-Id: msg_1\r\n"+
			"Vqs-Receipt-Handle: rh/1+a\r\n"+
			"Vqs-Delivery-Count: 2\r\n\r\n"+
			`{"monitorId":1}`+"\r\n"+
			"--"+boundary+"--\r\n")
	})
	mux.HandleFunc("DELETE /topic/checks/consumer/worker/lease/{handle}", func(w http.ResponseWriter, r *http.Request) {
		if got := r.PathValue("handle"); got != "rh/1+a" {
			t.Errorf("receipt handle = %q", got)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	client := &Client{baseURL: server.URL, http: server.Client()}
	ctx := context.Background()

	id, err := client.Send(ctx, "checks", []byte(`{"monitorId":1}`))
	if err != nil || id != "msg_1" {
		t.Fatalf("Send = %q, %v", id, err)
	}

	messages, err := client.Receive(ctx, "checks", "worker", 10, time.Minute)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("got %d messages, want 1", len(messages))
	}
	message := messages[0]
	if message.ID != "msg_1" || message.ReceiptHandle != "rh/1+a" || message.DeliveryCount != 2 || string(message.Body) != `{"monitorId":1}` {
		t.Fatalf("unexpected message: %+v", message)
	}

	if err := client.Ack(ctx, "checks", "worker", message.ReceiptHandle); err != nil {
		t.Fatalf("Ack: %v", err)
	}
}

func TestReceiveEmptyTopic(t *testing.T) {
	t.Setenv("VERCEL_OIDC_TOKEN", "test-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := &Client{baseURL: server.URL, http: server.Client()}
	messages, err := client.Receive(context.Background(), "checks", "worker", 10, time.Minute)
	if err != nil || len(messages) != 0 {
		t.Fatalf("Receive = %v, %v; want no messages", messages, err)
	}
}
