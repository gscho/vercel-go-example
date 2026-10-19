// Package oidc finds the Vercel OIDC token used to authenticate with Vercel Queues.
package oidc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
)

type contextKey struct{}

// Token returns the current Vercel OIDC token, checking the same sources in the same
// order as @vercel/oidc:
//   - the x-vercel-oidc-token request header that Vercel sends to functions
//   - the file named by VERCEL_OIDC_TOKEN_FILE, which Vercel refreshes for daemons
//   - VERCEL_OIDC_TOKEN, which `vercel env pull` writes for local development
//
// The file is read on every call because Vercel rotates its contents.
func Token(ctx context.Context) (string, error) {
	if token, ok := ctx.Value(contextKey{}).(string); ok && token != "" {
		return token, nil
	}

	if path := os.Getenv("VERCEL_OIDC_TOKEN_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read OIDC token file: %w", err)
		}
		token := strings.TrimSpace(string(data))
		if token == "" {
			return "", fmt.Errorf("OIDC token file %s is empty", path)
		}
		return token, nil
	}

	if token := os.Getenv("VERCEL_OIDC_TOKEN"); token != "" {
		return token, nil
	}
	return "", errors.New("no Vercel OIDC token found")
}

// Middleware stores the request's x-vercel-oidc-token header on the request context.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token := r.Header.Get("x-vercel-oidc-token"); token != "" {
			r = r.WithContext(context.WithValue(r.Context(), contextKey{}, token))
		}
		next.ServeHTTP(w, r)
	})
}
