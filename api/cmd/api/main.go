// Command api is the HTTP API. Vercel's Go preset finds it at cmd/api/main.go and runs
// it as the api service.
package main

import (
	"net/http"

	"go.uber.org/fx"

	"github.com/gscho/vercel-go-example/api/internal/app"
	"github.com/gscho/vercel-go-example/api/internal/httpapi"
)

func main() {
	fx.New(
		app.Core,
		fx.Provide(httpapi.NewHandlers, httpapi.NewRouter, httpapi.NewServer),
		fx.Invoke(func(*http.Server) {}),
	).Run()
}
