// Command worker is the background worker that runs as a Vercel Daemon. It is built
// from api/Dockerfile.
package main

import (
	"time"

	"go.uber.org/fx"

	"github.com/gscho/vercel-go-example/api/internal/app"
	"github.com/gscho/vercel-go-example/api/internal/checks"
	"github.com/gscho/vercel-go-example/api/internal/worker"
)

func main() {
	fx.New(
		app.Core,
		fx.Provide(checks.NewChecker, worker.New),
		fx.Invoke(worker.Register),
		// Leaves time to finish running checks within the daemon's 30 second
		// terminationGracePeriodSeconds after SIGTERM.
		fx.StopTimeout(25*time.Second),
	).Run()
}
