package httpapi

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"
	"go.uber.org/fx"

	"github.com/gscho/vercel-go-example/api/internal/config"
	"github.com/gscho/vercel-go-example/api/internal/oidc"
)

// NewRouter mounts every route under /api because Vercel passes the original request
// path through to the service.
func NewRouter(h *Handlers, log zerolog.Logger) http.Handler {
	r := chi.NewRouter()
	r.Use(hlog.NewHandler(log))
	r.Use(hlog.AccessHandler(func(r *http.Request, status, size int, duration time.Duration) {
		hlog.FromRequest(r).Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Int("status", status).
			Dur("duration", duration).
			Msg("request")
	}))
	r.Use(middleware.Recoverer)
	r.Use(oidc.Middleware)

	r.Route("/api", func(r chi.Router) {
		r.Get("/healthz", h.health)
		r.Get("/worker", h.workerStatus)
		r.Route("/monitors", func(r chi.Router) {
			r.Get("/", h.listMonitors)
			r.Post("/", h.createMonitor)
			r.Route("/{id}", func(r chi.Router) {
				r.Get("/", h.getMonitor)
				r.Patch("/", h.updateMonitor)
				r.Delete("/", h.deleteMonitor)
				r.Post("/check", h.checkNow)
			})
		})
	})
	return r
}

// NewServer listens on PORT, which Vercel sets for the Go service.
func NewServer(lc fx.Lifecycle, cfg config.Config, handler http.Handler, log zerolog.Logger) *http.Server {
	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			listener, err := net.Listen("tcp", server.Addr)
			if err != nil {
				return err
			}
			go func() {
				if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error().Err(err).Msg("server stopped unexpectedly")
				}
			}()
			log.Info().Str("addr", server.Addr).Msg("api listening")
			return nil
		},
		OnStop: server.Shutdown,
	})
	return server
}
