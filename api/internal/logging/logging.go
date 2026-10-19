package logging

import (
	"os"

	"github.com/rs/zerolog"
	"go.uber.org/fx/fxevent"
)

// New writes JSON logs, which Vercel's log viewer can parse. Set LOG_FORMAT=console
// for readable output during local development.
func New() zerolog.Logger {
	if os.Getenv("LOG_FORMAT") == "console" {
		return zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
	}
	return zerolog.New(os.Stdout).With().Timestamp().Logger()
}

// FxLogger reports fx lifecycle events through zerolog instead of fx's default console output.
type FxLogger struct {
	Log zerolog.Logger
}

func (l FxLogger) LogEvent(event fxevent.Event) {
	switch e := event.(type) {
	case *fxevent.OnStartExecuted:
		if e.Err != nil {
			l.Log.Error().Err(e.Err).Str("hook", e.FunctionName).Msg("start hook failed")
		}
	case *fxevent.OnStopExecuted:
		if e.Err != nil {
			l.Log.Error().Err(e.Err).Str("hook", e.FunctionName).Msg("stop hook failed")
		}
	case *fxevent.Provided:
		if e.Err != nil {
			l.Log.Error().Err(e.Err).Str("constructor", e.ConstructorName).Msg("provide failed")
		}
	case *fxevent.Invoked:
		if e.Err != nil {
			l.Log.Error().Err(e.Err).Str("function", e.FunctionName).Msg("invoke failed")
		}
	case *fxevent.Started:
		if e.Err != nil {
			l.Log.Error().Err(e.Err).Msg("start failed")
		} else {
			l.Log.Info().Msg("started")
		}
	case *fxevent.Stopping:
		l.Log.Info().Str("signal", e.Signal.String()).Msg("stopping")
	case *fxevent.Stopped:
		if e.Err != nil {
			l.Log.Error().Err(e.Err).Msg("stop failed")
		} else {
			l.Log.Info().Msg("stopped")
		}
	case *fxevent.RollingBack:
		l.Log.Error().Err(e.StartErr).Msg("start failed, rolling back")
	}
}
