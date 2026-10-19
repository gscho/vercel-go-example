# Go on Vercel: uptime monitor

An internal demo of a Go backend on Vercel. It monitors HTTP endpoints and shows their
status, response times, and check history.

| Part | Path | Runs as |
| --- | --- | --- |
| UI | `ui/` | Next.js [service](https://vercel.com/docs/services) |
| API | `api/cmd/api` | Go service built by the Go buildpack (`VERCEL_EXPERIMENTAL_BUILDPACK_GO=1`) |
| Worker | `api/cmd/worker` | [Vercel Daemon](https://vercel.com/docs/daemons) built from `api/Dockerfile` |

The API and the worker share one Go module: chi for routing, GORM with Neon Postgres for
storage, uber fx for dependency injection and lifecycle, and zerolog for JSON logs.

## How it works

```text
browser ──/api/*──▶ api (Go) ──▶ Postgres ◀── worker (daemon)
                      │                          ▲  │
                      └─ "Check now" ─▶ Vercel Queues ◀─ scheduled checks
```

1. Every 5 seconds the worker claims monitors whose next check is due
   (`FOR UPDATE SKIP LOCKED`, so more instances can share the work) and sends one
   message per check to the `monitor-checks` topic in Vercel Queues.
2. **Check now** in the UI calls the API, which sends the same message.
3. The worker polls the topic, runs up to 10 checks at once, stores each result, and
   acknowledges the message. Results are unique per message ID, so a redelivered message
   doesn't record a check twice.
4. The worker writes a heartbeat every 15 seconds, which the UI uses to show whether a
   worker is running.

Daemons can't receive traffic and Go services can't use service bindings yet, so the
API and the worker only communicate through Postgres and Vercel Queues. Both
authenticate with the Vercel OIDC token: the API reads the `x-vercel-oidc-token` request
header, and the worker reads the file at `VERCEL_OIDC_TOKEN_FILE`.

When Vercel stops a worker instance it sends `SIGTERM`. The worker stops taking new
messages, finishes the checks it's running, removes its heartbeat, and exits within
the 30 second `terminationGracePeriodSeconds`.

Both processes run GORM `AutoMigrate` at startup inside a transaction that holds a
Postgres advisory lock, so instances that start at the same time don't race to create
tables.

## Environment variables

| Name | Used by | Notes |
| --- | --- | --- |
| `DATABASE_URL` | API, worker | Set by the Neon integration. The pooled connection string works. |
| `QUEUE_REGION` | API, worker | Optional. Defaults to `iad1`. |
| `VERCEL_OIDC_TOKEN` | API, worker | Local development only. Written by `vercel env pull`. |

## Run it locally

```bash
vercel link
vercel env pull .env.local

# UI and API, with the same routing as production
vercel dev

# Worker, in another terminal
cd api
set -a; source ../.env.local; set +a
LOG_FORMAT=console go run ./cmd/worker
```

`vercel dev` doesn't run daemons, so start the worker yourself, either with `go run` as
above or with Docker:

```bash
docker build -t uptime-worker api/
docker run --env-file .env.local uptime-worker
```

`vercel dev` needs a recent Vercel CLI. 62.5.0 rejects the `daemons` config and 64.5.0
accepts it. The OIDC token from `vercel env pull` expires, so pull again if queue
requests start failing with `401`.

## Deploy

1. Create the project, connect a Neon database from the Marketplace, and enable
   Deployment Protection for all deployments.
2. Deploy to production with `vercel --prod`.

Daemons only run in production deployments. Preview deployments serve the UI and API,
and the UI reports that no worker is running.

## Tests

```bash
cd api && go test ./...
cd ui && npm run lint && npm run build
```
