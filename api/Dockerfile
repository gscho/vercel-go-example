# Builds the worker daemon. It lives at the api/ root so the build context includes go.mod
# and the internal packages it shares with the API.
FROM golang:1.25 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

# distroless/static includes the CA certificates the worker needs for HTTPS checks.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/worker /worker
ENTRYPOINT ["/worker"]
