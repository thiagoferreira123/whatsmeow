# whatsmeow REST server — build context = REPO ROOT (restserver importa o módulo
# local go.mau.fi/whatsmeow). No Coolify: Base Directory "/", Dockerfile "restserver/Dockerfile".
# CGO-free (modernc.org/sqlite) → binário estático, roda em qualquer arch (host é arm64/Graviton).

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/restserver ./restserver

FROM alpine:3.20
# Coolify's container health check uses curl against /health. Keep it in the
# runtime image so Docker health reflects the service instead of a missing tool.
RUN apk add --no-cache ca-certificates tzdata curl && mkdir -p /data
WORKDIR /app
COPY --from=build /out/restserver /app/restserver
# Sessões/keys do WhatsApp em /data (volume persistente — sobrevive a redeploys).
# GOMEMLIMIT (soft cap do GC) segura o heap antes de um OOM-kill derrubar todas
# as instâncias de uma vez — a EC2 é compartilhada com o prod (DietSystem/Pulso).
ENV PORT=8080 \
    GOMEMLIMIT=1750MiB \
    SEND_RATE_PER_MINUTE=30 \
    SEND_BURST=5 \
    SEND_RECIPIENT_COOLDOWN_SECONDS=10 \
    SEND_RECIPIENT_DAILY_MAX=20 \
    SEND_SERVICE_WINDOW_HOURS=24 \
    SEND_REQUIRE_LOCAL_CONSENT=false \
    GLOBAL_SEND_CONCURRENCY=8 \
    QUEUE_WORKERS=4 \
    QUEUE_POLL_MILLISECONDS=500 \
    QUEUE_MAX_ATTEMPTS=5 \
    QUEUE_RETRY_MAX_SECONDS=300 \
    RESET_COOLDOWN_SECONDS=60 \
    INSTANCE_LOG_RETENTION_DAYS=7 \
    INSTANCE_LOG_CLEANUP_INTERVAL_MINUTES=60 \
    RUNTIME_LEASE_TTL_SECONDS=30 \
    RUNTIME_LEASE_RETRY_SECONDS=2 \
    WHATSMEOW_DSN="file:/data/whatsmeow.db?_txlock=immediate&_pragma=foreign_keys(on)&_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/app/restserver"]
