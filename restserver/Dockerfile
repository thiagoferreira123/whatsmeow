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
RUN apk add --no-cache ca-certificates tzdata && mkdir -p /data
WORKDIR /app
COPY --from=build /out/restserver /app/restserver
# Sessões/keys do WhatsApp em /data (volume persistente — sobrevive a redeploys).
# GOMEMLIMIT (soft cap do GC) segura o heap antes de um OOM-kill derrubar todas
# as instâncias de uma vez — a EC2 é compartilhada com o prod (DietSystem/Pulso).
ENV PORT=8080 \
    GOMEMLIMIT=1750MiB \
    WHATSMEOW_DSN="file:/data/whatsmeow.db?_txlock=immediate&_pragma=foreign_keys(on)&_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["/app/restserver"]
