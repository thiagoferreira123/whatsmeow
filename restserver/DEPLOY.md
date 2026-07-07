# Deploy de produção — `zap.dietsystem.com.br`

Serviço "WhatsApp" (whatsmeow/restserver) em produção, **isolado**, na EC2/Coolify
(`coolify.dietsystem.com.br`), sem interferir em DietSystem/Pulso.

- **URL:** https://zap.dietsystem.com.br (painel + API) · health: `/health` · webhook verify: `/webhook`
- **Coolify:** projeto `WhatsApp` (`wj25thew94sxz3dfhh7pumhn`), app `whatsapp-zap` (`ww1t3zwj4d1q00ez6ur0d0oe`),
  server localhost (EC2), build = Dockerfile (`restserver/Dockerfile`, base dir `/`), porta 8080, 1 instância.
- **CI/CD:** push em `main` do repo `thiagoferreira123/whatsmeow` → GitHub webhook →
  `https://coolify.dietsystem.com.br/webhooks/source/github/events/manual` → rebuild + redeploy automático.
- **Persistência:** named volume `whatsmeow-zap-data` montado em `/data` (sessões/keys do WhatsApp sobrevivem a redeploys);
  SQLite WAL em `/data/whatsmeow.db`.
- **Env vars (Coolify):** `ADMIN_API_KEY` (auth do painel/API), `WEBHOOK_SECRET` (HMAC default do webhook global),
  `WATCHDOG_SECONDS=30`, `PORT=8080`. (`WHATSMEOW_DSN` vem do Dockerfile apontando p/ `/data`.)
- **Tuning de escala (defaults no código/Dockerfile, override por env):** `CONNECT_CONCURRENCY=8`
  (máx. de `Connect()` simultâneos no boot/watchdog — evita thundering herd com centenas de instâncias),
  `DB_MAX_CONNS=8` (pool SQLite), `GOMEMLIMIT=1750MiB` (soft cap do GC; a EC2 é compartilhada com o prod).
  SQLite: WAL + `synchronous(NORMAL)` + `busy_timeout(30000)` + `_txlock=immediate` (sem SQLITE_BUSY sob carga).
  Watchdog usa backoff exponencial c/ jitter (30s→10min); temp-ban (402) espera o ban expirar; client-outdated
  (405) loga alto e tenta de hora em hora (= atualizar a lib whatsmeow). SIGTERM desconecta os sockets
  limpo antes de sair (redeploy não deixa sessão suja).
- **Webhook global (WhatsApp Cloud API):** configurável no painel (URL destino + verify token + app secret).
  Entrega mensagens e eventos confirmado(1)/cancelado(2) no envelope oficial, assinado em `X-Hub-Signature-256`.
- **DNS:** `zap.dietsystem.com.br` A → `54.207.254.146` (DigitalOcean); TLS Let's Encrypt automático (Traefik/Coolify).

Redeploy manual: `GET https://coolify.dietsystem.com.br/api/v1/deploy?uuid=ww1t3zwj4d1q00ez6ur0d0oe` (Bearer COOLIFY_API_TOKEN).
