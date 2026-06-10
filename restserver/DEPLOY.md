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
- **Webhook global (WhatsApp Cloud API):** configurável no painel (URL destino + verify token + app secret).
  Entrega mensagens e eventos confirmado(1)/cancelado(2) no envelope oficial, assinado em `X-Hub-Signature-256`.
- **DNS:** `zap.dietsystem.com.br` A → `54.207.254.146` (DigitalOcean); TLS Let's Encrypt automático (Traefik/Coolify).

Redeploy manual: `GET https://coolify.dietsystem.com.br/api/v1/deploy?uuid=ww1t3zwj4d1q00ez6ur0d0oe` (Bearer COOLIFY_API_TOKEN).
