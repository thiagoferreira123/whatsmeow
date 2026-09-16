# Deploy de produção — `zap.dietsystem.com.br`

Serviço "WhatsApp" (whatsmeow/restserver) em produção, **isolado**, na EC2/Coolify
(`coolify.dietsystem.com.br`), sem interferir em DietSystem/Pulso.

- **URL:** https://zap.dietsystem.com.br (painel + API) · health: `/health` · webhook verify: `/webhook`
- **Coolify:** projeto `WhatsApp` (`wj25thew94sxz3dfhh7pumhn`), app `whatsapp-zap` (`ww1t3zwj4d1q00ez6ur0d0oe`),
  server localhost (EC2), build = Dockerfile (`restserver/Dockerfile`, base dir `/`), porta 8080, 1 instância.
- **CI/CD:** push em `main` do repo `thiagoferreira123/whatsmeow` → GitHub webhook →
  `https://coolify.dietsystem.com.br/webhooks/source/github/events/manual` → rebuild + redeploy automático.
- **Persistência:** named volume **`ww1t3zwj4d1q00ez6ur0d0oe-whatsmeow-zap-data`** montado em `/data`
  (sessões/keys do WhatsApp sobrevivem a redeploys); SQLite WAL em `/data/whatsmeow.db`.
  ⚠️ O Coolify **prefixa o volume com o UUID do app**. Montar `whatsmeow-zap-data` (sem prefixo)
  faz o Docker criar um volume VAZIO em silêncio — em 16/09/2026 isso gerou um "backup" de 4 KB
  que ainda assim passava no `quick_check`. Sempre confirmar o nome real no container:
  ```bash
  CID=$(sudo -n docker ps -q --filter "name=ww1t3zwj4d1q00ez6ur0d0oe" | head -1)
  sudo -n docker inspect "$CID" --format '{{range .Mounts}}{{.Name}} -> {{.Destination}}{{println}}{{end}}'
  ```
  **Backup antes de deploy com migration** (o schema do whatsmeow é forward-only). Usar o backup
  online do SQLite, nunca `cp` (cópia crua com WAL ativo pode sair rasgada), e **verificar**:
  ```bash
  sudo -n docker run --rm -v ww1t3zwj4d1q00ez6ur0d0oe-whatsmeow-zap-data:/data -v /home/ubuntu/wa-backup:/backup \
    alpine sh -c 'apk add --no-cache sqlite >/dev/null && sqlite3 /data/whatsmeow.db ".backup /backup/snapshot.db"'
  # Cuidado com aspas aninhadas via ssh: elas quebram o argumento do sqlite3 em silêncio.
  # Preferir mandar o script por heredoc (ssh 'bash -s' <<'REMOTE').
  # verificar SEMPRE (tamanho não prova nada):
  sqlite3 /backup/snapshot.db "pragma integrity_check;"   # -> ok
  sqlite3 /backup/snapshot.db "select count(*) from instances;"
  ```
  626 MB levam ~8 min; o `docker run` sobrevive à queda do SSH do Instance Connect.
- **Env vars (Coolify):** `ADMIN_API_KEY` (auth do painel/API), `WEBHOOK_SECRET` (HMAC default do webhook global e header `x-uazapi-secret`),
  `UAZAPI_COMPAT_WEBHOOK_URL` (destino Uazapi-compatible aplicado a novas instâncias), `AUTOREPLY_ENABLED=false` quando o DietSystem processa `1`/`2`,
  `WATCHDOG_SECONDS=30`, `PORT=8080`, `INSTANCE_LOG_RETENTION_DAYS=7` e
  `INSTANCE_LOG_CLEANUP_INTERVAL_MINUTES=60`. (`WHATSMEOW_DSN` vem do Dockerfile apontando p/ `/data`.)
- **Tuning de escala (defaults no código/Dockerfile, override por env):** `CONNECT_CONCURRENCY=8`
  (máx. de `Connect()` simultâneos no boot/watchdog — evita thundering herd com centenas de instâncias),
  `DB_MAX_CONNS=8` (pool SQLite), `GOMEMLIMIT=1750MiB` (soft cap do GC; a EC2 é compartilhada com o prod).
  `RUNTIME_LEASE_TTL_SECONDS=30` e `RUNTIME_LEASE_RETRY_SECONDS=2` coordenam o handoff
  exclusivo no SQLite compartilhado: `/live` permanece 200 no standby, enquanto `/health`
  só fica 200 no container que possui e carregou as sessões.
  SQLite: WAL + `synchronous(NORMAL)` + `busy_timeout(30000)` + `_txlock=immediate` (sem SQLITE_BUSY sob carga).
  Watchdog usa backoff exponencial c/ jitter (30s→10min); temp-ban (402) espera o ban expirar; client-outdated
  (405) loga alto e tenta de hora em hora (= atualizar a lib whatsmeow). SIGTERM desconecta os sockets
  limpo antes de sair (redeploy não deixa sessão suja).
- **Resiliência de envio:** `GLOBAL_SEND_CONCURRENCY=8`, `QUEUE_WORKERS=4`,
  `QUEUE_MAX_ATTEMPTS=5`, `QUEUE_RETRY_MAX_SECONDS=300`, `RESET_COOLDOWN_SECONDS=60`.
  A fila fica no mesmo volume SQLite, recupera jobs após restart e espera reconexão sem
  consumir tentativas. Não há teto artificial de instâncias; a proteção é por
  concorrência de conexão/envio e pressão natural de CPU/memória.
- **Webhook global (WhatsApp Cloud API):** configurável no painel (URL destino + verify token + app secret).
  Entrega mensagens e eventos confirmado(1)/cancelado(2) no envelope oficial, assinado em `X-Hub-Signature-256`.
- **DNS:** `zap.dietsystem.com.br` A → `54.207.254.146` (DigitalOcean); TLS Let's Encrypt automático (Traefik/Coolify).

Redeploy manual: `GET https://coolify.dietsystem.com.br/api/v1/deploy?uuid=ww1t3zwj4d1q00ez6ur0d0oe` (Bearer COOLIFY_API_TOKEN).

> **Primeiro deploy da versão com runtime lease:** parar o app antigo antes de iniciar
> o novo, pois releases anteriores não participam do lease. Depois dessa migração
> inicial, os redeploys rolling fazem o handoff automaticamente sem sobrepor sessões.

## Política de saída

Cadência, consentimento, opt-out e rollout isolado estão em
[`OUTBOUND_SAFETY.md`](OUTBOUND_SAFETY.md). Essas variáveis pertencem somente ao app
`whatsapp-zap`; não criar proxy nem variáveis de rede globais no host Coolify.
