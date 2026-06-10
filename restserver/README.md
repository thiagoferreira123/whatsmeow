# whatsmeow REST server

Servidor HTTP que expõe a biblioteca [whatsmeow](https://pkg.go.dev/go.mau.fi/whatsmeow)
como uma API REST. Serve de **fallback** e futuro **substituto** do uazapi usado pelo
`DietSystem 3/web`. Multi-instância, SQLite puro-Go (roda no Windows sem CGO).

É um subpacote `package main` dentro do próprio módulo `go.mau.fi/whatsmeow` — importa a lib
do código local, igual ao exemplo `mdtest`.

## Rodar

```powershell
# da raiz do repo whatsmeow
go run ./restserver
# API + painel em http://localhost:8080
```

**Painel web:** abra `http://localhost:8080` no navegador. A UI (servida pelo próprio
servidor, mesma origem) permite criar instâncias, ler o QR (com polling até conectar),
ver status, enviar texto/mídia, desconectar e excluir. Se `ADMIN_API_KEY` estiver setado,
informe a chave no campo "API key" do topo.

Sessões do WhatsApp + a tabela `instances` ficam num único arquivo `whatsmeow.db` (criado no
diretório de execução).

## Configuração (env vars)

| Var | Default | Descrição |
|---|---|---|
| `PORT` | `8080` | porta HTTP |
| `WHATSMEOW_DSN` | `file:whatsmeow.db?_pragma=foreign_keys(on)&_pragma=busy_timeout(5000)` | DSN SQLite (modernc) |
| `ADMIN_API_KEY` | *(vazio)* | se setado, exige `Authorization: Bearer <key>` (ou header `token`); vazio = auth off |
| `WEBHOOK_SECRET` | `dev-secret` | valor do header `x-uazapi-secret` nos webhooks de saída |
| `AUTOREPLY_ENABLED` | `true` | liga a auto-resposta embutida de confirmação 1/2 |
| `AUTOREPLY_CONFIRM_MSG` | ✅ Sua consulta foi confirmada! Até breve. | resposta ao `1` |
| `AUTOREPLY_CANCEL_MSG` | ❌ Sua consulta foi cancelada... | resposta ao `2` |

## Endpoints

| Método/Path | Body | Resposta |
|---|---|---|
| `GET /health` | — | `{ok:true}` (sem auth) |
| `POST /instances` | `{name, adminField01?, webhookUrl?, webhookSecret?}` | instância criada (com `id`, `token`) |
| `GET /instances` | — | `[instância]` |
| `GET /instances/{id}` | — | instância |
| `DELETE /instances/{id}` | — | `204` (logout + remove sessão e row) |
| `GET /instances/{id}/qr` | — | `{status, qrcode:"data:image/png;base64,…", code, expiresAt}` |
| `GET /instances/{id}/qr.png` | — | imagem PNG do QR (abrir no navegador p/ escanear) |
| `GET /instances/{id}/status` | — | `{status, loggedIn, connected, owner, profileName}` |
| `POST /instances/{id}/send/text` | `{number, text}` | `{id, status}` |
| `POST /instances/{id}/send/media` | **JSON:** `{number, type?, file:URL\|base64\|dataURI, text?, fileName?}` · **ou upload:** `multipart/form-data` com campo `file` (+ `number`, `type?`, `text?`, `fileName?`) | `{id, status}` |
| `POST /instances/{id}/webhook` | `{url, secret?, events?, enabled?}` | `{ok:true}` |
| `POST /instances/{id}/disconnect` | — | `204` (fecha socket, mantém sessão) |

`status` ∈ `disconnected | connecting | connected`. `number` aceita telefone (`5511999998888`,
com ou sem formatação) ou JID completo (`...@s.whatsapp.net`). O número é **resolvido via
`IsOnWhatsApp`** (o servidor do WhatsApp devolve o JID canônico), tratando a regra do **9º dígito**
brasileiro automaticamente — testa as variantes com/sem o `9`. Número não registrado → `422`.
`type` é opcional no envio de mídia (vazio = inferido do mime do arquivo).

## Teste rápido (PowerShell)

```powershell
$base = "http://localhost:8080"

# criar instância
$inst = irm -Method Post "$base/instances" -Body (@{name='teste';adminField01='46'}|ConvertTo-Json) -ContentType application/json
$id = $inst.id

# gerar QR e ESCANEAR com o WhatsApp (abre a imagem no navegador):
Start-Process "$base/instances/$id/qr.png"

# checar status (após escanear vira 'connected')
irm "$base/instances/$id/status"

# enviar texto
irm -Method Post "$base/instances/$id/send/text" -Body (@{number='5511999998888';text='oi do whatsmeow'}|ConvertTo-Json) -ContentType application/json

# enviar mídia (imagem por URL)
irm -Method Post "$base/instances/$id/send/media" -Body (@{number='5511999998888';type='image';file='https://go.dev/images/go-logo-blue.svg';text='logo'}|ConvertTo-Json) -ContentType application/json

# listar / buscar / deletar
irm "$base/instances"
irm "$base/instances/$id"
curl.exe -s -o NUL -w "%{http_code}" -X DELETE "$base/instances/$id"   # 204
```

### Testar o fluxo 1/2 (confirmação de consulta)

Com a instância conectada, mande **`1`** (ou **`2`**) do seu celular pessoal para o número logado.
Com `AUTOREPLY_ENABLED=true` o serviço responde automaticamente (confirma/cancela). Se a instância
tiver `webhookUrl` configurado, o evento também é encaminhado no formato uazapi (header
`x-uazapi-secret`) — aponte para `https://webhook.site/<id>` ou para o `/webhooks/uazapi` do web local
para inspecionar o payload.

> ⚠️ Nota: `Invoke-WebRequest -Method Delete` pode falhar no PowerShell 5.1 (modo NonInteractive);
> use `curl.exe -X DELETE` ou `Invoke-RestMethod -Method Delete`.

## Reconexão & estabilidade (evita o "sincronizando dados")

- **Keys persistidas em disco** (`whatsmeow.db`, tabelas `whatsmeow_*`): `whatsmeow_device`
  (noise/identity/signed-pre-key, registration_id, jid), `whatsmeow_sessions`,
  `whatsmeow_pre_keys`, `whatsmeow_sender_keys`, `whatsmeow_app_state_sync_keys` (estas evitam
  re-sync). Restart **não** pede QR de novo — `LoadAll` recarrega e reconecta do banco.
- **Auto-reconnect do whatsmeow** ligado explicitamente (`EnableAutoReconnect` +
  `InitialAutoReconnect` em `manager.go attachClient`): queda de socket re-disca e re-autentica
  das keys, **sem QR**. Keepalive a cada 20–30s; reconecta se falhar >3 min. (O `Got 515 code,
  reconnecting` no log é normal, é o reconnect pós-pareamento.)
- **Watchdog** (`WATCHDOG_SECONDS`, padrão 30s): rede de segurança que reconecta instâncias
  pareadas que ficaram caídas (cobre falha de conexão no boot e conflitos). **Respeita o
  `/disconnect` manual** (não fica reconectando o que você desligou de propósito) e dá **5 min de
  backoff em `stream_replaced`**.
- **Não recupera sozinho** (exigem ação): logout/aparelho removido pelo celular, `stream_replaced`
  (mesma sessão em 2 lugares — **rode só 1 processo por sessão/DB**), client desatualizado, ban.
- **SQLite em WAL** agora. **Produção:** trocar `WHATSMEOW_DSN` por Postgres e rodar sob
  **systemd / Docker `restart: always`** (crash reinicia e `LoadAll` reconecta tudo).

## Pendências para paridade total com uazapi (fase 2)

- Camada **uazapi-wire-compatível** (paths `/instance/init`, `/send/text` e headers `token`/`admintoken`)
  para o web trocar só a base-URL.
- Pairing code por número (`PairPhone`), perfil completo (foto/business), envio em lote/broadcast.
- Recebimento de mídia, resolução LID↔telefone mais robusta, auth multi-tenant por instância.
