# Segurança de mensagens de saída

Este serviço não implementa rotação de proxies, troca de IP para ocultar automação,
fingerprint spoofing ou mecanismos para contornar restrições do WhatsApp. Esses
mecanismos não corrigem a causa principal das restrições (mensagens inesperadas,
frequência, bloqueios, denúncias e baixa qualidade) e aumentam o risco de expor as
sessões e chaves do WhatsApp a terceiros.

## Controles implementados

- token bucket por instância, com rajada e cadência sustentada configuráveis;
- cooldown e teto móvel de 24 horas por destinatário;
- auditoria persistida de envios em SQLite, com retenção operacional de sete dias;
- registro local de consentimento e revogação, inclusive variantes brasileiras com/sem 9º dígito;
- opt-out automático para palavras de parada exatas;
- janela de atendimento aberta por mensagem recebida, sem apagar opt-out anterior;
- `429 Retry-After` para o chamador enfileirar e tentar novamente no momento correto;
- circuit breaker persistente após `TemporaryBan`, inclusive através de reinícios;
- auto-respostas submetidas à mesma política dos demais envios.

O chamado "consentimento local" não vem do WhatsApp. O protocolo Web usado por
`whatsmeow` não expõe estado de opt-in, quality rating, templates aprovados ou limites
oficiais da conta. O registro serve somente para este serviço respeitar informações
que ele próprio recebeu: uma conversa iniciada pelo usuário, uma palavra de opt-out ou
uma origem externa informada explicitamente. Por isso ele não deve ser apresentado
como prova de autorização da Meta e seu enforcement vem desligado por padrão.

Esses controles reduzem dano e duplicidade, mas não garantem que uma conta não será
restringida. `whatsmeow` automatiza o protocolo Web/Multi-Device e não é a API oficial
para disparos de negócio.

## Rollout isolado no Coolify

O repositório documenta o app `whatsapp-zap` dentro do projeto Coolify `WhatsApp`.
As variáveis abaixo devem ser configuradas **somente nesse app**, nunca como variáveis
globais do servidor e nunca nos apps DietSystem/Pulso/API:

```env
SEND_RATE_PER_MINUTE=30
SEND_BURST=5
SEND_RECIPIENT_COOLDOWN_SECONDS=10
SEND_RECIPIENT_DAILY_MAX=20
SEND_SERVICE_WINDOW_HOURS=24
SEND_REQUIRE_LOCAL_CONSENT=false
```

Rollout recomendado:

1. publicar com `SEND_REQUIRE_LOCAL_CONSENT=false`; limites e opt-outs já ficam ativos;
2. no aplicativo chamador, registrar a permissão antes do primeiro envio usando
   `POST /instances/{id}/consents`, com origem verificável como
   `appointment_checkout`, `patient_portal` ou `inbound_whatsapp`;
3. conferir que revogações também chegam ao endpoint `/consents/revoke`;
4. opcionalmente mudar este serviço para `SEND_REQUIRE_LOCAL_CONSENT=true` somente se
   a origem desses registros locais for confiável;
5. tratar `429` respeitando integralmente `Retry-After`, sem loop de retry imediato.

Não configure `HTTP_PROXY`, `HTTPS_PROXY` ou `ALL_PROXY` no host/servidor Coolify.
Além de afetar outros projetos, a biblioteca Go herda proxy por variável de ambiente,
o que poderia desviar também uploads de mídia e conexões do WhatsApp sem isolamento.

## Caminho recomendado para escala

Para mensagens comerciais iniciadas em escala, o caminho suportado pelo WhatsApp é a
Cloud API oficial, com templates aprovados e monitoramento de qualidade/leitura. Uma
eventual migração fica fora do escopo deste repositório e não exige alterar outros
projetos nesta implementação.

Como o produto lida com saúde, mensagens devem conter apenas o mínimo necessário para
o agendamento. Não inclua diagnósticos, dados nutricionais, documentos ou outros dados
sensíveis no texto de lembretes.

## Referências oficiais consultadas

- WhatsApp Business Messaging Policy: https://whatsappbusiness.com/policy/
- Meta, controles e qualidade de conversas comerciais:
  https://about.fb.com/news/2025/04/ways-to-manage-your-businesses-chats-on-whatsapp/
- WhatsApp, Best Practices for Marketing Messages (2026):
  https://whatsappbusiness.com/wp-content/uploads/2026/04/Best-Practices-for-Marketing-Messages-on-WhatsApp-.pdf
- Coleções oficiais da WhatsApp Business Platform/Cloud API:
  https://www.postman.com/meta/whatsapp-business-platform/overview
