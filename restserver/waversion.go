package main

import (
	"context"
	"time"

	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// syncWAVersion atualiza a versão do WhatsApp Web anunciada no handshake para a
// que o web.whatsapp.com está servindo agora. O telefone recusa pareamento por
// QR de cliente com versão velha ("não é possível conectar novos dispositivos
// no momento"), então a constante embutida na lib sozinha apodrece em semanas.
// Falha na busca mantém a versão embutida — sessões já pareadas ainda conectam
// com ela; nunca rebaixa a versão (uma resposta anômala não pode piorar o que
// já está embutido).
func syncWAVersion(ctx context.Context, fetch func(context.Context) (*store.WAVersionContainer, error), log waLog.Logger) store.WAVersionContainer {
	baked := store.GetWAVersion()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	latest, err := fetch(ctx)
	if err != nil {
		log.Warnf("Falha ao buscar versão atual do WhatsApp Web, mantendo %s: %v", baked, err)
		return baked
	}
	if latest.LessThan(baked) {
		log.Warnf("Versão remota %s é mais antiga que a embutida %s, mantendo a embutida", latest, baked)
		return baked
	}
	store.SetWAVersion(*latest)
	return *latest
}
