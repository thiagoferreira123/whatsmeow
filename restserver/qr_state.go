package main

import (
	"context"
	"time"
)

// qrMaxPlanSteps limita quantas decisões o executor toma numa única requisição.
// Cada ação não-terminal reduz estritamente o espaço de estados (device novo,
// vínculo descartado, socket derrubado, loop reiniciado), então o laço converge
// bem antes desse teto — ele é só a rede de segurança contra ciclo.
const qrMaxPlanSteps = 4

// qrSnapshot é a foto do estado de uma instância no instante em que alguém pede
// o QR. Só campos simples: a decisão é pura, sem rede e sem lock, para poder ser
// testada exaustivamente.
//
// De propósito NÃO entram aqui `paused` (hibernada), `conflicted`
// (stream_replaced), `loggedOut` nem `nextConnectAt`: nenhum deles é motivo para
// negar um QR. São freios que o executor solta, não condições de bloqueio.
type qrSnapshot struct {
	clientNil     bool
	storeNil      bool
	deviceDeleted bool // a lib apagou o device (401/403/406 ou PairError tardio)
	hasSession    bool // Store.ID != nil — o pareamento ainda está salvo
	connected     bool
	loggedIn      bool
	qrRunning     bool
	hasCode       bool
	qrAge         time.Duration // há quanto tempo o loop de pareamento começou
	stallAfter    time.Duration // idade a partir da qual o loop é dado como travado (0 = nunca)
}

// qrAction é o próximo passo que o executor deve dar.
type qrAction uint8

const (
	qrReportConnected qrAction = iota // ÚNICO caminho que não termina em QR
	qrRecycleDevice                   // troca por um device virgem (container.NewDevice)
	qrReviveSession                   // tenta ressuscitar a sessão salva; falhou → descarta o vínculo
	qrDropSocket                      // fecha o socket: GetQRChannel exige !IsConnected()
	qrRestartPairing                  // invalida um loop de pareamento travado/órfão
	qrServeCurrent                    // já há loop vivo: espera/entrega o código dele
	qrStartPairing                    // abre o canal de QR e disca
)

func (a qrAction) String() string {
	switch a {
	case qrReportConnected:
		return "report_connected"
	case qrRecycleDevice:
		return "recycle_device"
	case qrReviveSession:
		return "revive_session"
	case qrDropSocket:
		return "drop_socket"
	case qrRestartPairing:
		return "restart_pairing"
	case qrServeCurrent:
		return "serve_current"
	case qrStartPairing:
		return "start_pairing"
	}
	return "unknown"
}

// planQRRequest decide o próximo passo de um pedido de QR.
//
// A ordem das regras É o contrato do produto: só existe um estado em que o QR
// não é gerado — instância conectada E logada. Todo o resto converge para um
// código novo, curando o estado no caminho.
func planQRRequest(s qrSnapshot) qrAction {
	if s.connected && s.loggedIn {
		return qrReportConnected
	}
	// Client/device inutilizável: um *Client com Store.Deleted recusa qualquer
	// Connect() com ErrDeviceDeleted, e não existe "undelete" — só device novo.
	if s.clientNil || s.storeNil || s.deviceDeleted {
		return qrRecycleDevice
	}
	// Sessão salva: primeiro tenta trazer de volta sem obrigar a reparear.
	if s.hasSession {
		return qrReviveSession
	}
	// GetQRChannel devolve ErrQRAlreadyConnected com o socket de pé.
	if s.connected {
		return qrDropSocket
	}
	if s.qrRunning && !s.hasCode && s.stallAfter > 0 && s.qrAge >= s.stallAfter {
		return qrRestartPairing
	}
	if s.qrRunning {
		return qrServeCurrent
	}
	return qrStartPairing
}

// waitReconnect faz polling de isUp até virar true ou a janela acabar.
// window <= 0 desiste na hora, sem sequer consultar isUp — é o modo usado pelos
// testes e pelo kill-switch de produção.
func waitReconnect(ctx context.Context, window, poll time.Duration, isUp func() bool) bool {
	if window <= 0 {
		return false
	}
	if poll <= 0 {
		poll = 250 * time.Millisecond
	}
	deadline := time.Now().Add(window)
	for {
		if isUp() {
			return true
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		wait := poll
		if remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return isUp()
		case <-timer.C:
		}
	}
}
