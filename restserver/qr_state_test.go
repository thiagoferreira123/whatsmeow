package main

import (
	"context"
	"testing"
	"time"
)

const testStall = 10 * time.Second

// A regra que o produto exige: pedir QR só pode ser negado quando a instância
// está conectada E logada. Qualquer outro estado tem que levar a uma ação que
// termina em QR.
func TestPlanQRRequestOnlyBlocksWhenConnectedAndLoggedIn(t *testing.T) {
	cases := []struct {
		name string
		snap qrSnapshot
		want qrAction
	}{
		{
			name: "conectada e logada é o único bloqueio legítimo",
			snap: qrSnapshot{connected: true, loggedIn: true, hasSession: true, stallAfter: testStall},
			want: qrReportConnected,
		},
		{
			name: "device apagado pelo 401/403/406 ou PairError",
			snap: qrSnapshot{deviceDeleted: true, stallAfter: testStall},
			want: qrRecycleDevice,
		},
		{
			name: "runtime sem client",
			snap: qrSnapshot{clientNil: true, stallAfter: testStall},
			want: qrRecycleDevice,
		},
		{
			name: "client sem store",
			snap: qrSnapshot{storeNil: true, stallAfter: testStall},
			want: qrRecycleDevice,
		},
		{
			name: "device apagado tem precedência sobre sessão salva",
			snap: qrSnapshot{deviceDeleted: true, hasSession: true, stallAfter: testStall},
			want: qrRecycleDevice,
		},
		{
			name: "sessão salva mas offline vai para revive",
			snap: qrSnapshot{hasSession: true, stallAfter: testStall},
			want: qrReviveSession,
		},
		{
			name: "sessão salva com socket de pé porém não logada vai para revive",
			snap: qrSnapshot{hasSession: true, connected: true, stallAfter: testStall},
			want: qrReviveSession,
		},
		{
			name: "socket de pé sem sessão precisa cair antes do GetQRChannel",
			snap: qrSnapshot{connected: true, stallAfter: testStall},
			want: qrDropSocket,
		},
		{
			name: "loop de pareamento travado sem código reinicia",
			snap: qrSnapshot{qrRunning: true, qrAge: 30 * time.Second, stallAfter: testStall},
			want: qrRestartPairing,
		},
		{
			name: "loop novo sem código ainda é servido",
			snap: qrSnapshot{qrRunning: true, qrAge: time.Second, stallAfter: testStall},
			want: qrServeCurrent,
		},
		{
			name: "loop antigo com código é servido, não reiniciado",
			snap: qrSnapshot{qrRunning: true, hasCode: true, qrAge: time.Hour, stallAfter: testStall},
			want: qrServeCurrent,
		},
		{
			name: "stallAfter zerado nunca reinicia o loop",
			snap: qrSnapshot{qrRunning: true, qrAge: time.Hour},
			want: qrServeCurrent,
		},
		{
			name: "instância virgem começa a parear",
			snap: qrSnapshot{stallAfter: testStall},
			want: qrStartPairing,
		},
		{
			name: "hibernada/conflitada não são entrada da decisão: viram pareamento",
			snap: qrSnapshot{stallAfter: testStall},
			want: qrStartPairing,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planQRRequest(tc.snap)
			if got != tc.want {
				t.Fatalf("planQRRequest = %v; quero %v", got, tc.want)
			}
			if got == qrReportConnected && !(tc.snap.connected && tc.snap.loggedIn) {
				t.Fatal("negou o QR sem estar conectada e logada")
			}
		})
	}
}

// applyForTest aplica o efeito de cada ação sobre o estado, na versão mais
// pessimista (revive sempre falha, virando descarte do vínculo). Serve para
// provar que o laço do executor converge dentro de qrMaxPlanSteps.
func applyForTest(s qrSnapshot, a qrAction) qrSnapshot {
	switch a {
	case qrRecycleDevice:
		s.clientNil, s.storeNil, s.deviceDeleted = false, false, false
		s.hasSession, s.connected, s.loggedIn = false, false, false
		s.qrRunning, s.hasCode, s.qrAge = false, false, 0
	case qrReviveSession:
		s.hasSession, s.connected, s.loggedIn = false, false, false
		s.qrRunning, s.hasCode, s.qrAge = false, false, 0
	case qrDropSocket:
		s.connected = false
	case qrRestartPairing:
		s.qrRunning, s.hasCode, s.qrAge = false, false, 0
	}
	return s
}

func TestPlanQRRequestConvergesWithinMaxSteps(t *testing.T) {
	terminal := map[qrAction]bool{
		qrReportConnected: true,
		qrServeCurrent:    true,
		qrStartPairing:    true,
	}

	// Varre todas as combinações de flags booleanas relevantes.
	for mask := 0; mask < 1<<7; mask++ {
		snap := qrSnapshot{
			clientNil:     mask&1 != 0,
			storeNil:      mask&2 != 0,
			deviceDeleted: mask&4 != 0,
			hasSession:    mask&8 != 0,
			connected:     mask&16 != 0,
			loggedIn:      mask&32 != 0,
			qrRunning:     mask&64 != 0,
			qrAge:         time.Minute,
			stallAfter:    testStall,
		}
		state, steps := snap, 0
		for steps < qrMaxPlanSteps {
			action := planQRRequest(state)
			if terminal[action] {
				break
			}
			state = applyForTest(state, action)
			steps++
		}
		if steps >= qrMaxPlanSteps {
			t.Fatalf("mask=%d não convergiu em %d passos (estado final %+v)", mask, qrMaxPlanSteps, state)
		}
	}
}

func TestWaitReconnectReturnsTrueWhenClientComesBack(t *testing.T) {
	calls := 0
	up := waitReconnect(context.Background(), 2*time.Second, 5*time.Millisecond, func() bool {
		calls++
		return calls >= 3
	})
	if !up {
		t.Fatalf("waitReconnect = false depois de %d checagens; quero true", calls)
	}
}

func TestWaitReconnectGivesUpAfterWindow(t *testing.T) {
	start := time.Now()
	if waitReconnect(context.Background(), 80*time.Millisecond, 10*time.Millisecond, func() bool { return false }) {
		t.Fatal("waitReconnect = true com isUp sempre false")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waitReconnect demorou %s; deveria respeitar a janela", elapsed)
	}
}

func TestWaitReconnectZeroWindowDoesNotWait(t *testing.T) {
	called := false
	start := time.Now()
	if waitReconnect(context.Background(), 0, time.Second, func() bool { called = true; return true }) {
		t.Fatal("janela zero deveria desistir na hora")
	}
	if called {
		t.Fatal("janela zero não deveria consultar isUp")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("janela zero esperou %s", elapsed)
	}
}
