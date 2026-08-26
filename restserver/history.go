package main

// Colheita de histórico (HistorySync) para mineração de base de conhecimento.
//
// Opt-in por instância via env HISTORY_HARVEST_INSTANCES (nomes ou ids,
// separados por vírgula). Instâncias fora da lista seguem exatamente como
// antes: o evento é ignorado. Os lotes são gravados em streaming (JSONL,
// append) em HISTORY_DIR — nada fica retido em memória além do lote corrente.
//
// O download é feito por endpoints admin (Bearer): GET /history (lista) e
// GET /history/{file} (stream). O RequireFullSync é ligado APENAS quando o
// pareamento por QR é iniciado para uma instância da lista de colheita —
// pareamentos de instâncias normais (nutricionist_*) não são afetados.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
)

type historyHarvester struct {
	mu      sync.Mutex
	dir     string
	targets map[string]struct{} // nomes E ids habilitados (lowercase)
}

func newHistoryHarvester() *historyHarvester {
	h := &historyHarvester{
		dir:     getenv("HISTORY_DIR", "/data/history"),
		targets: map[string]struct{}{},
	}
	for _, t := range strings.Split(os.Getenv("HISTORY_HARVEST_INSTANCES"), ",") {
		if t = strings.ToLower(strings.TrimSpace(t)); t != "" {
			h.targets[t] = struct{}{}
		}
	}
	return h
}

func (h *historyHarvester) enabledFor(in Instance) bool {
	if len(h.targets) == 0 {
		return false
	}
	if _, ok := h.targets[strings.ToLower(in.Name)]; ok {
		return true
	}
	_, ok := h.targets[strings.ToLower(in.ID)]
	return ok
}

// historyText extrai texto de mensagens históricas, incluindo legendas de
// mídia (o extractText do fluxo ao vivo é deliberadamente mais restrito).
func historyText(msg *waE2E.Message) string {
	if msg == nil {
		return ""
	}
	if inner := msg.GetEphemeralMessage().GetMessage(); inner != nil {
		msg = inner
	}
	if inner := msg.GetViewOnceMessage().GetMessage(); inner != nil {
		msg = inner
	}
	switch {
	case msg.GetConversation() != "":
		return msg.GetConversation()
	case msg.GetExtendedTextMessage().GetText() != "":
		return msg.GetExtendedTextMessage().GetText()
	case msg.GetImageMessage().GetCaption() != "":
		return msg.GetImageMessage().GetCaption()
	case msg.GetVideoMessage().GetCaption() != "":
		return msg.GetVideoMessage().GetCaption()
	case msg.GetDocumentMessage().GetCaption() != "":
		return msg.GetDocumentMessage().GetCaption()
	}
	return ""
}

type historyRecord struct {
	Type     string `json:"type"` // "message" | "pushname" | "batch"
	SyncType string `json:"syncType,omitempty"`
	Progress uint32 `json:"progress,omitempty"`
	Chat     string `json:"chat,omitempty"`
	MsgID    string `json:"msgId,omitempty"`
	FromMe   bool   `json:"fromMe,omitempty"`
	Ts       string `json:"ts,omitempty"`
	PushName string `json:"pushName,omitempty"`
	Text     string `json:"text,omitempty"`
	JID      string `json:"jid,omitempty"` // para pushnames
	Media    string `json:"media,omitempty"`
}

// onHistorySync grava um lote de histórico da instância (se habilitada).
func (m *Manager) onHistorySync(instanceID string, v *events.HistorySync) {
	rt := m.get(instanceID)
	if rt == nil || m.history == nil {
		return
	}
	in := rt.metaCopy()
	if !m.history.enabledFor(in) {
		return
	}
	data := v.Data
	syncType := data.GetSyncType().String()

	h := m.history
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := os.MkdirAll(h.dir, 0o755); err != nil {
		m.log.Warnf("history: mkdir %s: %v", h.dir, err)
		return
	}
	path := filepath.Join(h.dir, sanitizeHistoryName(in.Name)+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		m.log.Warnf("history: open %s: %v", path, err)
		return
	}
	defer f.Close()
	enc := json.NewEncoder(f)

	msgs, convs := 0, 0
	for _, conv := range data.GetConversations() {
		convs++
		chat := conv.GetID()
		for _, hm := range conv.GetMessages() {
			wm := hm.GetMessage()
			if wm == nil {
				continue
			}
			text := historyText(wm.GetMessage())
			media := ""
			if text == "" {
				// marca presença de mídia sem texto para o minerador saber que
				// houve algo ali (áudio/imagem sem legenda etc.)
				switch {
				case wm.GetMessage().GetAudioMessage() != nil:
					media = "audio"
				case wm.GetMessage().GetImageMessage() != nil:
					media = "image"
				case wm.GetMessage().GetVideoMessage() != nil:
					media = "video"
				case wm.GetMessage().GetDocumentMessage() != nil:
					media = "document"
				case wm.GetMessage().GetStickerMessage() != nil:
					media = "sticker"
				}
				if media == "" {
					continue // sem texto e sem mídia relevante — ruído de protocolo
				}
			}
			_ = enc.Encode(historyRecord{
				Type: "message", SyncType: syncType, Chat: chat,
				MsgID:  wm.GetKey().GetID(),
				FromMe: wm.GetKey().GetFromMe(),
				Ts:     time.Unix(int64(wm.GetMessageTimestamp()), 0).UTC().Format(time.RFC3339),
				PushName: wm.GetPushName(),
				Text:     text,
				Media:    media,
			})
			msgs++
		}
	}
	for _, pn := range data.GetPushnames() {
		_ = enc.Encode(historyRecord{Type: "pushname", JID: pn.GetID(), PushName: pn.GetPushname()})
	}
	_ = enc.Encode(historyRecord{Type: "batch", SyncType: syncType, Progress: data.GetProgress(),
		Ts: time.Now().UTC().Format(time.RFC3339)})

	m.log.Infof("history %s: lote %s gravado (%d conversas, %d mensagens, progresso %d%%)",
		in.Name, syncType, convs, msgs, data.GetProgress())
	m.auditInstance(instanceID, logCategorySystem, "history_sync_batch", "info", InstanceLog{
		Status: in.Status, Source: "whatsapp_event",
		Details: map[string]any{"syncType": syncType, "messages": msgs, "progress": data.GetProgress()},
	})
}

func sanitizeHistoryName(name string) string {
	name = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, name)
	if name == "" {
		name = "instance"
	}
	return name
}

// -------------------------------------------------------- endpoints admin

func (h *Handlers) historyList(w http.ResponseWriter, r *http.Request) {
	dir := h.mgr.history.dir
	entries, err := os.ReadDir(dir)
	if err != nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	type fileInfo struct {
		Name string `json:"name"`
		Size int64  `json:"size"`
		Mod  string `json:"modifiedAt"`
	}
	out := []fileInfo{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		info, ierr := e.Info()
		if ierr != nil {
			continue
		}
		out = append(out, fileInfo{Name: e.Name(), Size: info.Size(), Mod: info.ModTime().UTC().Format(time.RFC3339)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, http.StatusOK, out)
}

func (h *Handlers) historyDownload(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("file")
	if name != sanitizeHistoryName(strings.TrimSuffix(name, ".jsonl"))+".jsonl" {
		http.Error(w, "invalid file name", http.StatusBadRequest)
		return
	}
	path := filepath.Join(h.mgr.history.dir, name)
	f, err := os.Open(path)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	defer f.Close()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name))
	if info, serr := f.Stat(); serr == nil {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))
	}
	_, _ = io.Copy(w, f)
}
