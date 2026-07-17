package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

type uazapiWebhookResponse struct {
	ID                  string   `json:"id"`
	Enabled             bool     `json:"enabled"`
	URL                 string   `json:"url"`
	Events              []string `json:"events"`
	ExcludeMessages     []string `json:"excludeMessages"`
	AddURLEvents        bool     `json:"addUrlEvents"`
	AddURLTypesMessages bool     `json:"addUrlTypesMessages"`
}

func testUazapiCompatManager(t *testing.T, cfg Config) *Manager {
	t.Helper()
	dsn := fmt.Sprintf("file:uazapi-compat-%d?mode=memory&cache=shared&_pragma=foreign_keys(on)", time.Now().UnixNano())
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	container := sqlstore.NewWithDB(db, "sqlite3", waLog.Noop)
	if err := container.Upgrade(context.Background()); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(db)
	if err != nil {
		t.Fatal(err)
	}
	return NewManager(container, store, cfg, waLog.Noop)
}

func decodeUazapiWebhookResponse(t *testing.T, rec *httptest.ResponseRecorder) []uazapiWebhookResponse {
	t.Helper()
	var body []uazapiWebhookResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
	}
	return body
}

func TestUazapiCompatMessagePayloadMatchesDietSystemContract(t *testing.T) {
	received := make(chan struct {
		secret string
		body   map[string]any
	}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode webhook body: %v", err)
		}
		received <- struct {
			secret string
			body   map[string]any
		}{secret: r.Header.Get("x-uazapi-secret"), body: body}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	in := Instance{
		ID:           "instance-id",
		Name:         "nutricionist_42",
		Token:        "instance-token",
		AdminField01: "42",
	}
	payload := messageWebhookPayload(in, map[string]any{
		"messageid":    "message-id",
		"text":         "1",
		"fromMe":       false,
		"wasSentByApi": false,
		"isGroup":      false,
		"sender_pn":    "5567999999999@s.whatsapp.net",
	})
	NewWebhookSender().deliver(server.URL, "shared-secret", payload)

	select {
	case got := <-received:
		if got.secret != "shared-secret" {
			t.Fatalf("x-uazapi-secret = %q, want shared-secret", got.secret)
		}
		if got.body["EventType"] != "messages" || got.body["token"] != "instance-token" {
			t.Fatalf("unexpected webhook envelope: %#v", got.body)
		}
		message, ok := got.body["message"].(map[string]any)
		if !ok {
			t.Fatalf("message payload missing: %#v", got.body["message"])
		}
		if message["messageid"] != "message-id" || message["text"] != "1" || message["sender_pn"] != "5567999999999@s.whatsapp.net" {
			t.Fatalf("unexpected message payload: %#v", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for uazapi-compatible webhook")
	}
}

func TestUazapiCompatWebhookRestoresRequestedBrazilianMobileNumber(t *testing.T) {
	received := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode webhook body: %v", err)
		}
		received <- body
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	m := testUazapiCompatManager(t, Config{})
	in, err := m.Create("nutricionist_46", "46", server.URL, "shared-secret")
	if err != nil {
		t.Fatal(err)
	}

	const requested = "5567991374895"
	const resolved = "556791374895"
	m.rememberRecipientAlias(in.ID, requested, types.NewJID(resolved, types.DefaultUserServer))

	resolvedJID := types.NewJID(resolved, types.DefaultUserServer)
	info := types.MessageInfo{
		MessageSource: types.MessageSource{Chat: resolvedJID, Sender: resolvedJID},
		ID:            "incoming-message-id",
		Timestamp:     time.Now(),
	}
	m.onMessage(in.ID, &events.Message{
		Info:    info,
		Message: &waE2E.Message{Conversation: proto.String("1")},
	})

	select {
	case body := <-received:
		message, ok := body["message"].(map[string]any)
		if !ok {
			t.Fatalf("message payload missing: %#v", body)
		}
		if got := message["sender_pn"]; got != requested+"@s.whatsapp.net" {
			t.Fatalf("sender_pn = %q, want canonical requested number", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for uazapi-compatible webhook")
	}
}

func TestUazapiCompatWebhookPostPersistsAndGetReturnsArray(t *testing.T) {
	m, _ := testPolicyManager(t, Config{})
	if err := m.SetGlobalWebhook(GlobalWebhook{VerifyToken: "cloud-verify", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	router := NewHandlers(m, Config{}).Router()

	postBody := `{
		"url":"https://api.example.test/api/webhooks/uazapi",
		"enabled":true,
		"events":["connection","messages"],
		"excludeMessages":["wasSentByApi","fromMeYes","isGroupYes"],
		"addUrlEvents":false,
		"addUrlTypesMessages":false
	}`
	postReq := httptest.NewRequest(http.MethodPost, "/webhook", strings.NewReader(postBody))
	postReq.Header.Set("Content-Type", "application/json")
	postReq.Header.Set("token", "token")
	postRec := httptest.NewRecorder()
	router.ServeHTTP(postRec, postReq)
	if postRec.Code != http.StatusOK {
		t.Fatalf("POST /webhook status=%d body=%s", postRec.Code, postRec.Body.String())
	}
	postConfig := decodeUazapiWebhookResponse(t, postRec)
	if len(postConfig) != 1 || postConfig[0].ID != "instance-1" || len(postConfig[0].ExcludeMessages) != 3 {
		t.Fatalf("POST /webhook response=%#v", postConfig)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/webhook", nil)
	getReq.Header.Set("token", "token")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET /webhook status=%d body=%s", getRec.Code, getRec.Body.String())
	}
	getConfig := decodeUazapiWebhookResponse(t, getRec)
	if len(getConfig) != 1 || !reflect.DeepEqual(getConfig[0], postConfig[0]) {
		t.Fatalf("GET /webhook response=%#v, want %#v", getConfig, postConfig)
	}

	verifyReq := httptest.NewRequest(http.MethodGet, "/webhook?hub.mode=subscribe&hub.verify_token=cloud-verify&hub.challenge=challenge-ok", nil)
	verifyRec := httptest.NewRecorder()
	router.ServeHTTP(verifyRec, verifyReq)
	if verifyRec.Code != http.StatusOK || verifyRec.Body.String() != "challenge-ok" {
		t.Fatalf("Cloud handshake status=%d body=%q", verifyRec.Code, verifyRec.Body.String())
	}
}

func TestUazapiCompatInitCreatesInstanceWithDietSystemWebhook(t *testing.T) {
	t.Setenv("ADMIN_API_KEY", "admin-secret")
	t.Setenv("WEBHOOK_SECRET", "shared-webhook-secret")
	t.Setenv("UAZAPI_COMPAT_WEBHOOK_URL", "https://api.example.test/api/webhooks/uazapi")
	t.Setenv("AUTOREPLY_ENABLED", "false")
	cfg := loadConfig()
	m := testUazapiCompatManager(t, cfg)
	router := NewHandlers(m, cfg).Router()

	initReq := httptest.NewRequest(http.MethodPost, "/instance/init", strings.NewReader(`{"name":"nutricionist_42","adminField01":"42"}`))
	initReq.Header.Set("Content-Type", "application/json")
	initReq.Header.Set("admintoken", "admin-secret")
	initRec := httptest.NewRecorder()
	router.ServeHTTP(initRec, initReq)
	if initRec.Code != http.StatusOK {
		t.Fatalf("POST /instance/init status=%d body=%s", initRec.Code, initRec.Body.String())
	}
	var created struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(initRec.Body).Decode(&created); err != nil || created.Token == "" {
		t.Fatalf("decode created instance: token=%q err=%v", created.Token, err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/webhook", nil)
	getReq.Header.Set("token", created.Token)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET /webhook status=%d body=%s", getRec.Code, getRec.Body.String())
	}
	configs := decodeUazapiWebhookResponse(t, getRec)
	if len(configs) != 1 {
		t.Fatalf("GET /webhook response=%#v", configs)
	}
	got := configs[0]
	if got.URL != "https://api.example.test/api/webhooks/uazapi" || !got.Enabled {
		t.Fatalf("new instance webhook=%#v", got)
	}
	if strings.Join(got.Events, ",") != "connection,messages" {
		t.Fatalf("events=%v", got.Events)
	}
	if strings.Join(got.ExcludeMessages, ",") != "wasSentByApi,fromMeYes,isGroupYes" {
		t.Fatalf("excludeMessages=%v", got.ExcludeMessages)
	}
}

func TestUazapiCompatStatusAndListKeepCurrentQRCode(t *testing.T) {
	t.Setenv("ADMIN_API_KEY", "admin-secret")
	cfg := loadConfig()
	m := testUazapiCompatManager(t, cfg)
	in, err := m.Create("nutricionist_46", "46", "", "")
	if err != nil {
		t.Fatal(err)
	}

	rt := m.get(in.ID)
	rt.mu.Lock()
	rt.qrCode = "current-pairing-code"
	rt.qrExpiresAt = time.Now().Add(time.Minute)
	rt.mu.Unlock()

	router := NewHandlers(m, cfg).Router()

	statusReq := httptest.NewRequest(http.MethodGet, "/instance/status", nil)
	statusReq.Header.Set("token", in.Token)
	statusRec := httptest.NewRecorder()
	router.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("GET /instance/status status=%d body=%s", statusRec.Code, statusRec.Body.String())
	}
	var statusBody struct {
		Instance struct {
			QRCode string `json:"qrcode"`
		} `json:"instance"`
	}
	if err := json.NewDecoder(statusRec.Body).Decode(&statusBody); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(statusBody.Instance.QRCode, "data:image/png;base64,") {
		t.Fatalf("GET /instance/status qrcode=%q, want current QR data URI", statusBody.Instance.QRCode)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/instance/all", nil)
	listReq.Header.Set("admintoken", "admin-secret")
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("GET /instance/all status=%d body=%s", listRec.Code, listRec.Body.String())
	}
	var listBody []struct {
		QRCode string `json:"qrcode"`
	}
	if err := json.NewDecoder(listRec.Body).Decode(&listBody); err != nil {
		t.Fatal(err)
	}
	if len(listBody) != 1 || !strings.HasPrefix(listBody[0].QRCode, "data:image/png;base64,") {
		t.Fatalf("GET /instance/all response=%#v, want current QR data URI", listBody)
	}
}

func TestUazapiCompatAsyncTextHonorsAvailableAt(t *testing.T) {
	t.Setenv("ADMIN_API_KEY", "admin-secret")
	cfg := loadConfig()
	m := testUazapiCompatManager(t, cfg)
	router := NewHandlers(m, cfg).Router()

	initReq := httptest.NewRequest(http.MethodPost, "/instance/init", strings.NewReader(`{"name":"nutricionist_46","adminField01":"46"}`))
	initReq.Header.Set("Content-Type", "application/json")
	initReq.Header.Set("admintoken", "admin-secret")
	initRec := httptest.NewRecorder()
	router.ServeHTTP(initRec, initReq)
	if initRec.Code != http.StatusOK {
		t.Fatalf("POST /instance/init status=%d body=%s", initRec.Code, initRec.Body.String())
	}
	var created struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(initRec.Body).Decode(&created); err != nil || created.Token == "" {
		t.Fatalf("decode created instance: token=%q err=%v", created.Token, err)
	}

	availableAt := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Second)
	body := fmt.Sprintf(`{"number":"5567999999999","text":"mensagem agendada","async":true,"idempotencyKey":"campaign-1:0","availableAt":%q}`, availableAt.Format(time.RFC3339))
	sendReq := httptest.NewRequest(http.MethodPost, "/send/text", strings.NewReader(body))
	sendReq.Header.Set("Content-Type", "application/json")
	sendReq.Header.Set("token", created.Token)
	sendRec := httptest.NewRecorder()
	router.ServeHTTP(sendRec, sendReq)
	if sendRec.Code != http.StatusAccepted {
		t.Fatalf("POST /send/text status=%d body=%s", sendRec.Code, sendRec.Body.String())
	}

	instances, err := m.store.List()
	if err != nil || len(instances) != 1 {
		t.Fatalf("list instances: count=%d err=%v", len(instances), err)
	}
	job, err := m.store.GetQueueByKey(instances[0].ID, "campaign-1:0")
	if err != nil {
		t.Fatalf("get queued message: %v", err)
	}
	got, err := time.Parse(time.RFC3339Nano, job.AvailableAt)
	if err != nil {
		t.Fatalf("parse availableAt %q: %v", job.AvailableAt, err)
	}
	if !got.Equal(availableAt) {
		t.Fatalf("availableAt=%s, want %s", got, availableAt)
	}
}

func TestUazapiCompatSenderAdvancedQueuesEntireCampaignAndListsHistory(t *testing.T) {
	t.Setenv("ADMIN_API_KEY", "admin-secret")
	cfg := loadConfig()
	m := testUazapiCompatManager(t, cfg)
	in, err := m.Create("nutricionist_46", "46", "", "")
	if err != nil {
		t.Fatal(err)
	}
	router := NewHandlers(m, cfg).Router()

	body := `{
		"messages":[
			{"number":"5567999999901","text":"mensagem 1","type":"text"},
			{"number":"5567999999902","text":"mensagem 2","type":"text"},
			{"number":"5567999999903","text":"mensagem 3","type":"text"}
		],
		"delayMin":10,
		"delayMax":10,
		"scheduled_for":1,
		"info":"campanha csv"
	}`
	advancedReq := httptest.NewRequest(http.MethodPost, "/sender/advanced", strings.NewReader(body))
	advancedReq.Header.Set("Content-Type", "application/json")
	advancedReq.Header.Set("token", in.Token)
	advancedRec := httptest.NewRecorder()
	router.ServeHTTP(advancedRec, advancedReq)
	if advancedRec.Code != http.StatusOK {
		t.Fatalf("POST /sender/advanced status=%d body=%s", advancedRec.Code, advancedRec.Body.String())
	}
	var advanced struct {
		FolderID string `json:"folder_id"`
		Count    int    `json:"count"`
		Status   string `json:"status"`
	}
	if err := json.NewDecoder(advancedRec.Body).Decode(&advanced); err != nil {
		t.Fatal(err)
	}
	if advanced.FolderID == "" || advanced.Count != 3 || advanced.Status != "queued" {
		t.Fatalf("POST /sender/advanced response=%#v", advanced)
	}
	for index := range 3 {
		key := fmt.Sprintf("broadcast:%s:%d", advanced.FolderID, index)
		if _, err := m.store.GetQueueByKey(in.ID, key); err != nil {
			t.Fatalf("campaign message %d was not queued: %v", index, err)
		}
	}

	listReq := httptest.NewRequest(http.MethodGet, "/sender/listfolders", nil)
	listReq.Header.Set("token", in.Token)
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("GET /sender/listfolders status=%d body=%s", listRec.Code, listRec.Body.String())
	}
	var folders []struct {
		ID         string `json:"id"`
		Status     string `json:"status"`
		LogSuccess int    `json:"log_sucess"`
		LogTotal   int    `json:"log_total"`
	}
	if err := json.NewDecoder(listRec.Body).Decode(&folders); err != nil {
		t.Fatal(err)
	}
	if len(folders) != 1 || folders[0].ID != advanced.FolderID || folders[0].Status != "sending" || folders[0].LogSuccess != 0 || folders[0].LogTotal != 3 {
		t.Fatalf("GET /sender/listfolders response=%#v", folders)
	}
}
