package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNativeBroadcastRoutesUseAdminAuthentication(t *testing.T) {
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
			{"number":"5567999999902","text":"mensagem 2","type":"text"}
		],
		"delayMin":10,
		"delayMax":10,
		"scheduled_for":1,
		"info":"campanha csv"
	}`

	createReq := httptest.NewRequest(http.MethodPost, "/instances/"+in.ID+"/broadcasts", strings.NewReader(body))
	createReq.Header.Set("Authorization", "Bearer admin-secret")
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("POST native broadcast status=%d body=%s", createRec.Code, createRec.Body.String())
	}

	var created struct {
		FolderID string `json:"folder_id"`
		Count    int    `json:"count"`
	}
	if err := json.NewDecoder(createRec.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if created.FolderID == "" || created.Count != 2 {
		t.Fatalf("POST native broadcast response=%#v", created)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/instances/"+in.ID+"/broadcasts", nil)
	listReq.Header.Set("Authorization", "Bearer admin-secret")
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("GET native broadcasts status=%d body=%s", listRec.Code, listRec.Body.String())
	}

	var folders []BroadcastFolder
	if err := json.NewDecoder(listRec.Body).Decode(&folders); err != nil {
		t.Fatal(err)
	}
	if len(folders) != 1 || folders[0].ID != created.FolderID {
		t.Fatalf("GET native broadcasts response=%#v", folders)
	}
}

func TestNativeBroadcastQueuesImageMessagesSpacedByDelay(t *testing.T) {
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
			{"number":"5567999999901","type":"image","file":"https://cdn.example.com/banner.png","text":"legenda 1"},
			{"number":"5567999999902","type":"image","file":"https://cdn.example.com/banner.png","text":"legenda 2"}
		],
		"delayMin":60,
		"delayMax":60
	}`
	req := httptest.NewRequest(http.MethodPost, "/instances/"+in.ID+"/broadcasts", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer admin-secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST image broadcast status=%d body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		FolderID string `json:"folder_id"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}

	first, err := m.store.GetQueueByKey(in.ID, "broadcast:"+created.FolderID+":0")
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.store.GetQueueByKey(in.ID, "broadcast:"+created.FolderID+":1")
	if err != nil {
		t.Fatal(err)
	}
	if first.Kind != "media" || second.Kind != "media" {
		t.Fatalf("kinds=%q,%q want media", first.Kind, second.Kind)
	}
	var payload queuedMediaPayload
	if err := json.Unmarshal([]byte(second.PayloadJSON), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Number != "5567999999902" || payload.Type != "image" || payload.File != "https://cdn.example.com/banner.png" || payload.Text != "legenda 2" {
		t.Fatalf("payload=%#v", payload)
	}
	firstAt, err := time.Parse(time.RFC3339Nano, first.AvailableAt)
	if err != nil {
		t.Fatalf("parse first availableAt %q: %v", first.AvailableAt, err)
	}
	secondAt, err := time.Parse(time.RFC3339Nano, second.AvailableAt)
	if err != nil {
		t.Fatalf("parse second availableAt %q: %v", second.AvailableAt, err)
	}
	if gap := secondAt.Sub(firstAt); gap < 59*time.Second || gap > 61*time.Second {
		t.Fatalf("gap=%s want ~60s", gap)
	}
}

func TestNativeBroadcastRejectsImageWithoutFile(t *testing.T) {
	t.Setenv("ADMIN_API_KEY", "admin-secret")
	cfg := loadConfig()
	m := testUazapiCompatManager(t, cfg)
	in, err := m.Create("nutricionist_46", "46", "", "")
	if err != nil {
		t.Fatal(err)
	}
	router := NewHandlers(m, cfg).Router()
	body := `{"messages":[{"number":"5567999999901","type":"image","text":"sem arquivo"}],"delayMin":1,"delayMax":1}`
	req := httptest.NewRequest(http.MethodPost, "/instances/"+in.ID+"/broadcasts", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer admin-secret")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", rec.Code, rec.Body.String())
	}
}
