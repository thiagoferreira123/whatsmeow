package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
