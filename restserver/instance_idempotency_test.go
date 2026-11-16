package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInstanceCreationIdempotency(t *testing.T) {
	m := testUazapiCompatManager(t, Config{})
	router := NewHandlers(m, Config{}).Router()
	create := func(name string) (int, Instance) {
		req := httptest.NewRequest("POST", "/instances", strings.NewReader(`{"name":"`+name+`"}`))
		req.Header.Set("Idempotency-Key", "synthetic-nfefy")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		var in Instance
		_ = json.Unmarshal(rec.Body.Bytes(), &in)
		return rec.Code, in
	}
	status, first := create("nfefy")
	if status != 201 {
		t.Fatalf("first status = %d", status)
	}
	status, second := create("nfefy")
	if status != 201 || first.ID != second.ID {
		t.Fatalf("retry created another instance")
	}
	status, _ = create("different")
	if status != 409 {
		t.Fatalf("changed payload status = %d", status)
	}
}
