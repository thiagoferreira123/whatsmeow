package main

import (
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

func TestPanelGroupsListsOnlyGroupsSortedByName(t *testing.T) {
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	groups := []*types.GroupInfo{
		{JID: types.NewJID("120363000000000002", types.GroupServer), GroupName: types.GroupName{Name: "Zebra"}, Participants: make([]types.GroupParticipant, 3)},
		{JID: types.NewJID("120363000000000001", types.GroupServer), GroupName: types.GroupName{Name: "Alfa"}, GroupCreated: created},
		{JID: types.NewJID("5567981490781", types.DefaultUserServer), GroupName: types.GroupName{Name: "não é grupo"}},
		nil,
	}
	items := panelGroups(groups)
	if len(items) != 2 {
		t.Fatalf("expected the two groups, got %v", items)
	}
	if items[0].Name != "Alfa" || items[0].JID != "120363000000000001@g.us" || items[0].CreatedAt != "2026-01-02T03:04:05Z" || items[0].Participants != 0 {
		t.Fatalf("first group misdescribed: %+v", items[0])
	}
	if items[1].Name != "Zebra" || items[1].JID != "120363000000000002@g.us" || items[1].CreatedAt != "" || items[1].Participants != 3 {
		t.Fatalf("second group misdescribed: %+v", items[1])
	}
	if got := panelGroups(nil); len(got) != 0 {
		t.Fatalf("no groups must be an empty list, got %v", got)
	}
}
