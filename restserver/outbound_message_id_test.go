package main

import (
	"fmt"
	"testing"
)

func TestQueuedMessageIDSurvivesRetry(t *testing.T) {
	calls := 0
	generate := func() string { calls++; return fmt.Sprint(calls) }
	first := outboundMessageID("durable-job-a", generate)
	if first != outboundMessageID("durable-job-a", generate) {
		t.Fatal("retry changed the message identity")
	}
	if first == outboundMessageID("durable-job-b", generate) {
		t.Fatal("distinct jobs share an identity")
	}
	if outboundMessageID("", generate) == outboundMessageID("", generate) {
		t.Fatal("direct sends must remain independent")
	}
}
