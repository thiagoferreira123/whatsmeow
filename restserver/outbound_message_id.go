package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func outboundMessageID(jobID string, generate func() string) string {
	if jobID != "" {
		// Queue IDs are durable and globally unique; derive before touching the
		// socket so restart/timeout retries reuse the same WhatsApp message ID.
		sum := sha256.Sum256([]byte("whatsmeow-queue-v1:" + jobID))
		return "3EB0" + strings.ToUpper(hex.EncodeToString(sum[:9]))
	}
	return generate()
}
