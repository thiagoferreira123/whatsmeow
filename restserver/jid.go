package main

import (
	"errors"
	"regexp"
	"strings"

	"go.mau.fi/whatsmeow/types"
)

var nonDigit = regexp.MustCompile(`\D`)

// parseRecipient turns a phone number (e.g. "5511999998888", "+55 11 99999-8888")
// or an already-formed JID ("...@s.whatsapp.net", "...@g.us") into a types.JID.
func parseRecipient(number string) (types.JID, error) {
	n := strings.TrimSpace(number)
	if n == "" {
		return types.JID{}, errors.New("empty number")
	}
	if strings.Contains(n, "@") {
		return types.ParseJID(n)
	}
	digits := nonDigit.ReplaceAllString(n, "")
	if digits == "" {
		return types.JID{}, errors.New("number has no digits")
	}
	return types.NewJID(digits, types.DefaultUserServer), nil
}

// phoneCandidates returns the digit string plus, for Brazilian mobile numbers
// (DDI 55), the variant with/without the extra "9" digit. WhatsApp registers
// some Brazilian numbers without the 9th digit, so we let the server pick the
// real one via IsOnWhatsApp instead of guessing region rules.
func phoneCandidates(digits string) []string {
	out := []string{digits}
	if strings.HasPrefix(digits, "55") && len(digits) >= 12 {
		ddd := digits[2:4]
		sub := digits[4:]
		switch {
		case len(sub) == 9 && sub[0] == '9': // has the 9 -> add variant without it
			out = append(out, "55"+ddd+sub[1:])
		case len(sub) == 8: // no 9 -> add variant with it
			out = append(out, "55"+ddd+"9"+sub)
		}
	}
	return dedup(out)
}

// withPlus prefixes each number with "+" (IsOnWhatsApp wants international format).
func withPlus(nums []string) []string {
	out := make([]string, len(nums))
	for i, n := range nums {
		out[i] = "+" + n
	}
	return out
}

func dedup(in []string) []string {
	seen := map[string]bool{}
	out := in[:0]
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// resolveSender returns the phone-number JID (sender_pn) and the LID JID (sender)
// for an incoming message, handling both phone-addressed and LID-addressed chats.
func resolveSender(info types.MessageInfo) (senderPN string, senderLID string) {
	s := info.Sender
	alt := info.SenderAlt

	if s.Server == types.DefaultUserServer {
		senderPN = s.String()
	} else if s.Server == types.HiddenUserServer {
		senderLID = s.String()
	}
	if senderPN == "" && alt.Server == types.DefaultUserServer {
		senderPN = alt.String()
	}
	if senderLID == "" && alt.Server == types.HiddenUserServer {
		senderLID = alt.String()
	}
	if senderPN == "" {
		senderPN = s.String() // fallback so the field is never empty
	}
	return senderPN, senderLID
}
