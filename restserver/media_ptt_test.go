package main

import (
	"encoding/binary"
	"testing"

	"go.mau.fi/whatsmeow"
)

// oggPage builds a minimal Ogg page header carrying the given granule position.
func oggPage(granule uint64, payload []byte) []byte {
	page := make([]byte, 27, 27+1+len(payload))
	copy(page, "OggS")
	binary.LittleEndian.PutUint64(page[6:14], granule)
	page[26] = 1
	page = append(page, byte(len(payload)))
	return append(page, payload...)
}

func opusHead(preSkip uint16) []byte {
	head := make([]byte, 19)
	copy(head, "OpusHead")
	head[8] = 1
	head[9] = 1
	binary.LittleEndian.PutUint16(head[10:12], preSkip)
	return head
}

func TestResolveMediaKindAcceptsVoiceNoteAliases(t *testing.T) {
	for _, alias := range []string{"ptt", "myaudio", "voice"} {
		waType, ptt, err := resolveMediaKind(alias)
		if err != nil {
			t.Fatalf("%s: unexpected error %v", alias, err)
		}
		if waType != whatsmeow.MediaAudio || !ptt {
			t.Fatalf("%s: got type=%v ptt=%v, want audio+ptt", alias, waType, ptt)
		}
	}
}

func TestResolveMediaKindKeepsPlainAudioAsFile(t *testing.T) {
	waType, ptt, err := resolveMediaKind("audio")
	if err != nil || waType != whatsmeow.MediaAudio || ptt {
		t.Fatalf("audio: got type=%v ptt=%v err=%v", waType, ptt, err)
	}
	if _, _, err := resolveMediaKind("sticker"); err == nil {
		t.Fatal("unknown type must be rejected")
	}
}

func TestOggOpusSecondsReadsLastGranuleMinusPreSkip(t *testing.T) {
	data := append(oggPage(0, opusHead(312)), oggPage(48000*12+312, []byte{1, 2, 3})...)
	if got := oggOpusSeconds(data); got != 12 {
		t.Fatalf("seconds=%d, want 12", got)
	}
}

func TestOggOpusSecondsRoundsUpPartialSecond(t *testing.T) {
	data := append(oggPage(0, opusHead(0)), oggPage(48000*3+100, nil)...)
	if got := oggOpusSeconds(data); got != 4 {
		t.Fatalf("seconds=%d, want 4", got)
	}
}

func TestOggOpusSecondsIgnoresNonOgg(t *testing.T) {
	if got := oggOpusSeconds([]byte("ID3 not an ogg file")); got != 0 {
		t.Fatalf("seconds=%d, want 0", got)
	}
}

func TestVoiceNoteMessageIsMarkedPttWithOpusMime(t *testing.T) {
	up := whatsmeow.UploadResponse{URL: "https://mmg.example/x", DirectPath: "/x", FileLength: 10}
	data := append(oggPage(0, opusHead(0)), oggPage(48000*5, nil)...)
	msg := audioMessage(up, data, "application/ogg", true)
	if msg.GetPTT() != true {
		t.Fatal("voice note must set PTT=true")
	}
	if msg.GetMimetype() != "audio/ogg; codecs=opus" {
		t.Fatalf("mimetype=%q", msg.GetMimetype())
	}
	if msg.GetSeconds() != 5 {
		t.Fatalf("seconds=%d, want 5", msg.GetSeconds())
	}
}

func TestPlainAudioMessageKeepsDetectedMime(t *testing.T) {
	up := whatsmeow.UploadResponse{URL: "https://mmg.example/x", DirectPath: "/x", FileLength: 10}
	msg := audioMessage(up, []byte("mp3"), "audio/mpeg", false)
	if msg.GetPTT() || msg.GetMimetype() != "audio/mpeg" {
		t.Fatalf("ptt=%v mimetype=%q", msg.GetPTT(), msg.GetMimetype())
	}
}
