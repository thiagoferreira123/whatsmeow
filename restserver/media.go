package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// resolveFile accepts a remote URL, a data URI ("data:<mime>;base64,..."),
// or a raw base64 string, and returns the decoded bytes plus the mime type
// (empty if it could not be derived from the input).
func resolveFile(file string) (data []byte, mime string, err error) {
	file = strings.TrimSpace(file)
	switch {
	case strings.HasPrefix(file, "data:"):
		comma := strings.IndexByte(file, ',')
		if comma < 0 {
			return nil, "", fmt.Errorf("invalid data URI")
		}
		header := file[len("data:"):comma]
		payload := file[comma+1:]
		mime = strings.SplitN(header, ";", 2)[0]
		if strings.Contains(header, "base64") {
			data, err = base64.StdEncoding.DecodeString(payload)
			return data, mime, err
		}
		return []byte(payload), mime, nil

	case strings.HasPrefix(file, "http://"), strings.HasPrefix(file, "https://"):
		client := &http.Client{Timeout: 60 * time.Second}
		resp, err := client.Get(file)
		if err != nil {
			return nil, "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, "", fmt.Errorf("download failed: %s", resp.Status)
		}
		data, err = io.ReadAll(io.LimitReader(resp.Body, 64<<20)) // 64 MB cap
		return data, resp.Header.Get("Content-Type"), err

	default:
		data, err = base64.StdEncoding.DecodeString(file)
		if err != nil {
			return nil, "", fmt.Errorf("file is not a URL, data URI, or valid base64: %w", err)
		}
		return data, "", nil
	}
}

func detectMime(data []byte, given string) string {
	given = strings.TrimSpace(given)
	if given != "" && given != "application/octet-stream" {
		return given
	}
	return http.DetectContentType(data)
}

// inferMediaType maps a mime type to one of our media categories.
func inferMediaType(mime string) string {
	switch {
	case strings.HasPrefix(mime, "image/"):
		return "image"
	case strings.HasPrefix(mime, "video/"):
		return "video"
	case strings.HasPrefix(mime, "audio/"):
		return "audio"
	default:
		return "document"
	}
}

// buildMediaMessage resolves a file reference (URL/base64/dataURI) then uploads it.
func buildMediaMessage(ctx context.Context, cli *whatsmeow.Client, mediaType, file, caption, fileName string) (*waE2E.Message, error) {
	data, mime, err := resolveFile(file)
	if err != nil {
		return nil, err
	}
	return buildMediaMessageBytes(ctx, cli, mediaType, data, mime, caption, fileName)
}

// buildMediaMessageBytes uploads raw bytes to WhatsApp and returns a ready
// waE2E.Message for the requested media type (inferred from mime if empty).
func buildMediaMessageBytes(ctx context.Context, cli *whatsmeow.Client, mediaType string, data []byte, mime, caption, fileName string) (*waE2E.Message, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("file is empty")
	}
	mime = detectMime(data, mime)
	if strings.TrimSpace(mediaType) == "" {
		mediaType = inferMediaType(mime)
	}

	var waType whatsmeow.MediaType
	switch mediaType {
	case "image":
		waType = whatsmeow.MediaImage
	case "video":
		waType = whatsmeow.MediaVideo
	case "audio":
		waType = whatsmeow.MediaAudio
	case "document", "doc", "file":
		waType = whatsmeow.MediaDocument
	default:
		return nil, fmt.Errorf("unsupported media type %q (use image|video|audio|document)", mediaType)
	}

	up, err := cli.Upload(ctx, data, waType)
	if err != nil {
		return nil, fmt.Errorf("upload failed: %w", err)
	}

	msg := &waE2E.Message{}
	switch waType {
	case whatsmeow.MediaImage:
		msg.ImageMessage = &waE2E.ImageMessage{
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(up.FileLength),
			Mimetype:      proto.String(mime),
			Caption:       optString(caption),
		}
	case whatsmeow.MediaVideo:
		msg.VideoMessage = &waE2E.VideoMessage{
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(up.FileLength),
			Mimetype:      proto.String(mime),
			Caption:       optString(caption),
		}
	case whatsmeow.MediaAudio:
		msg.AudioMessage = &waE2E.AudioMessage{
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(up.FileLength),
			Mimetype:      proto.String(mime),
		}
	case whatsmeow.MediaDocument:
		name := strings.TrimSpace(fileName)
		if name == "" {
			name = "file"
		}
		msg.DocumentMessage = &waE2E.DocumentMessage{
			URL:           proto.String(up.URL),
			DirectPath:    proto.String(up.DirectPath),
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    proto.Uint64(up.FileLength),
			Mimetype:      proto.String(mime),
			FileName:      proto.String(name),
			Caption:       optString(caption),
		}
	}
	return msg, nil
}

func optString(s string) *string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return proto.String(s)
}
