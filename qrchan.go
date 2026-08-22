// Copyright (c) 2022 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type QRChannelItem struct {
	// The type of event, "code" for new QR codes (see Code field) and "error" for pairing errors (see Error) field.
	// For non-code/error events, you can just compare the whole item to the event variables (like QRChannelSuccess).
	Event string
	// If the item is a pair error, then this field contains the error message.
	Error error
	// If the item is a new code, then this field contains the raw data.
	Code string
	// The timeout after which the next code will be sent down the channel.
	Timeout time.Duration
}

const QRChannelEventCode = "code"
const QRChannelEventError = "error"

// Possible final items in the QR channel. In addition to these, an `error` event may be emitted,
// in which case the Error field will have the error that occurred during pairing.
var (
	// QRChannelSuccess is emitted from GetQRChannel when the pairing is successful.
	QRChannelSuccess = QRChannelItem{Event: "success"}
	// QRChannelTimeout is emitted from GetQRChannel if the socket gets disconnected by the server before the pairing is successful.
	QRChannelTimeout = QRChannelItem{Event: "timeout"}
	// QRChannelErrUnexpectedEvent is emitted from GetQRChannel if an unexpected connection event is received,
	// as that likely means that the pairing has already happened before the channel was set up.
	QRChannelErrUnexpectedEvent = QRChannelItem{Event: "err-unexpected-state"}
	// QRChannelClientOutdated is emitted from GetQRChannel if events.ClientOutdated is received.
	QRChannelClientOutdated = QRChannelItem{Event: "err-client-outdated"}
	// QRChannelScannedWithoutMultidevice is emitted from GetQRChannel if events.QRScannedWithoutMultidevice is received.
	QRChannelScannedWithoutMultidevice = QRChannelItem{Event: "err-scanned-without-multidevice"}
)

type qrChannel struct {
	sync.Mutex
	cli       *Client
	log       waLog.Logger
	ctx       context.Context
	handlerID uint32
	closed    atomic.Bool
	output    chan<- QRChannelItem
	stopQRs   chan struct{}
	done      chan struct{}
}

func (qrc *qrChannel) close() bool {
	return qrc.closed.Swap(true) == false
}

// finish serializes the final channel write and close with regular QR writes.
// This matters when the context is cancelled concurrently with a socket event.
func (qrc *qrChannel) finish(item *QRChannelItem) bool {
	qrc.Lock()
	defer qrc.Unlock()
	if !qrc.close() {
		return false
	}
	if item != nil {
		qrc.output <- *item
	}
	close(qrc.output)
	close(qrc.done)
	return true
}

func (qrc *qrChannel) send(item QRChannelItem, nonBlocking bool) bool {
	qrc.Lock()
	defer qrc.Unlock()
	if qrc.closed.Load() {
		return false
	}
	if nonBlocking {
		select {
		case qrc.output <- item:
			return true
		default:
			return false
		}
	}
	qrc.output <- item
	return true
}

func (qrc *qrChannel) emitQRs(codes []string) {
	var nextCode string
	for {
		if len(codes) == 0 {
			if qrc.finish(&QRChannelTimeout) {
				qrc.log.Debugf("Ran out of QR codes, closing channel with status %s and disconnecting client", QRChannelTimeout)
				go qrc.cli.RemoveEventHandler(qrc.handlerID)
				qrc.cli.Disconnect()
			} else {
				qrc.log.Debugf("Ran out of QR codes, but channel is already closed")
			}
			return
		} else if qrc.closed.Load() {
			qrc.log.Debugf("QR code channel is closed, exiting QR emitter")
			return
		}
		timeout := 20 * time.Second
		if len(codes) == 6 {
			timeout = 60 * time.Second
		}
		nextCode, codes = codes[0], codes[1:]
		qrc.log.Debugf("Emitting QR code %s", nextCode)
		if !qrc.send(QRChannelItem{Code: nextCode, Timeout: timeout, Event: QRChannelEventCode}, true) {
			qrc.log.Debugf("Output channel didn't accept code, exiting QR emitter")
			if qrc.finish(nil) {
				go qrc.cli.RemoveEventHandler(qrc.handlerID)
				qrc.cli.Disconnect()
			}
			return
		}
		select {
		case <-time.After(timeout):
		case <-qrc.stopQRs:
			qrc.log.Debugf("Got signal to stop QR emitter")
			return
		case <-qrc.cli.expectedDisconnect.GetChan():
			qrc.log.Debugf("Client is expected to disconnect, stopping QR emitter")
			return
		case <-qrc.ctx.Done():
			qrc.log.Debugf("Context is done, stopping QR emitter")
			if qrc.finish(nil) {
				go qrc.cli.RemoveEventHandler(qrc.handlerID)
				qrc.cli.Disconnect()
			}
			return
		}
	}
}

func (qrc *qrChannel) handleEvent(rawEvt any) {
	if qrc.closed.Load() {
		qrc.log.Debugf("Dropping event of type %T, channel is closed", rawEvt)
		return
	}
	var outputType QRChannelItem
	switch evt := rawEvt.(type) {
	case *events.QR:
		qrc.log.Debugf("Received QR code event, starting to emit codes to channel")
		go qrc.emitQRs(slices.Clone(evt.Codes))
		return
	case *events.QRScannedWithoutMultidevice:
		qrc.log.Debugf("QR code scanned without multidevice enabled")
		qrc.send(QRChannelScannedWithoutMultidevice, false)
		return
	case *events.ClientOutdated:
		outputType = QRChannelClientOutdated
	case *events.PairSuccess:
		outputType = QRChannelSuccess
	case *events.PairError:
		outputType = QRChannelItem{
			Event: QRChannelEventError,
			Error: evt.Error,
		}
	case *events.Disconnected:
		outputType = QRChannelTimeout
	case *events.Connected, *events.ConnectFailure, *events.LoggedOut, *events.TemporaryBan:
		outputType = QRChannelErrUnexpectedEvent
	default:
		return
	}
	if qrc.finish(&outputType) {
		close(qrc.stopQRs)
		qrc.log.Debugf("Closing channel with status %+v", outputType)
	} else {
		qrc.log.Debugf("Got status %+v, but channel is already closed", outputType)
	}
	// Has to be done in background because otherwise there's a deadlock with eventHandlersLock
	go qrc.cli.RemoveEventHandler(qrc.handlerID)
}

// GetQRChannel returns a channel that automatically outputs a new QR code when the previous one expires.
//
// This must be called *before* Connect(). It will then listen to all the relevant events from the client.
//
// The last value to be emitted will be a special event like "success", "timeout" or another error code
// depending on the result of the pairing. The channel will be closed immediately after one of those.
func (cli *Client) GetQRChannel(ctx context.Context) (<-chan QRChannelItem, error) {
	if cli == nil {
		return nil, ErrClientIsNil
	} else if cli.IsConnected() {
		return nil, ErrQRAlreadyConnected
	} else if cli.Store.ID != nil {
		return nil, ErrQRStoreContainsID
	}
	ch := make(chan QRChannelItem, 8)
	qrc := qrChannel{
		output:  ch,
		stopQRs: make(chan struct{}),
		done:    make(chan struct{}),
		cli:     cli,
		log:     cli.Log.Sub("QRChannel"),
		ctx:     ctx,
	}
	qrc.handlerID = cli.AddEventHandler(qrc.handleEvent)
	go func() {
		select {
		case <-ctx.Done():
			qrc.log.Debugf("Context cancelled before QR channel completed")
			if qrc.finish(nil) {
				go qrc.cli.RemoveEventHandler(qrc.handlerID)
				qrc.cli.Disconnect()
			}
		case <-qrc.done:
		}
	}()
	return ch, nil
}
