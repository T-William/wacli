package app

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/out"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// A message is acked to WhatsApp only after it is stored. whatsmeow withholds the
// receipt when a success-flag handler returns false, so the server redelivers it.

func ackTestHandler(t *testing.T) (*App, *fakeWA, *bytes.Buffer) {
	t.Helper()
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f
	var evbuf bytes.Buffer
	a.opts.Events = out.NewEventWriter(&evbuf, true)
	var messagesStored, lastEvent atomic.Int64
	a.addSyncEventHandler(context.Background(), SyncOptions{}, &messagesStored, &lastEvent,
		make(chan struct{}, 1), make(chan struct{}, 1), make(chan staleReconnectRequest, 1),
		func(string, string) {}, nil, nil, &syncPresence{}, nil)
	return a, f, &evbuf
}

func ackTestMessage(id string) *events.Message {
	chat := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			ID:            types.MessageID(id),
			Timestamp:     time.Date(2026, 10, 28, 4, 4, 54, 0, time.UTC),
			PushName:      "Alice",
		},
		Message: &waProto.Message{Conversation: proto.String("hello")},
	}
}

func TestSyncHandlerAcksAStoredMessage(t *testing.T) {
	a, f, _ := ackTestHandler(t)
	if !f.emitForAck(ackTestMessage("m1")) {
		t.Fatal("stored message was refused; want ack")
	}
	if n, _ := a.db.CountMessages(); n != 1 {
		t.Fatalf("messages = %d, want 1", n)
	}
}

func TestSyncHandlerRefusesAckAndReportsWhenStoreFails(t *testing.T) {
	a, f, evbuf := ackTestHandler(t)
	_ = a.db.Close()
	if f.emitForAck(ackTestMessage("m2")) {
		t.Fatal("message acked although the store failed; it would be lost")
	}
	if !strings.Contains(evbuf.String(), `"event":"store_failed"`) || !strings.Contains(evbuf.String(), "m2") {
		t.Fatalf("no store_failed event naming the message: %s", evbuf.String())
	}
}

func TestSyncHandlerRefusesAckWhenItPanicsOnAMessage(t *testing.T) {
	_, f, _ := ackTestHandler(t)
	f.decryptReactionPanic = "boom"
	evt := ackTestMessage("m3")
	evt.Message = &waProto.Message{EncReactionMessage: &waProto.EncReactionMessage{
		TargetMessageKey: &waCommon.MessageKey{ID: proto.String("m1")}}}
	if f.emitForAck(evt) {
		t.Fatal("a message whose handler panicked was acked; it would be lost")
	}
	if f.emitForAck(&events.Receipt{}) == false {
		t.Fatal("after a panic, unrelated events must still be acked")
	}
}

func TestSyncHandlerReportsUndecryptableMessages(t *testing.T) {
	_, f, evbuf := ackTestHandler(t)
	f.emitForAck(&events.UndecryptableMessage{Info: ackTestMessage("m4").Info})
	if !strings.Contains(evbuf.String(), `"event":"undecryptable"`) || !strings.Contains(evbuf.String(), "m4") {
		t.Fatalf("no undecryptable event: %s", evbuf.String())
	}
}

func TestSyncHandlerKeepsAHistoryChunkWhoseStoreFailed(t *testing.T) {
	a, f, _ := ackTestHandler(t)
	chat := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	f.downloadHistory = func(*waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		return historySyncWithTextMessages(chat, time.Date(2026, 10, 28, 4, 0, 0, 0, time.UTC), "h1", "h2").Data, nil
	}
	syncType := waE2E.HistorySyncType_RECENT
	notif := &events.Message{
		Info: ackTestMessage("n1").Info,
		Message: &waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{
			HistorySyncNotification: &waE2E.HistorySyncNotification{SyncType: &syncType}}},
	}
	_ = a.db.Close()
	if f.emitForAck(notif) {
		t.Fatal("history notification acked although its messages were not stored")
	}
	if len(f.deleteHistoryCalls) != 0 {
		t.Fatal("history blob deleted from the server although its messages were not stored")
	}
}

func TestSyncHandlerAcksAndDeletesAStoredHistoryChunk(t *testing.T) {
	a, f, _ := ackTestHandler(t)
	chat := types.JID{User: "15551234567", Server: types.DefaultUserServer}
	f.downloadHistory = func(*waE2E.HistorySyncNotification) (*waHistorySync.HistorySync, error) {
		return historySyncWithTextMessages(chat, time.Date(2026, 10, 28, 4, 0, 0, 0, time.UTC), "h1", "h2").Data, nil
	}
	syncType := waE2E.HistorySyncType_RECENT
	notif := &events.Message{
		Info: ackTestMessage("n2").Info,
		Message: &waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{
			HistorySyncNotification: &waE2E.HistorySyncNotification{SyncType: &syncType}}},
	}
	if !f.emitForAck(notif) {
		t.Fatal("stored history chunk was refused")
	}
	if n, _ := a.db.CountMessages(); n != 2 {
		t.Fatalf("messages = %d, want 2", n)
	}
	if len(f.deleteHistoryCalls) != 1 {
		t.Fatalf("delete calls = %d, want 1", len(f.deleteHistoryCalls))
	}
}

func TestSyncHandlerRefusesAHistoryChunkThatFailedToDownload(t *testing.T) {
	_, f, _ := ackTestHandler(t)
	syncType := waE2E.HistorySyncType_RECENT
	notif := &events.Message{
		Info: ackTestMessage("n3").Info,
		Message: &waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{
			HistorySyncNotification: &waE2E.HistorySyncNotification{SyncType: &syncType}}},
	}
	if f.emitForAck(notif) { // fake download returns "not supported"
		t.Fatal("history notification acked although the download failed")
	}
}
