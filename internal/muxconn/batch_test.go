package muxconn

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	cryptopkg "github.com/openlibrecommunity/olcrtc/internal/crypto"
	"github.com/openlibrecommunity/olcrtc/internal/transport"
)

// ai-generated: the whole file (tests of write batching, olcrtc#11).

const batchTestPayload = 12 * 1024

// timedLink records every message with the time it was sent. A send waits
// for hold when hold is set.
type timedLink struct {
	stubLink
	tmu  sync.Mutex
	at   []time.Time
	hold chan struct{}
}

func newTimedLink(interval time.Duration) *timedLink {
	l := &timedLink{}
	l.canSend = true
	l.features = transport.Features{MaxPayloadSize: batchTestPayload, WriteInterval: interval}
	return l
}

func (l *timedLink) Send(data []byte) error {
	l.tmu.Lock()
	l.at = append(l.at, time.Now())
	l.tmu.Unlock()
	if l.hold != nil {
		<-l.hold
	}
	return l.stubLink.Send(data)
}

// waitCalls waits until n sends have begun.
func (l *timedLink) waitCalls(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		l.tmu.Lock()
		calls := len(l.at)
		l.tmu.Unlock()
		if calls >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sends began within a second, want %d", calls, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitSent waits until the link carried n messages and returns them with
// their send times.
func (l *timedLink) waitSent(t *testing.T, n int, within time.Duration) ([][]byte, []time.Time) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		l.mu.Lock()
		l.tmu.Lock()
		sent, at := append([][]byte(nil), l.sent...), append([]time.Time(nil), l.at...)
		l.tmu.Unlock()
		l.mu.Unlock()
		if len(sent) >= n {
			return sent, at
		}
		if time.Now().After(deadline) {
			t.Fatalf("link carried %d messages within %v, want %d", len(sent), within, n)
		}
		time.Sleep(time.Millisecond)
	}
}

func openRecord(t *testing.T, keys *cryptopkg.KeySet, record []byte) []byte {
	t.Helper()
	plain, err := keys.Open(record, []byte(dataRecordAAD))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return plain
}

// waitTaken waits until the flusher has taken n batches.
func waitTaken(t *testing.T, conn *Conn, n uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		conn.batch.mu.Lock()
		taken := conn.batch.taken
		conn.batch.mu.Unlock()
		if taken >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the flusher took %d batches within a second, want %d", taken, n)
		}
		time.Sleep(time.Millisecond)
	}
}

func mustWrite(t *testing.T, conn *Conn, frame []byte) {
	t.Helper()
	if n, err := conn.Write(frame); err != nil || n != len(frame) {
		t.Fatalf("Write() = %d, %v; want %d, nil", n, err, len(frame))
	}
}

func TestBatchingSendsALoneFrameAtOnce(t *testing.T) {
	clientKeys, serverKeys := newTestKeyPair(t)
	link := newTimedLink(time.Hour)
	conn := New(link, clientKeys)
	defer func() { _ = conn.Close() }()

	frame := smuxFrame(2, 3, []byte("hello"))
	mustWrite(t, conn, frame)
	sent, _ := link.waitSent(t, 1, time.Second)
	if got := openRecord(t, serverKeys, sent[0]); !bytes.Equal(got, frame) {
		t.Fatalf("record = %x, want %x", got, frame)
	}
}

func TestBatchingCarriesFramesWrittenCloseTogetherInOneRecord(t *testing.T) {
	const interval = 100 * time.Millisecond
	clientKeys, serverKeys := newTestKeyPair(t)
	link := newTimedLink(interval)
	link.hold = make(chan struct{})
	conn := New(link, clientKeys)
	defer func() { _ = conn.Close() }()

	first, second, third := smuxFrame(0, 3, nil), smuxFrame(2, 3, []byte("connect")), smuxFrame(0, 5, nil)
	mustWrite(t, conn, first)
	link.waitCalls(t, 1) // the first record is on its way and holds the flusher
	mustWrite(t, conn, second)
	mustWrite(t, conn, third)
	released := time.Now()
	close(link.hold)
	sent, at := link.waitSent(t, 2, 3*time.Second)
	if got, want := openRecord(t, serverKeys, sent[1]), append(bytes.Clone(second), third...); !bytes.Equal(got, want) {
		t.Fatalf("second record = %x, want both frames %x", got, want)
	}
	if gap := at[1].Sub(released); gap < interval-interval/10 {
		t.Fatalf("second record left %v after the first, want about %v", gap, interval)
	}
	time.Sleep(interval + interval/2)
	if sent, _ := link.waitSent(t, 2, 0); len(sent) != 2 {
		t.Fatalf("link carried %d messages, want 2", len(sent))
	}
}

func TestBatchingSendsAHalfFullBatchWithoutWaiting(t *testing.T) {
	clientKeys, serverKeys := newTestKeyPair(t)
	link := newTimedLink(time.Hour)
	conn := New(link, clientKeys)
	defer func() { _ = conn.Close() }()

	mustWrite(t, conn, smuxFrame(0, 3, nil))
	link.waitSent(t, 1, time.Second)
	big := smuxFrame(2, 3, make([]byte, (batchTestPayload-cryptopkg.WireOverhead)/2))
	mustWrite(t, conn, big)
	sent, _ := link.waitSent(t, 2, time.Second)
	if got := openRecord(t, serverKeys, sent[1]); !bytes.Equal(got, big) {
		t.Fatalf("second record is %d bytes, want the %d-byte frame", len(got), len(big))
	}
}

func TestBatchedRecordsReadBackAsTheFramesInOrder(t *testing.T) {
	clientKeys, serverKeys := newTestKeyPair(t)
	link := newTimedLink(2 * time.Millisecond)
	conn := New(link, clientKeys)
	defer func() { _ = conn.Close() }()

	var want []byte
	var payload uint64
	for i := range 300 {
		frame := smuxFrame(2, uint32(3+2*(i%7)), bytes.Repeat([]byte{byte(i)}, i%97))
		want = append(want, frame...)
		payload += uint64(i % 97)
		mustWrite(t, conn, frame)
	}
	peer := New(&stubLink{canSend: true}, serverKeys)
	defer func() { _ = peer.Close() }()
	got := make([]byte, 0, len(want))
	for len(got) < len(want) {
		sent, _ := link.waitSent(t, 1, 3*time.Second)
		link.mu.Lock()
		link.sent = link.sent[len(sent):]
		link.mu.Unlock()
		for _, record := range sent {
			peer.Push(record)
		}
		buf := make([]byte, len(want))
		for peer.inBytes.Load() > uint64(len(got)) {
			n, err := peer.Read(buf)
			if err != nil {
				t.Fatalf("Read() error = %v", err)
			}
			got = append(got, buf[:n]...)
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatal("the frames read back differ from the frames written")
	}
	if peer.PayloadBytes() != payload {
		t.Fatalf("PayloadBytes() = %d, want %d: every frame of a record counts", peer.PayloadBytes(), payload)
	}
}

func TestBatchingReportsAFailedSendOnALaterWrite(t *testing.T) {
	clientKeys, _ := newTestKeyPair(t)
	link := newTimedLink(time.Millisecond)
	link.sendErr = errMuxBoom
	conn := New(link, clientKeys)
	defer func() { _ = conn.Close() }()

	mustWrite(t, conn, smuxFrame(0, 3, nil))
	deadline := time.Now().Add(time.Second)
	for {
		_, err := conn.Write(smuxFrame(0, 5, nil))
		if errors.Is(err, errMuxBoom) {
			return
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("Write() after a failed send = %v, want %v", err, errMuxBoom)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBatchingCloseReleasesAWriterWaitingForRoom(t *testing.T) {
	clientKeys, _ := newTestKeyPair(t)
	link := newTimedLink(time.Millisecond)
	link.canSend = false // the flusher waits for the link with the first record
	link.features.MaxPayloadSize = cryptopkg.WireOverhead + 64
	conn := New(link, clientKeys)

	small := smuxFrame(2, 3, make([]byte, 16)) // 24 bytes: under half of 64
	mustWrite(t, conn, small)
	waitTaken(t, conn, 1)     // the flusher holds it and waits for the link
	mustWrite(t, conn, small) // the next batch
	done := make(chan error, 1)
	go func() {
		_, err := conn.Write(smuxFrame(2, 3, make([]byte, 40))) // 48 bytes: no room left
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("Write() with no room returned %v, want it to wait", err)
	case <-time.After(50 * time.Millisecond):
	}
	_ = conn.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Write() after Close = %v, want %v", err, ErrClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not release the waiting writer")
	}
}

// A frame that makes the batch due is written through: its Write returns
// once the record has gone to the link, so bulk data waits in smux, not
// here, while the link is busy.
func TestBatchingWritesADueBatchThrough(t *testing.T) {
	clientKeys, _ := newTestKeyPair(t)
	link := newTimedLink(time.Hour)
	link.hold = make(chan struct{})
	conn := New(link, clientKeys)
	defer func() { _ = conn.Close() }()

	big := smuxFrame(2, 3, make([]byte, (batchTestPayload-cryptopkg.WireOverhead)/2))
	done := make(chan error, 1)
	go func() {
		_, err := conn.Write(big)
		done <- err
	}()
	link.waitCalls(t, 1)
	select {
	case err := <-done:
		t.Fatalf("Write() returned %v before its record went to the link", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(link.hold)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Write() = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Write() did not return once its record went")
	}
}

func TestLinkWithoutWriteIntervalSendsEveryWriteAsItsOwnRecord(t *testing.T) {
	clientKeys, _ := newTestKeyPair(t)
	link := &stubLink{canSend: true, features: transport.Features{MaxPayloadSize: batchTestPayload}}
	conn := New(link, clientKeys)
	defer func() { _ = conn.Close() }()

	for range 3 {
		mustWrite(t, conn, smuxFrame(0, 3, nil))
	}
	link.mu.Lock()
	defer link.mu.Unlock()
	if len(link.sent) != 3 {
		t.Fatalf("link carried %d messages for 3 writes, want 3", len(link.sent))
	}
}

func TestPayloadBytesCountsEveryFrameOfARecord(t *testing.T) {
	clientKeys, serverKeys := newTestKeyPair(t)
	conn := New(&stubLink{canSend: true}, serverKeys)
	defer func() { _ = conn.Close() }()

	record := make([]byte, 0, 64)
	record = append(record, smuxFrame(4, 3, make([]byte, 8))...)
	record = append(record, smuxFrame(2, 3, []byte("abc"))...)
	record = append(record, smuxFrame(2, 0, []byte("zz"))...)
	record = append(record, smuxFrame(2, 5, []byte("hello"))...)
	sealed, err := clientKeys.Seal(record, []byte(dataRecordAAD))
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	conn.Push(sealed)
	if got := conn.PayloadBytes(); got != 3+5 {
		t.Fatalf("PayloadBytes() = %d, want 8: both stream PSH frames of the record", got)
	}
	if _, err := io.ReadFull(conn, make([]byte, len(record))); err != nil {
		t.Fatalf("ReadFull() error = %v", err)
	}
}
