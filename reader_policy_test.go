package kafka

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
)

// blockingDialFunc blocks until the context is canceled. It makes child
// reader goroutines deterministic in broker-free tests: a canceled
// FetchMessage ctx wins its select, and Close releases the blocked dial.
func blockingDialFunc() func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
}

func newPolicyTestReader(t *testing.T) *Reader {
	t.Helper()
	r := NewReader(ReaderConfig{
		Brokers:   []string{"127.0.0.1:9092"},
		Topic:     "reader-policy-topic",
		Partition: 0,
		Dialer:    &Dialer{DialFunc: blockingDialFunc()},
	})
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func setReaderPosition(r *Reader, offset int64) {
	r.mutex.Lock()
	r.offset = offset
	r.mutex.Unlock()
}

func readerStartOffset(r *Reader) (offset int64, ok bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	offset, ok = r.lastStartOffsets[topicPartition{topic: r.config.Topic, partition: int32(r.config.Partition)}]
	return
}

// fetchWithCanceledContext drives one FetchMessage entry with an already
// canceled context: the policy-aware start path runs, then ctx wins the
// select deterministically because the dialer blocks.
func fetchWithCanceledContext(t *testing.T, r *Reader) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.FetchMessage(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// reject_commit: CommitMessages fails testably, enqueues nothing, and the
// gate is evaluated before group/standalone bookkeeping.
func TestReaderPolicyRejectCommit(t *testing.T) {
	r := newPolicyTestReader(t)

	frame := PushReaderPolicy(r, ReaderPolicy{OffsetCommit: OffsetCommitRejectCommit})

	if r.EffectiveFetchPolicy().CommitMessagesEnabled {
		t.Fatal("reject_commit must report CommitMessagesEnabled=false")
	}

	msg := Message{Topic: r.config.Topic, Partition: 0, Offset: 9}
	err := r.CommitMessages(context.Background(), msg)
	if !errors.Is(err, ErrReaderPolicyCommitRejected) {
		t.Fatalf("expected ErrReaderPolicyCommitRejected, got %v", err)
	}

	select {
	case <-r.commits:
		t.Fatal("rejected commit must not be enqueued, committed offset must not advance")
	default:
	}

	if err := frame.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}

	// Pin restored: standalone reader reaches its normal pre-commit check.
	if err := r.CommitMessages(context.Background(), msg); !errors.Is(err, errOnlyAvailableWithGroup) {
		t.Fatalf("expected errOnlyAvailableWithGroup after pop, got %v", err)
	}
}

// force_first: new consumption starts at FirstOffset, without mutating the
// reader position or config.StartOffset and without going through SetOffset.
func TestReaderPolicyForceFirst(t *testing.T) {
	r := newPolicyTestReader(t)
	setReaderPosition(r, 42)

	frame := PushReaderPolicy(r, ReaderPolicy{OffsetCommit: OffsetCommitForceFirst})

	eff := r.EffectiveFetchPolicy()
	if eff.OffsetCommit != OffsetCommitForceFirst || eff.StartOffset != FirstOffset || !eff.CommitMessagesEnabled {
		t.Fatalf("unexpected effective view: %+v", eff)
	}

	fetchWithCanceledContext(t, r)

	offset, ok := readerStartOffset(r)
	if !ok || offset != FirstOffset {
		t.Fatalf("child reader must start at FirstOffset, got %d (present=%v)", offset, ok)
	}

	r.mutex.Lock()
	position := r.offset
	configStart := r.config.StartOffset
	marker := r.forceFirstFrameID
	r.mutex.Unlock()

	if position != 42 {
		t.Fatalf("force_first must not rewrite the reader position, got %d", position)
	}
	if configStart != 0 {
		t.Fatalf("force_first must not mutate config.StartOffset, got %d", configStart)
	}
	if marker != frame.id || marker == 0 {
		t.Fatalf("force-first marker must record the applied frame id, marker=%d frame=%d", marker, frame.id)
	}

	if err := frame.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}

	if eff := r.EffectiveFetchPolicy(); eff.OffsetCommit != OffsetCommitPin || eff.StartOffset != 42 {
		t.Fatalf("popped view must restore the flat position, got %+v", eff)
	}
}

// A force_first frame applies once; a freshly pushed frame re-applies even
// though no fetch advanced the child-reader generation in between.
func TestReaderPolicyForceFirstRestartsOncePerFrame(t *testing.T) {
	r := newPolicyTestReader(t)
	setReaderPosition(r, 42)

	first := PushReaderPolicy(r, ReaderPolicy{OffsetCommit: OffsetCommitForceFirst})
	fetchWithCanceledContext(t, r)
	r.mutex.Lock()
	v1 := r.version
	marker1 := r.forceFirstFrameID
	r.mutex.Unlock()
	if marker1 != first.id || v1 != 1 {
		t.Fatalf("first frame not applied: marker=%d id=%d version=%d", marker1, first.id, v1)
	}

	// Same frame: another fetch must not restart again.
	fetchWithCanceledContext(t, r)
	r.mutex.Lock()
	vSame := r.version
	r.mutex.Unlock()
	if vSame != v1 {
		t.Fatalf("same force_first frame must not restart on every fetch: %d != %d", vSame, v1)
	}
	if err := first.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}

	// A fresh frame restarts at FirstOffset again and bumps the generation,
	// even though the generation did not move while the stack was empty.
	second := PushReaderPolicy(r, ReaderPolicy{OffsetCommit: OffsetCommitForceFirst})
	fetchWithCanceledContext(t, r)
	offset, _ := readerStartOffset(r)
	if offset != FirstOffset {
		t.Fatalf("fresh force_first frame must start at FirstOffset, got %d", offset)
	}
	r.mutex.Lock()
	v2 := r.version
	marker2 := r.forceFirstFrameID
	r.mutex.Unlock()
	if v2 <= v1 {
		t.Fatalf("fresh force_first frame must bump the generation: %d <= %d", v2, v1)
	}
	if marker2 != second.id {
		t.Fatalf("marker must record the new frame: marker=%d id=%d", marker2, second.id)
	}
	if err := second.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}
}

// Two readers on the SAME goroutine keep independent stack tops.
func TestReaderPolicyTwoInstancesSameGoroutine(t *testing.T) {
	r1 := newPolicyTestReader(t)
	r2 := newPolicyTestReader(t)
	setReaderPosition(r1, 42)
	setReaderPosition(r2, 77)

	frame := PushReaderPolicy(r1, ReaderPolicy{OffsetCommit: OffsetCommitForceFirst})
	defer frame.Pop()

	if r1.EffectiveFetchPolicy().Depth != 1 || r2.EffectiveFetchPolicy().Depth != 0 {
		t.Fatalf("stacks leaked across readers: r1=%d r2=%d", r1.EffectiveFetchPolicy().Depth, r2.EffectiveFetchPolicy().Depth)
	}

	fetchWithCanceledContext(t, r1)
	fetchWithCanceledContext(t, r2)

	o1, _ := readerStartOffset(r1)
	o2, _ := readerStartOffset(r2)
	if o1 != FirstOffset {
		t.Fatalf("r1 must start at FirstOffset under its own frame, got %d", o1)
	}
	if o2 != 77 {
		t.Fatalf("r2 must keep its own pinned position 77, got %d", o2)
	}

	// r1's force_first frame must not reject commits on r2.
	if err := r2.CommitMessages(context.Background(), Message{}); !errors.Is(err, errOnlyAvailableWithGroup) {
		t.Fatalf("r2 commits must not be gated by r1's frame: %v", err)
	}
}

// Concurrent readers with different frames never cross stacks.
func TestReaderPolicyConcurrentInstances(t *testing.T) {
	r1 := newPolicyTestReader(t)
	r2 := newPolicyTestReader(t)
	setReaderPosition(r1, 42)
	setReaderPosition(r2, 55)

	var wg sync.WaitGroup
	wg.Add(2)
	var f1, f2 *ReaderPolicyFrame
	go func() {
		defer wg.Done()
		f1 = PushReaderPolicy(r1, ReaderPolicy{OffsetCommit: OffsetCommitForceFirst})
	}()
	go func() {
		defer wg.Done()
		f2 = PushReaderPolicy(r2, ReaderPolicy{OffsetCommit: OffsetCommitRejectCommit})
	}()
	wg.Wait()
	defer f1.Pop()
	defer f2.Pop()

	fetchWithCanceledContext(t, r1)
	fetchWithCanceledContext(t, r2)

	o1, _ := readerStartOffset(r1)
	o2, _ := readerStartOffset(r2)
	if o1 != FirstOffset {
		t.Fatalf("r1 must start at FirstOffset, got %d", o1)
	}
	if o2 != 55 {
		t.Fatalf("r2 must start at its pinned position, got %d", o2)
	}

	// Cross-check the gates concurrently: r1 commits admitted, r2 rejected.
	const n = 16
	var cg sync.WaitGroup
	cg.Add(n * 2)
	for i := 0; i < n; i++ {
		go func() {
			defer cg.Done()
			if err := r1.CommitMessages(context.Background(), Message{}); !errors.Is(err, errOnlyAvailableWithGroup) {
				t.Errorf("r1 commit gate leaked: %v", err)
			}
		}()
		go func() {
			defer cg.Done()
			if err := r2.CommitMessages(context.Background(), Message{}); !errors.Is(err, ErrReaderPolicyCommitRejected) {
				t.Errorf("r2 commit must be rejected, got: %v", err)
			}
		}()
	}
	cg.Wait()

	select {
	case <-r2.commits:
		t.Fatal("r2 rejected commits must never be enqueued")
	default:
	}
}

// Nested frames and snapshot immutability after pop.
func TestReaderPolicyNestedFrames(t *testing.T) {
	r := newPolicyTestReader(t)
	setReaderPosition(r, 42)

	outer := PushReaderPolicy(r, ReaderPolicy{OffsetCommit: OffsetCommitForceFirst})
	inner := PushReaderPolicy(r, ReaderPolicy{OffsetCommit: OffsetCommitRejectCommit})

	// The inner frame replaces the top: reject_commit applies, its pinned
	// start offset falls back to the flat reader position; popping restores
	// the outer force_first.
	eff := r.EffectiveFetchPolicy()
	if eff.Depth != 2 || eff.OffsetCommit != OffsetCommitRejectCommit || eff.StartOffset != 42 ||
		eff.CommitMessagesEnabled {
		t.Fatalf("inner effective view wrong: %+v", eff)
	}

	if err := inner.Pop(); err != nil {
		t.Fatalf("Pop inner: %v", err)
	}
	if eff := r.EffectiveFetchPolicy(); eff.Depth != 1 || eff.OffsetCommit != OffsetCommitForceFirst ||
		!eff.CommitMessagesEnabled {
		t.Fatalf("outer frame not restored: %+v", eff)
	}

	if err := outer.Pop(); err != nil {
		t.Fatalf("Pop outer: %v", err)
	}
	if eff := r.EffectiveFetchPolicy(); eff.Depth != 0 || eff.OffsetCommit != OffsetCommitPin || eff.StartOffset != 42 {
		t.Fatalf("stack not empty/pinned: %+v", eff)
	}
	if err := outer.Pop(); !errors.Is(err, ErrReaderPolicyFramePopped) {
		t.Fatalf("double pop must return ErrReaderPolicyFramePopped, got %v", err)
	}
}

// A captured frame snapshot stays immutable/valid after it is popped and the
// stack is empty: in-flight operations are not silently rebound to another
// top.
func TestReaderPolicyFrameSnapshotImmutable(t *testing.T) {
	r := newPolicyTestReader(t)

	frame := PushReaderPolicy(r, ReaderPolicy{OffsetCommit: OffsetCommitForceFirst})
	if err := frame.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}

	if r.EffectiveFetchPolicy().Depth != 0 {
		t.Fatal("stack must be empty after pop")
	}
	if frame.Policy().OffsetCommit != OffsetCommitForceFirst {
		t.Fatal("popped frame snapshot was mutated")
	}
}

// WithReaderPolicy always pops, including panic paths.
func TestWithReaderPolicyAutoPop(t *testing.T) {
	r := newPolicyTestReader(t)

	func() {
		defer func() { _ = recover() }()
		_ = WithReaderPolicy(r, ReaderPolicy{OffsetCommit: OffsetCommitRejectCommit}, func() error {
			panic("boom")
		})
	}()

	if r.EffectiveFetchPolicy().Depth != 0 {
		t.Fatal("panic path must leave the policy stack empty")
	}
	if err := r.CommitMessages(context.Background(), Message{}); !errors.Is(err, errOnlyAvailableWithGroup) {
		t.Fatalf("commits must be admitted after auto-pop: %v", err)
	}
}

// Invalid policy values panic on push without leaving anything on the stack.
func TestReaderPolicyPushInvalidPanics(t *testing.T) {
	r := newPolicyTestReader(t)

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic on invalid enum")
			}
		}()
		PushReaderPolicy(r, ReaderPolicy{OffsetCommit: OffsetCommitAdmit(42)})
	}()

	if r.EffectiveFetchPolicy().Depth != 0 {
		t.Fatal("rejected push must not leave a frame on the stack")
	}
}
