package kafka

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metadataAPI "github.com/segmentio/kafka-go/protocol/metadata"
	produceAPI "github.com/segmentio/kafka-go/protocol/produce"
)

// fakePolicyTransport is a broker-free RoundTripper. Metadata requests get a
// synthetic 4-partition layout and Produce requests record the requested acks
// and partition, optionally blocking until a release channel is closed.
type fakePolicyTransport struct {
	mu sync.Mutex

	metadataCalls int
	produceCalls  int
	acks          []int16
	partitions    []int32

	// entered is signaled (non-blocking) when a Produce request arrives.
	entered chan struct{}

	// release, when non-nil and open, blocks the Produce response until
	// closed.
	release chan struct{}

	// fail makes Produce return a plain, non-transient error.
	fail bool
}

func (t *fakePolicyTransport) RoundTrip(_ context.Context, _ net.Addr, req Request) (Response, error) {
	switch r := req.(type) {
	case *metadataAPI.Request:
		t.mu.Lock()
		t.metadataCalls++
		t.mu.Unlock()

		topics := make([]metadataAPI.ResponseTopic, 0, len(r.TopicNames))
		for _, name := range r.TopicNames {
			partitions := make([]metadataAPI.ResponsePartition, 4)
			for i := range partitions {
				partitions[i] = metadataAPI.ResponsePartition{PartitionIndex: int32(i)}
			}
			topics = append(topics, metadataAPI.ResponseTopic{Name: name, Partitions: partitions})
		}
		return &metadataAPI.Response{Topics: topics}, nil

	case *produceAPI.Request:
		var (
			acks      int16
			partition int32
			topic     string
		)
		if len(r.Topics) > 0 {
			topic = r.Topics[0].Topic
			acks = r.Acks
			if len(r.Topics[0].Partitions) > 0 {
				partition = r.Topics[0].Partitions[0].Partition
			}
		}

		t.mu.Lock()
		t.produceCalls++
		t.acks = append(t.acks, acks)
		t.partitions = append(t.partitions, partition)
		t.mu.Unlock()

		if t.entered != nil {
			select {
			case t.entered <- struct{}{}:
			default:
			}
		}

		if t.release != nil {
			<-t.release
		}

		if t.fail {
			return nil, errors.New("fakePolicyTransport: injected produce failure")
		}

		return &produceAPI.Response{
			Topics: []produceAPI.ResponseTopic{{
				Topic: topic,
				Partitions: []produceAPI.ResponsePartition{{
					Partition: partition,
				}},
			}},
		}, nil
	}

	return nil, errors.New("fakePolicyTransport: unexpected request type")
}

func (t *fakePolicyTransport) snapshot() (metadata, produce int, acks []int16, partitions []int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	metadata = t.metadataCalls
	produce = t.produceCalls
	acks = append(acks, t.acks...)
	partitions = append(partitions, t.partitions...)
	return
}

type completionRecorder struct {
	mu      sync.Mutex
	success int
	failure int
}

func (c *completionRecorder) callback(_ []Message, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil {
		c.success++
	} else {
		c.failure++
	}
}

func (c *completionRecorder) counts() (success, failure int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.success, c.failure
}

func newPolicyTestWriter(rt RoundTripper, async bool, rec *completionRecorder) *Writer {
	w := &Writer{
		Addr:         TCP("127.0.0.1:9092"),
		Topic:        "policy-test-topic",
		Transport:    rt,
		RequiredAcks: RequireNone,
		Async:        async,
		BatchSize:    1,
		BatchTimeout: time.Minute,
		WriteTimeout: 10 * time.Second,
		MaxAttempts:  1,
	}
	if rec != nil {
		w.Completion = rec.callback
	}
	return w
}

func policyMsg(key []byte) Message {
	return Message{Key: key, Value: []byte("v")}
}

func waitEntered(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("produce request never entered the fake transport")
	}
}

func assertWriteBlocked(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("WriteMessages returned while ack was still pending: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
}

// Empty stack == historical behavior.
func TestWriterPolicyEmptyStackIsPinned(t *testing.T) {
	rt := &fakePolicyTransport{}
	w := newPolicyTestWriter(rt, false, nil)
	defer w.Close()

	if err := w.WriteMessages(context.Background(), policyMsg([]byte("k"))); err != nil {
		t.Fatalf("WriteMessages: %v", err)
	}

	_, produce, acks, _ := rt.snapshot()
	if produce != 1 || len(acks) != 1 || acks[0] != int16(RequireNone) {
		t.Fatalf("expected 1 produce with acks=0, got produce=%d acks=%v", produce, acks)
	}

	eff := w.EffectiveWritePolicy()
	if eff.FrameID != 0 || eff.Depth != 0 ||
		eff.AckSurface != ProduceAckPin ||
		eff.KeyPartition != KeyPartitionPin ||
		eff.BatchGate != WriteBatchPin ||
		!eff.WaitForDelivery {
		t.Fatalf("empty stack must report a fully pinned view, got %+v", eff)
	}
}

// force_all: async WriteMessages observably waits for RequireAll, success
// Completion cannot fire before the ack, and RequiredAcks flat field is never
// mutated.
func TestWriterPolicyForceAllAsyncWaitsForAck(t *testing.T) {
	rt := &fakePolicyTransport{
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	rec := &completionRecorder{}
	w := newPolicyTestWriter(rt, true, rec)
	defer w.Close()

	frame := PushWriterPolicy(w, WriterPolicy{AckSurface: ProduceAckForceAll})
	defer frame.Pop()

	eff := w.EffectiveWritePolicy()
	if eff.RequiredAcks != RequireAll || !eff.WaitForDelivery || eff.AckSurface != ProduceAckForceAll {
		t.Fatalf("unexpected effective view: %+v", eff)
	}
	if w.RequiredAcks != RequireNone {
		t.Fatalf("flat RequiredAcks must not be mutated, got %v", w.RequiredAcks)
	}

	done := make(chan error, 1)
	go func() { done <- w.WriteMessages(context.Background(), policyMsg([]byte("k"))) }()

	waitEntered(t, rt.entered)
	assertWriteBlocked(t, done)

	if ok, fail := rec.counts(); ok != 0 || fail != 0 {
		t.Fatalf("Completion must not fire before full ack, got success=%d failure=%d", ok, fail)
	}

	close(rt.release)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WriteMessages: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WriteMessages did not return after acks arrived")
	}

	if ok, _ := rec.counts(); ok != 1 {
		t.Fatalf("expected one success Completion after ack, got %d", ok)
	}

	_, produce, acks, _ := rt.snapshot()
	if produce != 1 || acks[0] != int16(RequireAll) {
		t.Fatalf("expected 1 produce with acks=-1, got produce=%d acks=%v", produce, acks)
	}
}

// Popping force_all restores async fire-and-forget behavior with acks=0.
func TestWriterPolicyPopRestoresPinnedAsync(t *testing.T) {
	rt := &fakePolicyTransport{}
	w := newPolicyTestWriter(rt, true, nil)
	defer w.Close()

	frame := PushWriterPolicy(w, WriterPolicy{AckSurface: ProduceAckForceAll})
	if err := frame.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}

	if err := w.WriteMessages(context.Background(), policyMsg([]byte("k"))); err != nil {
		t.Fatalf("async pin WriteMessages: %v", err)
	}

	// Close flushes the pending async batch; its produce must use the flat
	// RequireNone level.
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, produce, acks, _ := rt.snapshot()
	if produce != 1 || acks[0] != int16(RequireNone) {
		t.Fatalf("expected 1 produce with acks=0 after pop, got produce=%d acks=%v", produce, acks)
	}
}

// suppress_completion: no success callback, but failure callback still fires.
func TestWriterPolicySuppressCompletion(t *testing.T) {
	rt := &fakePolicyTransport{}
	rec := &completionRecorder{}
	w := newPolicyTestWriter(rt, false, rec)
	defer w.Close()

	frame := PushWriterPolicy(w, WriterPolicy{AckSurface: ProduceAckSuppressCompletion})

	if err := w.WriteMessages(context.Background(), policyMsg([]byte("k"))); err != nil {
		t.Fatalf("WriteMessages: %v", err)
	}
	if ok, fail := rec.counts(); ok != 0 || fail != 0 {
		t.Fatalf("suppressed success must not invoke Completion, got success=%d failure=%d", ok, fail)
	}

	if err := frame.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}

	// Pin restored: success callback fires again.
	if err := w.WriteMessages(context.Background(), policyMsg([]byte("k2"))); err != nil {
		t.Fatalf("WriteMessages: %v", err)
	}
	if ok, _ := rec.counts(); ok != 1 {
		t.Fatalf("expected one success Completion after pop, got %d", ok)
	}

	// Failures under suppression are still reported through Completion.
	rt.fail = true
	failFrame := PushWriterPolicy(w, WriterPolicy{AckSurface: ProduceAckSuppressCompletion})
	err := w.WriteMessages(context.Background(), policyMsg([]byte("k3")))
	if err == nil {
		t.Fatal("expected write error from failing transport")
	}
	if _, fail := rec.counts(); fail != 1 {
		t.Fatalf("failure Completion must still fire under suppress_completion, got %d", fail)
	}
	if err := failFrame.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}
}

// require_key: keyless messages fail testably with no metadata and no Produce.
func TestWriterPolicyRequireKey(t *testing.T) {
	rt := &fakePolicyTransport{}
	w := newPolicyTestWriter(rt, false, nil)
	defer w.Close()

	frame := PushWriterPolicy(w, WriterPolicy{KeyPartition: KeyPartitionRequireKey})

	err := w.WriteMessages(context.Background(), Message{Value: []byte("no-key")})
	if !errors.Is(err, ErrWriterPolicyKeyRequired) {
		t.Fatalf("expected ErrWriterPolicyKeyRequired, got %v", err)
	}

	if metadata, produce, _, _ := rt.snapshot(); metadata != 0 || produce != 0 {
		t.Fatalf("rejected frame must send nothing, got metadata=%d produce=%d", metadata, produce)
	}

	if err := w.WriteMessages(context.Background(), policyMsg([]byte("with-key"))); err != nil {
		t.Fatalf("keyed write: %v", err)
	}
	if _, produce, _, _ := rt.snapshot(); produce != 1 {
		t.Fatalf("keyed write must produce, got %d", produce)
	}

	if err := frame.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}

	// Pin restored: keyless write goes through.
	if err := w.WriteMessages(context.Background(), Message{Value: []byte("no-key-again")}); err != nil {
		t.Fatalf("pinned keyless write: %v", err)
	}
}

// force_partition: Message.Partition wins, Balancer is not called, metadata
// partition count is not needed.
func TestWriterPolicyForcePartition(t *testing.T) {
	rt := &fakePolicyTransport{}
	w := newPolicyTestWriter(rt, false, nil)
	defer w.Close()

	var balancerCalls int32
	w.Balancer = BalancerFunc(func(_ Message, _ ...int) int {
		atomic.AddInt32(&balancerCalls, 1)
		return 0
	})

	frame := PushWriterPolicy(w, WriterPolicy{KeyPartition: KeyPartitionForcePartition})

	msg := policyMsg([]byte("k"))
	msg.Partition = 3
	if err := w.WriteMessages(context.Background(), msg); err != nil {
		t.Fatalf("WriteMessages: %v", err)
	}

	if atomic.LoadInt32(&balancerCalls) != 0 {
		t.Fatal("balancer must not be invoked under force_partition")
	}
	if metadata, produce, _, parts := rt.snapshot(); metadata != 0 || produce != 1 || len(parts) != 1 || parts[0] != 3 {
		t.Fatalf("expected direct produce to partition 3 with no metadata, got metadata=%d produce=%d parts=%v", metadata, produce, parts)
	}

	if err := frame.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}
}

// The three dimensions are independent: one frame can combine force_all with
// require_key and force_partition.
func TestWriterPolicyCombinedDimensions(t *testing.T) {
	rt := &fakePolicyTransport{}
	w := newPolicyTestWriter(rt, true, nil) // async pin: would normally use acks=0
	defer w.Close()

	var balancerCalls int32
	w.Balancer = BalancerFunc(func(_ Message, _ ...int) int {
		atomic.AddInt32(&balancerCalls, 1)
		return 0
	})

	frame := PushWriterPolicy(w, WriterPolicy{
		AckSurface:   ProduceAckForceAll,
		KeyPartition: KeyPartitionForcePartition,
	})
	defer frame.Pop()

	msg := policyMsg([]byte("k"))
	msg.Partition = 2
	if err := w.WriteMessages(context.Background(), msg); err != nil {
		t.Fatalf("combined write: %v", err)
	}

	if atomic.LoadInt32(&balancerCalls) != 0 {
		t.Fatal("balancer must not run under force_partition")
	}
	if _, produce, acks, parts := rt.snapshot(); produce != 1 || acks[0] != int16(RequireAll) || parts[0] != 2 {
		t.Fatalf("combined frame wrong produce: produce=%d acks=%v parts=%v", produce, acks, parts)
	}
}

// reject_write: fails without entering the queue or creating partition writers.
func TestWriterPolicyRejectWrite(t *testing.T) {
	rt := &fakePolicyTransport{}
	w := newPolicyTestWriter(rt, false, nil)
	defer w.Close()

	frame := PushWriterPolicy(w, WriterPolicy{BatchGate: WriteBatchRejectWrite})

	err := w.WriteMessages(context.Background(), policyMsg([]byte("k")))
	if !errors.Is(err, ErrWriterPolicyRejectWrite) {
		t.Fatalf("expected ErrWriterPolicyRejectWrite, got %v", err)
	}

	w.mutex.Lock()
	partitionWriters := len(w.writers)
	w.mutex.Unlock()

	if partitionWriters != 0 {
		t.Fatalf("rejected write must not create partition writers, got %d", partitionWriters)
	}
	if metadata, produce, _, _ := rt.snapshot(); metadata != 0 || produce != 0 {
		t.Fatalf("rejected write must send nothing, got metadata=%d produce=%d", metadata, produce)
	}

	if err := frame.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if err := w.WriteMessages(context.Background(), policyMsg([]byte("k"))); err != nil {
		t.Fatalf("write after pop: %v", err)
	}
	if _, produce, _, _ := rt.snapshot(); produce != 1 {
		t.Fatalf("expected one produce after pop, got %d", produce)
	}
}

// force_flush on an async writer: pending batches are flushed and awaited
// before return, without touching BatchTimeout/BatchSize.
func TestWriterPolicyForceFlushAsync(t *testing.T) {
	rt := &fakePolicyTransport{
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	w := newPolicyTestWriter(rt, true, nil)
	// Large, slow batch: a pinned async write stays pending until the frame
	// forces a flush.
	w.BatchSize = 1000
	w.BatchTimeout = time.Hour
	defer w.Close()

	// Pinned async write enqueues but does not flush (huge batch, 1h timeout).
	if err := w.WriteMessages(context.Background(), policyMsg([]byte("a"))); err != nil {
		t.Fatalf("async pin write: %v", err)
	}
	select {
	case <-rt.entered:
		t.Fatal("pinned async write must not flush before the batch timeout")
	case <-time.After(150 * time.Millisecond):
	}

	frame := PushWriterPolicy(w, WriterPolicy{BatchGate: WriteBatchForceFlush})

	done := make(chan error, 1)
	go func() { done <- w.WriteMessages(context.Background(), policyMsg([]byte("b"))) }()

	waitEntered(t, rt.entered)
	assertWriteBlocked(t, done)

	if w.BatchTimeout != time.Hour || w.BatchSize != 1000 {
		t.Fatal("force_flush must not mutate BatchTimeout/BatchSize")
	}

	close(rt.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("force_flush WriteMessages: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("force_flush WriteMessages did not return after flushing")
	}

	// Exactly one produce carries both messages: the pending batch was
	// flushed, not a duplicated/second queue entry.
	if _, produce, _, _ := rt.snapshot(); produce != 1 {
		t.Fatalf("expected exactly one flushed produce, got %d", produce)
	}

	if err := frame.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}
}

// Two writers on the SAME goroutine keep independent stack tops.
func TestWriterPolicyTwoInstancesSameGoroutine(t *testing.T) {
	rt1 := &fakePolicyTransport{}
	rt2 := &fakePolicyTransport{}
	w1 := newPolicyTestWriter(rt1, false, nil)
	w2 := newPolicyTestWriter(rt2, false, nil)
	defer w1.Close()
	defer w2.Close()

	w1.RequiredAcks = RequireOne
	w2.RequiredAcks = RequireOne

	frame := PushWriterPolicy(w1, WriterPolicy{AckSurface: ProduceAckForceAll})
	defer frame.Pop()

	if w1.EffectiveWritePolicy().Depth != 1 || w2.EffectiveWritePolicy().Depth != 0 {
		t.Fatalf("stacks leaked across writers: w1=%d w2=%d", w1.EffectiveWritePolicy().Depth, w2.EffectiveWritePolicy().Depth)
	}

	if err := w1.WriteMessages(context.Background(), policyMsg([]byte("k"))); err != nil {
		t.Fatalf("w1 write: %v", err)
	}
	if err := w2.WriteMessages(context.Background(), policyMsg([]byte("k"))); err != nil {
		t.Fatalf("w2 write: %v", err)
	}

	if _, _, acks1, _ := rt1.snapshot(); len(acks1) != 1 || acks1[0] != int16(RequireAll) {
		t.Fatalf("w1 must produce with acks=-1, got %v", acks1)
	}
	if _, _, acks2, _ := rt2.snapshot(); len(acks2) != 1 || acks2[0] != int16(RequireOne) {
		t.Fatalf("w2 must keep its own flat acks=1, got %v", acks2)
	}
}

// Concurrent writers with different frames never cross stacks.
func TestWriterPolicyConcurrentInstances(t *testing.T) {
	rt1 := &fakePolicyTransport{}
	rt2 := &fakePolicyTransport{}
	rec := &completionRecorder{}
	w1 := newPolicyTestWriter(rt1, false, nil)
	w2 := newPolicyTestWriter(rt2, false, rec)
	defer w1.Close()
	defer w2.Close()

	f1 := PushWriterPolicy(w1, WriterPolicy{AckSurface: ProduceAckForceAll})
	f2 := PushWriterPolicy(w2, WriterPolicy{AckSurface: ProduceAckSuppressCompletion})

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := w1.WriteMessages(context.Background(), policyMsg([]byte("k"))); err != nil {
				t.Errorf("w1 write: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := w2.WriteMessages(context.Background(), policyMsg([]byte("k"))); err != nil {
				t.Errorf("w2 write: %v", err)
			}
		}()
	}
	wg.Wait()

	_, n1, acks1, _ := rt1.snapshot()
	_, n2, acks2, _ := rt2.snapshot()
	if n1 != 16 || n2 != 16 {
		t.Fatalf("expected 16 produces each, got %d/%d", n1, n2)
	}
	for _, a := range acks1 {
		if a != int16(RequireAll) {
			t.Fatalf("w1 produce saw foreign ack level %d", a)
		}
	}
	for _, a := range acks2 {
		if a != int16(RequireNone) {
			t.Fatalf("w2 must keep its flat ack level, got %d", a)
		}
	}
	if ok, _ := rec.counts(); ok != 0 {
		t.Fatalf("w2 suppress_completion leaked to %d success callbacks", ok)
	}

	if err := f1.Pop(); err != nil {
		t.Fatalf("Pop w1: %v", err)
	}
	if err := f2.Pop(); err != nil {
		t.Fatalf("Pop w2: %v", err)
	}
}

// Nested frames: inner pop restores the outer; double-pop is reported.
func TestWriterPolicyNestedFrames(t *testing.T) {
	rt := &fakePolicyTransport{}
	rec := &completionRecorder{}
	w := newPolicyTestWriter(rt, false, rec)
	defer w.Close()

	outer := PushWriterPolicy(w, WriterPolicy{AckSurface: ProduceAckForceAll})
	if eff := w.EffectiveWritePolicy(); eff.AckSurface != ProduceAckForceAll || eff.Depth != 1 || !eff.InvokeCompletion {
		t.Fatalf("outer frame effective view wrong: %+v", eff)
	}

	// The inner frame replaces the top wholesale: its suppress_completion
	// applies, its pinned ack surface falls back to the flat field (frames do
	// not merge dimension-wise); popping it restores the outer force_all.
	inner := PushWriterPolicy(w, WriterPolicy{AckSurface: ProduceAckSuppressCompletion})
	if eff := w.EffectiveWritePolicy(); eff.AckSurface != ProduceAckSuppressCompletion || eff.Depth != 2 ||
		eff.RequiredAcks != RequireNone || !eff.WaitForDelivery || eff.InvokeCompletion ||
		eff.KeyPartition != KeyPartitionPin || eff.BatchGate != WriteBatchPin {
		t.Fatalf("inner frame effective view wrong: %+v", eff)
	}

	if err := inner.Pop(); err != nil {
		t.Fatalf("Pop inner: %v", err)
	}
	if eff := w.EffectiveWritePolicy(); eff.AckSurface != ProduceAckForceAll || eff.InvokeCompletion != true {
		t.Fatalf("outer frame not restored: %+v", eff)
	}

	if err := outer.Pop(); err != nil {
		t.Fatalf("Pop outer: %v", err)
	}
	if eff := w.EffectiveWritePolicy(); eff.Depth != 0 || eff.AckSurface != ProduceAckPin {
		t.Fatalf("stack not empty: %+v", eff)
	}
	if err := outer.Pop(); !errors.Is(err, ErrWriterPolicyFramePopped) {
		t.Fatalf("double pop must return ErrWriterPolicyFramePopped, got %v", err)
	}
}

// A frame pushed after the call entered ack waiting is void for the in-flight
// call; subsequent calls observe it.
func TestWriterPolicyLatePushVoidForInFlight(t *testing.T) {
	rt := &fakePolicyTransport{
		entered: make(chan struct{}, 8),
		release: make(chan struct{}),
	}
	w := newPolicyTestWriter(rt, false, nil)
	defer w.Close()

	done := make(chan error, 1)
	go func() { done <- w.WriteMessages(context.Background(), policyMsg([]byte("k"))) }()

	waitEntered(t, rt.entered)

	// Late push while the pinned call is waiting on its acks=0 produce.
	late := PushWriterPolicy(w, WriterPolicy{AckSurface: ProduceAckForceAll})

	close(rt.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("in-flight write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight write never returned")
	}

	_, produce, acks, _ := rt.snapshot()
	if produce != 1 || acks[0] != int16(RequireNone) {
		t.Fatalf("late frame must not recompute the in-flight produce, got acks=%v", acks)
	}

	// Subsequent call observes the late frame.
	if err := w.WriteMessages(context.Background(), policyMsg([]byte("k2"))); err != nil {
		t.Fatalf("second write: %v", err)
	}
	_, produce, acks, _ = rt.snapshot()
	if produce != 2 || acks[1] != int16(RequireAll) {
		t.Fatalf("subsequent produce must use force_all, got acks=%v", acks)
	}

	if err := late.Pop(); err != nil {
		t.Fatalf("Pop: %v", err)
	}
}

// WithWriterPolicy always pops, including panic paths.
func TestWithWriterPolicyAutoPop(t *testing.T) {
	rt := &fakePolicyTransport{}
	w := newPolicyTestWriter(rt, false, nil)
	defer w.Close()

	func() {
		defer func() { _ = recover() }()
		_ = WithWriterPolicy(w, WriterPolicy{BatchGate: WriteBatchRejectWrite}, func() error {
			panic("boom")
		})
	}()

	if w.EffectiveWritePolicy().Depth != 0 {
		t.Fatal("panic path must leave the policy stack empty")
	}
	if err := w.WriteMessages(context.Background(), policyMsg([]byte("k"))); err != nil {
		t.Fatalf("write after auto-pop: %v", err)
	}
	if _, produce, _, _ := rt.snapshot(); produce != 1 {
		t.Fatalf("expected one produce, got %d", produce)
	}
}

// Invalid policy values panic on push rather than being silently pinned.
func TestWriterPolicyPushInvalidPanics(t *testing.T) {
	rt := &fakePolicyTransport{}
	w := newPolicyTestWriter(rt, false, nil)
	defer w.Close()

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic on invalid enum")
			}
		}()
		PushWriterPolicy(w, WriterPolicy{AckSurface: ProduceAckSurface(42)})
	}()

	if w.EffectiveWritePolicy().Depth != 0 {
		t.Fatal("rejected push must not leave a frame on the stack")
	}
}
