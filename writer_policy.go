package kafka

import (
	"errors"
	"sync"
)

// This file implements the Writer-scoped Kafka runtime policy stack.
//
// The stack is an instance field of Writer (never a package-level singleton,
// never goroutine-local storage): two distinct *Writer values, even when used
// from the same goroutine, each own an independent stack top. Writer stacks
// and Reader stacks are separate types and APIs; a Writer frame can never be
// observed by a Reader outlet and vice versa.
//
// WriteMessages snapshots the stack top exactly once on entry. The snapshot is
// immutable for the whole call: pushing a frame after the call already queued
// messages or started waiting for acks is void for that in-flight call (it
// applies to subsequent calls instead), and popping a frame mid-flight does
// not revoke the captured snapshot. Background batch workers read policy
// attributes captured on the writeBatch, they never consult the live stack.

// ProduceAckSurface controls how a WriteMessages frame waits for, and reports,
// producer acknowledgements.
type ProduceAckSurface int

const (
	// ProduceAckPin keeps the Writer's flat configuration: the call waits (or
	// not) according to Writer.RequiredAcks / Writer.Async and invokes
	// Writer.Completion exactly as configured.
	ProduceAckPin ProduceAckSurface = iota

	// ProduceAckForceAll requires the frame's produce requests to be sent with
	// RequireAll and the call to observably wait for the RequireAll-level
	// acknowledgement, even when Writer.Async is true. A successful
	// Completion cannot fire before the full acknowledgement was received.
	ProduceAckForceAll

	// ProduceAckSuppressCompletion keeps delivery and acknowledgement
	// semantics untouched but forbids invoking Writer.Completion for
	// successful writes made by the frame. Completion is still invoked for
	// failed writes so errors are never swallowed silently.
	ProduceAckSuppressCompletion
)

// String returns a human readable representation of the ack surface.
func (s ProduceAckSurface) String() string {
	switch s {
	case ProduceAckPin:
		return "pin"
	case ProduceAckForceAll:
		return "force_all"
	case ProduceAckSuppressCompletion:
		return "suppress_completion"
	default:
		return "unknown"
	}
}

func (s ProduceAckSurface) valid() bool {
	return s >= ProduceAckPin && s <= ProduceAckSuppressCompletion
}

// KeyPartitionBind controls how a WriteMessages frame binds messages to
// partitions.
type KeyPartitionBind int

const (
	// KeyPartitionPin keeps using the configured Writer.Balancer unchanged.
	KeyPartitionPin KeyPartitionBind = iota

	// KeyPartitionRequireKey fails the frame (without sending any Produce
	// request) when one of the messages carries no Key. Messages must never
	// fall through to the balancer's default partition silently.
	KeyPartitionRequireKey

	// KeyPartitionForcePartition routes each message to the explicit
	// Message.Partition value; the Balancer is not consulted and cannot
	// override the selection.
	KeyPartitionForcePartition
)

// String returns a human readable representation of the key/partition bind.
func (b KeyPartitionBind) String() string {
	switch b {
	case KeyPartitionPin:
		return "pin"
	case KeyPartitionRequireKey:
		return "require_key"
	case KeyPartitionForcePartition:
		return "force_partition"
	default:
		return "unknown"
	}
}

func (b KeyPartitionBind) valid() bool {
	return b >= KeyPartitionPin && b <= KeyPartitionForcePartition
}

// WriteBatchGate controls how a WriteMessages frame interacts with the
// internal batching queue.
type WriteBatchGate int

const (
	// WriteBatchPin keeps the configured BatchSize/BatchTimeout flush
	// behavior.
	WriteBatchPin WriteBatchGate = iota

	// WriteBatchRejectWrite fails the frame before any message enters the
	// internal write queue; no Produce request is sent.
	WriteBatchRejectWrite

	// WriteBatchForceFlush forces pending batches of the touched partitions
	// to be flushed, and observably waits for the frame's batches to complete
	// before WriteMessages returns, regardless of Writer.Async. This does
	// not mutate BatchSize/BatchTimeout.
	WriteBatchForceFlush
)

// String returns a human readable representation of the batch gate.
func (g WriteBatchGate) String() string {
	switch g {
	case WriteBatchPin:
		return "pin"
	case WriteBatchRejectWrite:
		return "reject_write"
	case WriteBatchForceFlush:
		return "force_flush"
	default:
		return "unknown"
	}
}

func (g WriteBatchGate) valid() bool {
	return g >= WriteBatchPin && g <= WriteBatchForceFlush
}

// WriterPolicy is one frame of the Writer runtime policy stack.
//
// The three dimensions are independent enums and are read independently by
// WriteMessages; they are never collapsed into a single boolean.
//
// The zero value is equivalent to an empty stack: every dimension pins the
// Writer's flat fields, so deployments that never push a frame keep the exact
// historical behavior.
type WriterPolicy struct {
	// AckSurface overrides the produce acknowledgement surface.
	AckSurface ProduceAckSurface

	// KeyPartition constrains key presence / explicit partition routing.
	KeyPartition KeyPartitionBind

	// BatchGate gates queue admission and batch flushing.
	BatchGate WriteBatchGate
}

func (p WriterPolicy) validate() {
	if !p.AckSurface.valid() || !p.KeyPartition.valid() || !p.BatchGate.valid() {
		panic("kafka: invalid WriterPolicy pushed on a Writer policy stack")
	}
}

func (p WriterPolicy) forceAllAcks() bool { return p.AckSurface == ProduceAckForceAll }

func (p WriterPolicy) suppressCompletion() bool {
	return p.AckSurface == ProduceAckSuppressCompletion
}

func (p WriterPolicy) requireKey() bool { return p.KeyPartition == KeyPartitionRequireKey }

func (p WriterPolicy) forcePartition() bool { return p.KeyPartition == KeyPartitionForcePartition }

func (p WriterPolicy) rejectWrite() bool { return p.BatchGate == WriteBatchRejectWrite }

func (p WriterPolicy) forceFlush() bool { return p.BatchGate == WriteBatchForceFlush }

// WriterPolicyFrame is a handle on a frame pushed on a specific *Writer
// instance. Frames are immutable after creation and safe to retain for the
// whole duration of a call.
type WriterPolicyFrame struct {
	id     uint64
	owner  *Writer
	policy WriterPolicy
}

// Policy returns a copy of the frame's policy.
func (f *WriterPolicyFrame) Policy() WriterPolicy { return f.policy }

// Pop removes this frame from its owning Writer's stack. Popping an inner
// frame restores the outer frame; the stack never falls back to any
// process-wide value. Removing an already removed frame returns
// ErrWriterPolicyFramePopped.
func (f *WriterPolicyFrame) Pop() error {
	return f.owner.policy.remove(f)
}

// writerPolicyStack is the per-Writer policy stack. Its zero value is an empty
// stack, which is exactly equivalent to the historical Writer behavior.
type writerPolicyStack struct {
	mutex  sync.Mutex
	seq    uint64
	frames []*WriterPolicyFrame
}

func (s *writerPolicyStack) push(w *Writer, p WriterPolicy) *WriterPolicyFrame {
	p.validate()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.seq++
	f := &WriterPolicyFrame{
		id:     s.seq,
		owner:  w,
		policy: p,
	}
	s.frames = append(s.frames, f)
	return f
}

// current returns the immutable top frame, or nil when the stack is empty.
func (s *writerPolicyStack) current() *WriterPolicyFrame {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if n := len(s.frames); n > 0 {
		return s.frames[n-1]
	}
	return nil
}

func (s *writerPolicyStack) depth() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.frames)
}

func (s *writerPolicyStack) remove(frame *WriterPolicyFrame) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for i, f := range s.frames {
		if f == frame {
			s.frames = append(s.frames[:i], s.frames[i+1:]...)
			return nil
		}
	}
	return ErrWriterPolicyFramePopped
}

// PushWriterPolicy pushes a frame on the given Writer's own stack and returns
// a handle used to pop the frame. The frame only affects this *Writer: another
// Writer (even on the same goroutine) never observes it.
//
// Push panics if p contains out-of-range enum values.
func PushWriterPolicy(w *Writer, p WriterPolicy) *WriterPolicyFrame {
	return w.policy.push(w, p)
}

// WithWriterPolicy pushes p on w's stack, runs do, and always pops the frame
// when do returns or panics. A panic propagates after the stack has been
// cleaned, so error paths cannot leak frames onto the stack.
func WithWriterPolicy(w *Writer, p WriterPolicy, do func() error) (err error) {
	frame := PushWriterPolicy(w, p)
	defer func() { _ = frame.Pop() }()
	return do()
}

// EffectiveWriterPolicy is the read-only merged view of the Writer's flat
// configuration with its current stack top. It is returned by
// (*Writer).EffectiveWritePolicy and intentionally has no Reader counterpart:
// Writer and Reader effective views are produced by separate outlets.
type EffectiveWriterPolicy struct {
	// FrameID identifies the top frame, or 0 when the stack is empty.
	FrameID uint64

	// Depth reports how many frames are currently pushed on this Writer.
	Depth int

	// AckSurface, KeyPartition and BatchGate are the raw top-frame values
	// (each defaults to its pin constant when the stack is empty).
	AckSurface   ProduceAckSurface
	KeyPartition KeyPartitionBind
	BatchGate    WriteBatchGate

	// RequiredAcks is the effective producer ack level: RequireAll under
	// force_all, otherwise Writer.RequiredAcks.
	RequiredAcks RequiredAcks

	// WaitForDelivery reports whether WriteMessages observably waits for
	// delivery: false for plain Async writers, true under force_all or
	// force_flush.
	WaitForDelivery bool

	// InvokeCompletion reports whether successful Completion callbacks are
	// invoked: false under suppress_completion, otherwise Writer.Completion
	// != nil.
	InvokeCompletion bool
}

// EffectiveWritePolicy returns the merged view of w's flat Writer fields and
// its current stack top. An empty stack is reported as fully pinned values.
func (w *Writer) EffectiveWritePolicy() EffectiveWriterPolicy {
	frame := w.policy.current()

	eff := EffectiveWriterPolicy{
		Depth:            w.policy.depth(),
		AckSurface:       ProduceAckPin,
		KeyPartition:     KeyPartitionPin,
		BatchGate:        WriteBatchPin,
		RequiredAcks:     w.RequiredAcks,
		WaitForDelivery:  !w.Async,
		InvokeCompletion: w.Completion != nil,
	}

	if frame != nil {
		eff.FrameID = frame.id
		eff.AckSurface = frame.policy.AckSurface
		eff.KeyPartition = frame.policy.KeyPartition
		eff.BatchGate = frame.policy.BatchGate

		if frame.policy.forceAllAcks() {
			eff.RequiredAcks = RequireAll
			eff.WaitForDelivery = true
		}
		if frame.policy.forceFlush() {
			eff.WaitForDelivery = true
		}
		if frame.policy.suppressCompletion() {
			eff.InvokeCompletion = false
		}
	}

	return eff
}

// Errors returned by policy-gated Writer outlets.
var (
	// ErrWriterPolicyRejectWrite is returned by WriteMessages while a
	// reject_write frame is active; no message entered the internal write
	// queue and no Produce request was sent.
	ErrWriterPolicyRejectWrite = errors.New("kafka.(*Writer).WriteMessages: rejected by write_batch_gate policy frame")

	// ErrWriterPolicyKeyRequired is returned by WriteMessages while a
	// require_key frame is active and one of the messages has no Key; no
	// Produce request is sent.
	ErrWriterPolicyKeyRequired = errors.New("kafka.(*Writer).WriteMessages: message key required by key_partition_bind policy frame")

	// ErrWriterPolicyFramePopped is returned when popping a frame that has
	// already been removed from its Writer's stack.
	ErrWriterPolicyFramePopped = errors.New("kafka.(*Writer): policy frame has already been popped")
)
