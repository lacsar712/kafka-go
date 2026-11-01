package kafka

import (
	"errors"
	"sync"
)

// This file implements the Reader-scoped Kafka runtime policy stack.
//
// The stack is an instance field of Reader (never a package-level singleton,
// never goroutine-local storage): two distinct *Reader values, even when used
// from the same goroutine, each own an independent stack top. Reader stacks
// and Writer stacks are separate types and APIs; a Reader frame can never be
// observed by a Writer outlet and vice versa.
//
// FetchMessage/ReadMessage snapshot the stack top exactly once on entry. The
// snapshot is immutable for the whole call: pushing a frame after FetchMessage
// already returned a message is void for that fetch (it applies to subsequent
// fetches instead), and popping a frame mid-flight does not revoke the
// captured snapshot. CommitMessages snapshots its top before any commit is
// enqueued, so a frame pushed once the commit write-out has started cannot
// retroactively reject it.

// OffsetCommitAdmit controls where a Reader frame starts consuming and whether
// its commits are admitted.
type OffsetCommitAdmit int

const (
	// OffsetCommitPin keeps the Reader's flat configuration: starting
	// position follows ReaderConfig.StartOffset / the reader position, and
	// commits proceed as configured.
	OffsetCommitPin OffsetCommitAdmit = iota

	// OffsetCommitForceFirst starts new consumption of the frame at the
	// partition's earliest offset (FirstOffset). This neither mutates
	// ReaderConfig.StartOffset nor goes through SetOffset/SetOffsetAt.
	//
	// The enforced path is the standalone Reader (GroupID == ""), where the
	// starting offset is owned by the client; with a consumer group the
	// coordinator-assigned offset governs and the frame cannot seek below it.
	OffsetCommitForceFirst

	// OffsetCommitRejectCommit fails CommitMessages of the frame with
	// ErrReaderPolicyCommitRejected; no commit is enqueued and the committed
	// offset does not advance.
	OffsetCommitRejectCommit
)

// String returns a human readable representation of the offset/commit admit.
func (a OffsetCommitAdmit) String() string {
	switch a {
	case OffsetCommitPin:
		return "pin"
	case OffsetCommitForceFirst:
		return "force_first"
	case OffsetCommitRejectCommit:
		return "reject_commit"
	default:
		return "unknown"
	}
}

func (a OffsetCommitAdmit) valid() bool {
	return a >= OffsetCommitPin && a <= OffsetCommitRejectCommit
}

// ReaderPolicy is one frame of the Reader runtime policy stack.
//
// The zero value is equivalent to an empty stack: the Reader's flat fields
// are pinned, so deployments that never push a frame keep the exact
// historical behavior.
type ReaderPolicy struct {
	// OffsetCommit controls the consume-start and commit admission.
	OffsetCommit OffsetCommitAdmit
}

func (p ReaderPolicy) validate() {
	if !p.OffsetCommit.valid() {
		panic("kafka: invalid ReaderPolicy pushed on a Reader policy stack")
	}
}

func (p ReaderPolicy) forceFirst() bool { return p.OffsetCommit == OffsetCommitForceFirst }

func (p ReaderPolicy) rejectCommit() bool { return p.OffsetCommit == OffsetCommitRejectCommit }

// ReaderPolicyFrame is a handle on a frame pushed on a specific *Reader
// instance. Frames are immutable after creation and safe to retain for the
// whole duration of a call.
type ReaderPolicyFrame struct {
	id     uint64
	owner  *Reader
	policy ReaderPolicy
}

// Policy returns a copy of the frame's policy.
func (f *ReaderPolicyFrame) Policy() ReaderPolicy { return f.policy }

// Pop removes this frame from its owning Reader's stack. Popping an inner
// frame restores the outer frame; the stack never falls back to any
// process-wide value. Removing an already removed frame returns
// ErrReaderPolicyFramePopped.
func (f *ReaderPolicyFrame) Pop() error {
	return f.owner.policy.remove(f)
}

// readerPolicyStack is the per-Reader policy stack. Its zero value is an empty
// stack, which is exactly equivalent to the historical Reader behavior.
type readerPolicyStack struct {
	mutex  sync.Mutex
	seq    uint64
	frames []*ReaderPolicyFrame
}

func (s *readerPolicyStack) push(r *Reader, p ReaderPolicy) *ReaderPolicyFrame {
	p.validate()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.seq++
	f := &ReaderPolicyFrame{
		id:     s.seq,
		owner:  r,
		policy: p,
	}
	s.frames = append(s.frames, f)
	return f
}

// current returns the immutable top frame, or nil when the stack is empty.
func (s *readerPolicyStack) current() *ReaderPolicyFrame {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if n := len(s.frames); n > 0 {
		return s.frames[n-1]
	}
	return nil
}

func (s *readerPolicyStack) depth() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.frames)
}

func (s *readerPolicyStack) remove(frame *ReaderPolicyFrame) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for i, f := range s.frames {
		if f == frame {
			s.frames = append(s.frames[:i], s.frames[i+1:]...)
			return nil
		}
	}
	return ErrReaderPolicyFramePopped
}

// PushReaderPolicy pushes a frame on the given Reader's own stack and returns
// a handle used to pop the frame. The frame only affects this *Reader: another
// Reader (even on the same goroutine) never observes it.
//
// Push panics if p contains an out-of-range enum value.
func PushReaderPolicy(r *Reader, p ReaderPolicy) *ReaderPolicyFrame {
	return r.policy.push(r, p)
}

// WithReaderPolicy pushes p on r's stack, runs do, and always pops the frame
// when do returns or panics. A panic propagates after the stack has been
// cleaned, so error paths cannot leak frames onto the stack.
func WithReaderPolicy(r *Reader, p ReaderPolicy, do func() error) (err error) {
	frame := PushReaderPolicy(r, p)
	defer func() { _ = frame.Pop() }()
	return do()
}

// EffectiveReaderPolicy is the read-only merged view of the Reader's flat
// configuration with its current stack top. It is returned by
// (*Reader).EffectiveFetchPolicy and intentionally has no Writer counterpart:
// Reader and Writer effective views are produced by separate outlets.
type EffectiveReaderPolicy struct {
	// FrameID identifies the top frame, or 0 when the stack is empty.
	FrameID uint64

	// Depth reports how many frames are currently pushed on this Reader.
	Depth int

	// OffsetCommit is the raw top-frame value (OffsetCommitPin when empty).
	OffsetCommit OffsetCommitAdmit

	// StartOffset is the effective starting offset: FirstOffset under
	// force_first; otherwise ReaderConfig.StartOffset for group readers and
	// the reader's current position for standalone readers.
	StartOffset int64

	// CommitMessagesEnabled reports whether CommitMessages is admitted:
	// false under reject_commit, true otherwise.
	CommitMessagesEnabled bool
}

// EffectiveFetchPolicy returns the merged view of r's flat Reader fields and
// its current stack top. An empty stack is reported as fully pinned values.
func (r *Reader) EffectiveFetchPolicy() EffectiveReaderPolicy {
	frame := r.policy.current()

	eff := EffectiveReaderPolicy{
		Depth:                 r.policy.depth(),
		OffsetCommit:          OffsetCommitPin,
		CommitMessagesEnabled: true,
	}

	r.mutex.Lock()
	if r.useConsumerGroup() {
		eff.StartOffset = r.config.StartOffset
	} else {
		eff.StartOffset = r.offset
	}
	r.mutex.Unlock()

	if frame != nil {
		eff.FrameID = frame.id
		eff.OffsetCommit = frame.policy.OffsetCommit
		if frame.policy.forceFirst() {
			eff.StartOffset = FirstOffset
		}
		if frame.policy.rejectCommit() {
			eff.CommitMessagesEnabled = false
		}
	}

	return eff
}

// Errors returned by policy-gated Reader outlets.
var (
	// ErrReaderPolicyCommitRejected is returned by CommitMessages while a
	// reject_commit frame is active; the commit is not enqueued and the
	// committed offset does not advance.
	ErrReaderPolicyCommitRejected = errors.New("kafka.(*Reader).CommitMessages: rejected by offset_commit_admit policy frame")

	// ErrReaderPolicyFramePopped is returned when popping a frame that has
	// already been removed from its Reader's stack.
	ErrReaderPolicyFramePopped = errors.New("kafka.(*Reader): policy frame has already been popped")
)

// policyStartOffsets returns the offsets the child readers of this frame must
// be started at. force_first seeks to the partition earliest without touching
// ReaderConfig.StartOffset or the reader position; the pinned path keeps the
// reader's current position.
func (r *Reader) policyStartOffsets(frame *ReaderPolicyFrame) map[topicPartition]int64 {
	if frame != nil && frame.policy.forceFirst() {
		return map[topicPartition]int64{
			{topic: r.config.Topic, partition: int32(r.config.Partition)}: FirstOffset,
		}
	}
	return r.getTopicPartitionOffset()
}
