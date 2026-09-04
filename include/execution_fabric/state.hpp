#pragma once
#include <cstdint>

namespace execution_fabric {

// ---------------------------------------------------------------------------
// Lifecycle states of a logical execution. A logical execution's state is the
// authority-level answer to "what may happen to this execution now". Physical
// attempts each carry their own independent lifecycle (see AttemptState).
//
// Precise mapping of the requested vocabulary:
//   CREATED            -> CREATED
//   READY              -> READY
//   DISPATCHED         -> DISPATCHED
//   RUNNING            -> RUNNING
//   CANCELLATION_REQ.  -> CANCELLATION_REQUESTED
//   PREEMPTION_REQ.    -> PREEMPTION_REQUESTED
//   PREEMPTED          -> PREEMPTED
//   RESUMABLE          -> RESUMABLE
//   RESUMING           -> RESUMING
//   COMPLETION_PENDING -> COMPLETION_PENDING
//   COMPLETED          -> COMPLETED
//   COMMITTED          -> COMMITTED
//   FAILED             -> FAILED
//   CANCELLED          -> CANCELLED
//   AMBIGUOUS          -> AMBIGUOUS
//   SUPERSEDED         -> SUPERSEDED
//   TERMINAL           -> TERMINAL
// ---------------------------------------------------------------------------
enum class ExecutionState : std::uint8_t {
    CREATED = 0,
    READY = 1,
    DISPATCHED = 2,
    RUNNING = 3,
    CANCELLATION_REQUESTED = 4,
    PREEMPTION_REQUESTED = 5,
    PREEMPTED = 6,
    RESUMABLE = 7,
    RESUMING = 8,
    COMPLETION_PENDING = 9,
    COMPLETED = 10,
    COMMITTED = 11,
    FAILED = 12,
    CANCELLED = 13,
    AMBIGUOUS = 14,
    SUPERSEDED = 15,
    TERMINAL = 16,
};

const char* to_string(ExecutionState s) noexcept;

// Attempt lifecycle: independent of the logical execution. A logical execution
// may have many physical attempts over its lifetime; each attempt moves
// through its own lifecycle.
enum class AttemptState : std::uint8_t {
    CREATED = 0,
    DISPATCHED = 1,
    RUNNING = 2,
    PREEMPTED = 3,
    COMPLETE_PENDING = 4,
    COMPLETED = 5,
    FAILED = 6,
    CANCELLED = 7,
    LOST = 8,
    SUPERSEDED = 9,
    TERMINAL = 10,
};

const char* to_string(AttemptState s) noexcept;

}  // namespace execution_fabric