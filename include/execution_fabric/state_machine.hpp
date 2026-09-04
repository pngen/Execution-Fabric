#pragma once
#include "execution_fabric/state.hpp"

namespace execution_fabric {

// ---------------------------------------------------------------------------
// ExecutionStateMachine
//
// A guarded lifecycle controller for a logical execution. Every state change
// must be validated against an explicit transition table; invalid transitions
// are rejected. Terminal states are absorbing: no further transition out of a
// terminal state is ever allowed, so a committed logical outcome can never be
// silently reopened.
//
// Self-transitions (e.g. DISPATCHED -> DISPATCHED) are used when the logical
// execution's authority is re-issued to a new attempt without the *logical*
// state changing: re-dispatch supersedes the prior attempt but the logical
// execution remains "in dispatch".
// ---------------------------------------------------------------------------
class ExecutionStateMachine {
public:
    static constexpr bool from(const ExecutionState from, const ExecutionState to) noexcept {
        switch (from) {
        case ExecutionState::CREATED:
            return to == ExecutionState::READY || to == ExecutionState::CANCELLED ||
                   to == ExecutionState::FAILED || to == ExecutionState::TERMINAL;
        case ExecutionState::READY:
            return to == ExecutionState::DISPATCHED || to == ExecutionState::CANCELLATION_REQUESTED ||
                   to == ExecutionState::CANCELLED || to == ExecutionState::FAILED ||
                   to == ExecutionState::SUPERSEDED || to == ExecutionState::TERMINAL;
        case ExecutionState::DISPATCHED:
            return to == ExecutionState::DISPATCHED || to == ExecutionState::READY ||
                   to == ExecutionState::RUNNING || to == ExecutionState::COMPLETED ||
                   to == ExecutionState::COMPLETION_PENDING || to == ExecutionState::CANCELLATION_REQUESTED ||
                   to == ExecutionState::PREEMPTION_REQUESTED || to == ExecutionState::CANCELLED ||
                   to == ExecutionState::FAILED || to == ExecutionState::AMBIGUOUS ||
                   to == ExecutionState::SUPERSEDED || to == ExecutionState::TERMINAL;
        case ExecutionState::RUNNING:
            return to == ExecutionState::DISPATCHED || to == ExecutionState::COMPLETED ||
                   to == ExecutionState::COMPLETION_PENDING || to == ExecutionState::CANCELLATION_REQUESTED ||
                   to == ExecutionState::PREEMPTION_REQUESTED || to == ExecutionState::PREEMPTED ||
                   to == ExecutionState::FAILED || to == ExecutionState::CANCELLED ||
                   to == ExecutionState::AMBIGUOUS || to == ExecutionState::SUPERSEDED ||
                   to == ExecutionState::TERMINAL;
        case ExecutionState::CANCELLATION_REQUESTED:
            return to == ExecutionState::CANCELLED || to == ExecutionState::COMPLETED ||
                   to == ExecutionState::COMPLETION_PENDING || to == ExecutionState::FAILED ||
                   to == ExecutionState::AMBIGUOUS || to == ExecutionState::TERMINAL;
        case ExecutionState::PREEMPTION_REQUESTED:
            return to == ExecutionState::PREEMPTED || to == ExecutionState::COMPLETED ||
                   to == ExecutionState::FAILED || to == ExecutionState::CANCELLED ||
                   to == ExecutionState::AMBIGUOUS || to == ExecutionState::TERMINAL;
        case ExecutionState::PREEMPTED:
            return to == ExecutionState::RESUMABLE || to == ExecutionState::FAILED ||
                   to == ExecutionState::CANCELLED || to == ExecutionState::TERMINAL;
        case ExecutionState::RESUMABLE:
            return to == ExecutionState::RESUMING || to == ExecutionState::CANCELLED ||
                   to == ExecutionState::FAILED || to == ExecutionState::TERMINAL;
        case ExecutionState::RESUMING:
            return to == ExecutionState::RUNNING || to == ExecutionState::FAILED ||
                   to == ExecutionState::CANCELLED || to == ExecutionState::AMBIGUOUS ||
                   to == ExecutionState::TERMINAL;
        case ExecutionState::COMPLETION_PENDING:
            return to == ExecutionState::COMPLETED || to == ExecutionState::FAILED ||
                   to == ExecutionState::AMBIGUOUS || to == ExecutionState::TERMINAL;
        case ExecutionState::COMPLETED:
            return to == ExecutionState::COMMITTED || to == ExecutionState::FAILED ||
                   to == ExecutionState::AMBIGUOUS || to == ExecutionState::TERMINAL;
        case ExecutionState::AMBIGUOUS:
            return to == ExecutionState::COMPLETED || to == ExecutionState::DISPATCHED ||
                   to == ExecutionState::FAILED || to == ExecutionState::CANCELLED ||
                   to == ExecutionState::SUPERSEDED || to == ExecutionState::TERMINAL;
        case ExecutionState::COMMITTED:
        case ExecutionState::FAILED:
        case ExecutionState::CANCELLED:
        case ExecutionState::SUPERSEDED:
        case ExecutionState::TERMINAL:
            return false;
        }
        return false;
    }

    static constexpr bool is_terminal(const ExecutionState s) noexcept {
        return s == ExecutionState::COMMITTED || s == ExecutionState::FAILED ||
               s == ExecutionState::CANCELLED || s == ExecutionState::SUPERSEDED ||
               s == ExecutionState::TERMINAL;
    }
};

// ---------------------------------------------------------------------------
// AttemptStateMachine
// ---------------------------------------------------------------------------
class AttemptStateMachine {
public:
    static constexpr bool from(const AttemptState from, const AttemptState to) noexcept {
        switch (from) {
        case AttemptState::CREATED:
            return to == AttemptState::DISPATCHED || to == AttemptState::CANCELLED ||
                   to == AttemptState::FAILED || to == AttemptState::TERMINAL;
        case AttemptState::DISPATCHED:
            return to == AttemptState::RUNNING || to == AttemptState::COMPLETE_PENDING ||
                   to == AttemptState::COMPLETED || to == AttemptState::FAILED ||
                   to == AttemptState::CANCELLED || to == AttemptState::LOST ||
                   to == AttemptState::SUPERSEDED || to == AttemptState::TERMINAL;
        case AttemptState::RUNNING:
            return to == AttemptState::COMPLETE_PENDING || to == AttemptState::COMPLETED ||
                   to == AttemptState::FAILED || to == AttemptState::CANCELLED ||
                   to == AttemptState::PREEMPTED || to == AttemptState::LOST ||
                   to == AttemptState::SUPERSEDED || to == AttemptState::TERMINAL;
        case AttemptState::PREEMPTED:
            return to == AttemptState::SUPERSEDED || to == AttemptState::TERMINAL;
        case AttemptState::COMPLETE_PENDING:
            return to == AttemptState::COMPLETED || to == AttemptState::FAILED ||
                   to == AttemptState::LOST || to == AttemptState::TERMINAL;
        case AttemptState::COMPLETED:
            return to == AttemptState::TERMINAL;
        case AttemptState::FAILED:
        case AttemptState::CANCELLED:
        case AttemptState::LOST:
        case AttemptState::SUPERSEDED:
        case AttemptState::TERMINAL:
            return false;
        }
        return false;
    }

    static constexpr bool is_terminal(const AttemptState s) noexcept {
        return s == AttemptState::FAILED || s == AttemptState::CANCELLED ||
               s == AttemptState::LOST || s == AttemptState::SUPERSEDED ||
               s == AttemptState::TERMINAL;
    }
};

}  // namespace execution_fabric