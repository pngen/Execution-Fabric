#include "execution_fabric/state.hpp"

namespace execution_fabric {

const char* to_string(ExecutionState s) noexcept {
    switch (s) {
    case ExecutionState::CREATED: return "CREATED";
    case ExecutionState::READY: return "READY";
    case ExecutionState::DISPATCHED: return "DISPATCHED";
    case ExecutionState::RUNNING: return "RUNNING";
    case ExecutionState::CANCELLATION_REQUESTED: return "CANCELLATION_REQUESTED";
    case ExecutionState::PREEMPTION_REQUESTED: return "PREEMPTION_REQUESTED";
    case ExecutionState::PREEMPTED: return "PREEMPTED";
    case ExecutionState::RESUMABLE: return "RESUMABLE";
    case ExecutionState::RESUMING: return "RESUMING";
    case ExecutionState::COMPLETION_PENDING: return "COMPLETION_PENDING";
    case ExecutionState::COMPLETED: return "COMPLETED";
    case ExecutionState::COMMITTED: return "COMMITTED";
    case ExecutionState::FAILED: return "FAILED";
    case ExecutionState::CANCELLED: return "CANCELLED";
    case ExecutionState::AMBIGUOUS: return "AMBIGUOUS";
    case ExecutionState::SUPERSEDED: return "SUPERSEDED";
    case ExecutionState::TERMINAL: return "TERMINAL";
    }
    return "UNKNOWN";
}

const char* to_string(AttemptState s) noexcept {
    switch (s) {
    case AttemptState::CREATED: return "CREATED";
    case AttemptState::DISPATCHED: return "DISPATCHED";
    case AttemptState::RUNNING: return "RUNNING";
    case AttemptState::PREEMPTED: return "PREEMPTED";
    case AttemptState::COMPLETE_PENDING: return "COMPLETE_PENDING";
    case AttemptState::COMPLETED: return "COMPLETED";
    case AttemptState::FAILED: return "FAILED";
    case AttemptState::CANCELLED: return "CANCELLED";
    case AttemptState::LOST: return "LOST";
    case AttemptState::SUPERSEDED: return "SUPERSEDED";
    case AttemptState::TERMINAL: return "TERMINAL";
    }
    return "UNKNOWN";
}

}  // namespace execution_fabric