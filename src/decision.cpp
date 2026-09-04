#include "execution_fabric/decision.hpp"
#include "execution_fabric/authority.hpp"

namespace execution_fabric {

const char* to_string(DecisionCode c) noexcept {
    switch (c) {
    case DecisionCode::ALLOW: return "ALLOW";
    case DecisionCode::REJECT_STALE_EPOCH: return "REJECT_STALE_EPOCH";
    case DecisionCode::REJECT_STALE_BOOT: return "REJECT_STALE_BOOT";
    case DecisionCode::REJECT_STALE_ATTEMPT: return "REJECT_STALE_ATTEMPT";
    case DecisionCode::REJECT_STALE_GENERATION: return "REJECT_STALE_GENERATION";
    case DecisionCode::REJECT_NOT_OWNER: return "REJECT_NOT_OWNER";
    case DecisionCode::REJECT_ALREADY_TERMINAL: return "REJECT_ALREADY_TERMINAL";
    case DecisionCode::REJECT_CANCELLED: return "REJECT_CANCELLED";
    case DecisionCode::REJECT_PREEMPTED: return "REJECT_PREEMPTED";
    case DecisionCode::REJECT_CONFLICTING_COMPLETION: return "REJECT_CONFLICTING_COMPLETION";
    case DecisionCode::REJECT_ALREADY_COMMITTED: return "REJECT_ALREADY_COMMITTED";
    case DecisionCode::REJECT_INSUFFICIENT_AUTHORITY: return "REJECT_INSUFFICIENT_AUTHORITY";
    case DecisionCode::RETRY_ALLOWED: return "RETRY_ALLOWED";
    case DecisionCode::RESUME_ALLOWED: return "RESUME_ALLOWED";
    case DecisionCode::DEFER: return "DEFER";
    case DecisionCode::UNKNOWN: return "UNKNOWN";
    case DecisionCode::REJECT_EXISTS: return "REJECT_EXISTS";
    case DecisionCode::REJECT_UNKNOWN_EXECUTION: return "REJECT_UNKNOWN_EXECUTION";
    case DecisionCode::REJECT_NO_CURRENT_ATTEMPT: return "REJECT_NO_CURRENT_ATTEMPT";
    case DecisionCode::RETRY_REJECTED: return "RETRY_REJECTED";
    }
    return "UNKNOWN";
}

const char* to_string(OwnershipRole r) noexcept {
    switch (r) {
    case OwnershipRole::COORDINATOR: return "COORDINATOR";
    case OwnershipRole::DISPATCHER: return "DISPATCHER";
    case OwnershipRole::EXECUTOR: return "EXECUTOR";
    case OwnershipRole::CANCELLER: return "CANCELLER";
    case OwnershipRole::PREEMPTOR: return "PREEMPTOR";
    case OwnershipRole::RESUMER: return "RESUMER";
    case OwnershipRole::COMPLETER: return "COMPLETER";
    case OwnershipRole::COMMITTER: return "COMMITTER";
    }
    return "UNKNOWN";
}

std::string Decision::to_string() const {
    std::string out = std::string(execution_fabric::to_string(code));
    if (!reason.empty()) { out += ": " + reason; }
    if (execution_id) {
        out += " [exec=" + execution_id->to_string();
        if (execution_generation) { out += " gen=" + std::to_string(execution_generation->value()); }
        if (attempt_id) { out += " attempt=" + attempt_id->to_string(); }
        if (attempt_generation) { out += " attemptGen=" + std::to_string(attempt_generation->value()); }
        if (worker_id) { out += " worker=" + worker_id->to_string(); }
        if (worker_boot_id) { out += " boot=" + worker_boot_id->to_string(); }
        if (coordinator_epoch) { out += " epoch=" + std::to_string(coordinator_epoch->value()); }
        out += "]";
    }
    if (!required_authority.empty()) { out += " required=" + required_authority; }
    return out;
}

}  // namespace execution_fabric