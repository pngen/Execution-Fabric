#pragma once
#include "execution_fabric/identity.hpp"
#include "execution_fabric/generation.hpp"
#include <optional>
#include <string>

namespace execution_fabric {

// ---------------------------------------------------------------------------
// DecisionCode
//
// The explicit outcome of evaluating whether some requested mutation or
// authority assertion may proceed. Unknown never becomes implicit permission:
// every rejected path yields a specific, machine-readable code, and any path
// the system cannot conclusively adjudicate yields UNKNOWN (which callers must
// treat as non-permission).
// ---------------------------------------------------------------------------
enum class DecisionCode : std::uint8_t {
    ALLOW = 0,                     // the mutation is accepted
    REJECT_STALE_EPOCH = 1,        // coordinator epoch is older than current
    REJECT_STALE_BOOT = 2,         // WorkerBootId is no longer the live incarnation
    REJECT_STALE_ATTEMPT = 3,      // attempt is not the current attempt
    REJECT_STALE_GENERATION = 4,   // a generation counter is older than current
    REJECT_NOT_OWNER = 5,          // actor does not currently hold the relevant ownership
    REJECT_ALREADY_TERMINAL = 6,   // execution is already in a terminal state
    REJECT_CANCELLED = 7,          // cancellation has been authorised
    REJECT_PREEMPTED = 8,          // preemption has been authorised
    REJECT_CONFLICTING_COMPLETION = 9,   // completion result conflicts with committed result
    REJECT_ALREADY_COMMITTED = 10,       // logical result is already committed
    REJECT_INSUFFICIENT_AUTHORITY = 11,  // ticket lacks required authority fields
    RETRY_ALLOWED = 12,            // a retry may be planned
    RESUME_ALLOWED = 13,           // resume may proceed
    DEFER = 14,                    // cannot decide now; retry later
    UNKNOWN = 15,                  // cannot conclusively adjudicate
    REJECT_EXISTS = 16,            // identity already exists (duplicate create)
    REJECT_UNKNOWN_EXECUTION = 17, // no such logical execution
    REJECT_NO_CURRENT_ATTEMPT = 18,// no authoritative attempt exists yet
    RETRY_REJECTED = 19,           // retry is forbidden by policy/state
};

const char* to_string(DecisionCode c) noexcept;

// The full, actionable explanation of a decision. Contains the code plus
// enough authoritative context to answer "why" and "what authority must
// change before this could become valid".
struct Decision {
    DecisionCode code = DecisionCode::UNKNOWN;
    std::string reason;

    // Current authoritative context at decision time (where relevant).
    std::optional<ExecutionId> execution_id;
    std::optional<ExecutionGeneration> execution_generation;
    std::optional<AttemptId> attempt_id;
    std::optional<AttemptGeneration> attempt_generation;
    std::optional<WorkerId> worker_id;
    std::optional<WorkerBootId> worker_boot_id;
    std::optional<CoordinatorEpoch> coordinator_epoch;
    std::optional<DispatchId> dispatch_id;
    std::optional<DispatchGeneration> dispatch_generation;
    std::optional<OwnershipGeneration> ownership_generation;
    std::optional<FenceGeneration> fence_generation;

    // What would have to change for this operation to become valid.
    std::string required_authority;

    bool allowed() const noexcept {
        return code == DecisionCode::ALLOW || code == DecisionCode::RETRY_ALLOWED ||
               code == DecisionCode::RESUME_ALLOWED;
    }
    bool is_reject() const noexcept {
        return code == DecisionCode::REJECT_STALE_EPOCH || code == DecisionCode::REJECT_STALE_BOOT ||
               code == DecisionCode::REJECT_STALE_ATTEMPT || code == DecisionCode::REJECT_STALE_GENERATION ||
               code == DecisionCode::REJECT_NOT_OWNER || code == DecisionCode::REJECT_ALREADY_TERMINAL ||
               code == DecisionCode::REJECT_CANCELLED || code == DecisionCode::REJECT_PREEMPTED ||
               code == DecisionCode::REJECT_CONFLICTING_COMPLETION || code == DecisionCode::REJECT_ALREADY_COMMITTED ||
               code == DecisionCode::REJECT_INSUFFICIENT_AUTHORITY || code == DecisionCode::UNKNOWN;
    }

    std::string to_string() const;
};

}  // namespace execution_fabric