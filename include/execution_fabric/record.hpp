#pragma once
#include "execution_fabric/identity.hpp"
#include "execution_fabric/generation.hpp"
#include "execution_fabric/state.hpp"
#include "execution_fabric/digest.hpp"
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace execution_fabric {

// ---------------------------------------------------------------------------
// AttemptRecord
//
// A durable snapshot of one physical execution attempt. Historical attempts
// are preserved: an execution's authority decisions must be auditable through
// its full attempt history, not just the current attempt.
// ---------------------------------------------------------------------------
struct AttemptRecord {
    AttemptId id;
    AttemptGeneration generation{0};
    WorkerId worker_id;
    WorkerBootId worker_boot_id;
    DispatchId dispatch_id;
    DispatchGeneration dispatch_generation{0};
    OwnershipGeneration ownership_generation{0};
    FenceGeneration fence_generation{0};
    ExecutionState execution_state_at_dispatch = ExecutionState::CREATED;
    AttemptState state = AttemptState::CREATED;
    std::optional<ResultDigest> result_digest;
    std::optional<std::string> result_payload_hex;   // snapshot for tests
    std::optional<std::string> failure_reason;
};

// ---------------------------------------------------------------------------
// ExecutionRecord
//
// Durable authoritative record of one logical execution. This is the
// coordinator-side source of truth for the authority model: it answers who
// owns the execution, which attempt is current, what may still run, and which
// completion may commit.
// ---------------------------------------------------------------------------
struct ExecutionRecord {
    ExecutionId id;
    ExecutionGeneration generation;
    ExecutionState state = ExecutionState::CREATED;

    CoordinatorEpoch coordinator_epoch;
    OwnershipGeneration ownership_generation;
    FenceGeneration fence_generation;

    // Current attempt (if any).
    std::optional<AttemptId> current_attempt_id;
    std::optional<AttemptGeneration> current_attempt_generation;
    std::optional<WorkerId> owner_worker;
    std::optional<WorkerBootId> owner_worker_boot;

    // Generation counters for the authority domains.
    CancellationGeneration cancellation_generation;
    PreemptionGeneration preemption_generation;
    ResumeGeneration resume_generation;
    CompletionGeneration completion_generation;
    CommitGeneration commit_generation;

    std::optional<ResultDigest> committed_digest;

    // Attempt history (oldest first). The current attempt, if any, is the last
    // non-superseded entry.
    std::vector<AttemptRecord> attempts;

    std::string created_at;   // human-readable, for diagnostics only
    std::string committed_at; // human-readable, for diagnostics only
};

}  // namespace execution_fabric