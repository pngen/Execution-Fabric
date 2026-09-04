#pragma once
#include "execution_fabric/identity.hpp"
#include "execution_fabric/generation.hpp"
#include "execution_fabric/digest.hpp"
#include <cstdint>
#include <vector>

namespace execution_fabric {

// ---------------------------------------------------------------------------
// OwnershipRole
//
// The distinct authority domains that guard mutating operations. Ownership of
// each domain is generation-fenced: the holder of the current generation for
// that domain may act, and any action carrying a stale generation is rejected.
// ---------------------------------------------------------------------------
enum class OwnershipRole : std::uint8_t {
    COORDINATOR = 0,   // authoritative orchestration authority
    DISPATCHER = 1,    // may create/dispatch new attempts
    EXECUTOR = 2,      // may begin/continue physical execution
    CANCELLER = 3,     // may authorise cancellation
    PREEMPTOR = 4,     // may authorise preemption
    RESUMER = 5,       // may authorise resume
    COMPLETER = 6,     // may report physical completion
    COMMITTER = 7,     // may authorise the logical commit
};

const char* to_string(OwnershipRole r) noexcept;

// ---------------------------------------------------------------------------
// Ticket types
//
// A ticket is the authority-fenced credential that a physical actor presents
// when it acts. Every field is validated against the coordinator's current
// authoritative record; any mismatch yields a precise reject code.
// ---------------------------------------------------------------------------

// The authority ticket a worker presents in a physical completion message.
struct CompletionTicket {
    ExecutionId execution_id;
    ExecutionGeneration execution_generation;
    AttemptId attempt_id;
    AttemptGeneration attempt_generation;
    WorkerId worker_id;
    WorkerBootId worker_boot_id;
    CoordinatorEpoch epoch;
    DispatchId dispatch_id;
    DispatchGeneration dispatch_generation;
    OwnershipGeneration ownership_generation;
    FenceGeneration fence_generation;
    CompletionGeneration completion_generation;
    bool has_result = false;
    ResultDigest result_digest;
};

// The authority ticket a worker presents when acknowledging that it started.
struct StartTicket {
    ExecutionId execution_id;
    ExecutionGeneration execution_generation;
    AttemptId attempt_id;
    AttemptGeneration attempt_generation;
    WorkerId worker_id;
    WorkerBootId worker_boot_id;
    CoordinatorEpoch epoch;
    DispatchId dispatch_id;
    DispatchGeneration dispatch_generation;
    OwnershipGeneration ownership_generation;
    FenceGeneration fence_generation;
};

// The authority ticket the coordinator issues to a worker to authorise a
// specific physical attempt.
struct DispatchTicket {
    ExecutionId execution_id;
    ExecutionGeneration execution_generation;
    AttemptId attempt_id;
    AttemptGeneration attempt_generation;
    WorkerId worker_id;
    WorkerBootId worker_boot_id;
    CoordinatorEpoch epoch;
    DispatchId dispatch_id;
    DispatchGeneration dispatch_generation;
    OwnershipGeneration ownership_generation;
    FenceGeneration fence_generation;
};

// An authorisation request for cancellation / preemption / resume.
struct ControlRequest {
    ExecutionId execution_id;
    ExecutionGeneration execution_generation;
    CoordinatorEpoch epoch;
    OwnershipGeneration authority_generation;  // canceller/preemptor/resumer fence
    CancellationGeneration cancellation_generation{0};
    PreemptionGeneration preemption_generation{0};
    ResumeGeneration resume_generation{0};
};

}  // namespace execution_fabric