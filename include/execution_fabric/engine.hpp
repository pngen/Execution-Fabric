#pragma once
#include "execution_fabric/authority.hpp"
#include "execution_fabric/decision.hpp"
#include "execution_fabric/record.hpp"
#include <cstdint>
#include <unordered_map>
#include <vector>

namespace execution_fabric {

// ---------------------------------------------------------------------------
// ExecutionEngine
//
// The pure authority-decision component. It holds the authoritative record of
// every logical execution and answers, with a deterministic Decision, whether
// each requested mutation may proceed. It contains no I/O: the coordinator
// process pumps framed messages into it and applies the returned decisions,
// while a single decision thread owns all mutation so that winner semantics
// are defined by a deterministic, serial event ordering.
//
// Thread-safety: an ExecutionEngine must be driven by one thread at a time.
// The coordinator provides that via an internal single-threaded event queue.
// ---------------------------------------------------------------------------
class ExecutionEngine {
public:
    explicit ExecutionEngine(CoordinatorEpoch initial_epoch);

    CoordinatorEpoch current_epoch() const noexcept { return epoch_; }

    // --- Creation / activation -------------------------------------------------
    Decision create(const ExecutionId& id, const CoordinatorEpoch& epoch);
    Decision activate(const ExecutionId& id, const CoordinatorEpoch& epoch);

    // --- Dispatch / execution --------------------------------------------------
    // Authorises a new physical attempt on a worker. Advances attempt &
    // ownership authority and supersedes any prior current attempt. On ALLOW,
    // fills out_ticket with the authority ticket the worker must present.
    Decision dispatch(const ExecutionId& id, const WorkerId& worker, const WorkerBootId& boot,
                      const CoordinatorEpoch& epoch, DispatchTicket& out_ticket);

    // Worker acknowledges that it began running the dispatched attempt.
    Decision mark_running(const StartTicket& ticket);

    // Worker reports physical completion; validates authority and commits.
    Decision complete(const CompletionTicket& ticket);

    // --- Control ---------------------------------------------------------------
    Decision cancel(const ControlRequest& req);
    // Terminalize a requested cancellation (worker acked or worker gone).
    Decision finalize_cancel(const ExecutionId& id, const CoordinatorEpoch& epoch);
    Decision preempt(const ControlRequest& req);
    // Worker acknowledges preemption quiescence.
    Decision acknowledge_preempt(const ExecutionId& id, const CoordinatorEpoch& epoch);
    // Mark the preempted execution as eligible for resume.
    Decision mark_resumable(const ExecutionId& id, const CoordinatorEpoch& epoch);
    // Resume under a fresh attempt/generation. Fills out_ticket on ALLOW.
    Decision resume(const ExecutionId& id, const WorkerId& worker, const WorkerBootId& boot,
                    const CoordinatorEpoch& epoch, const ResumeGeneration& resume_gen,
                    DispatchTicket& out_ticket);

    // --- Worker-loss classification -------------------------------------------
    // Classify what a worker loss means for an execution and advance authority.
    Decision mark_worker_lost(const ExecutionId& id, const WorkerId& worker, const WorkerBootId& boot,
                              const CoordinatorEpoch& epoch);

    // --- Supersession (logical generation) ------------------------------------
    // Advance the logical execution generation, superseding the prior one.
    Decision supersede(const ExecutionId& id, const CoordinatorEpoch& epoch);

    // --- Introspection ---------------------------------------------------------
    const ExecutionRecord* find(const ExecutionId& id) const noexcept;
    std::vector<ExecutionId> all_executions() const;
    std::size_t execution_count() const noexcept { return records_.size(); }

    // Recovery: install records (validated by the persistence layer).
    void replace_records(std::vector<ExecutionRecord>&& records);

    std::vector<ExecutionRecord> records() const;

    // Raise the coordinator epoch (restart/rollover).
    void raise_epoch(const CoordinatorEpoch& epoch);

private:
    ExecutionRecord& require(const ExecutionId& id);
    ExecutionRecord* find_mut(const ExecutionId& id) noexcept;
    const ExecutionRecord* try_find(const ExecutionId& id) const noexcept;

    bool epoch_valid(const CoordinatorEpoch& e) const noexcept { return e >= epoch_; }
    void adopt_epoch(const CoordinatorEpoch& e) noexcept { if (e > epoch_) { epoch_ = e; } }

    // Validate a completion/start ticket's authority against the record.
    Decision validate_ticket(const ExecutionRecord& rec, const CompletionTicket& t) const;
    Decision validate_start(const ExecutionRecord& rec, const StartTicket& t) const;

    // Helpers.
    Decision transition(ExecutionRecord& rec, ExecutionState to);
    Decision transition_attempt(AttemptRecord& at, AttemptState to);
    CoordinatorEpoch epoch_;
    std::unordered_map<ExecutionId, ExecutionRecord> records_;
};

}  // namespace execution_fabric