#include "execution_fabric/engine.hpp"
#include "execution_fabric/state_machine.hpp"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <sstream>
#include <utility>

namespace execution_fabric {

namespace {

std::string now_str() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
#ifdef _WIN32
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return std::string(buf);
}

// Populate the authoritative context that explains a decision.
void fill_context(Decision& d, const ExecutionRecord& rec) {
    d.execution_id = rec.id;
    d.execution_generation = rec.generation;
    d.coordinator_epoch = rec.coordinator_epoch;
    d.ownership_generation = rec.ownership_generation;
    d.fence_generation = rec.fence_generation;
    if (rec.current_attempt_id) { d.attempt_id = rec.current_attempt_id; }
    if (rec.current_attempt_generation) { d.attempt_generation = rec.current_attempt_generation; }
    if (rec.owner_worker) { d.worker_id = rec.owner_worker; }
    if (rec.owner_worker_boot) { d.worker_boot_id = rec.owner_worker_boot; }
}

}  // namespace

ExecutionEngine::ExecutionEngine(CoordinatorEpoch initial_epoch) : epoch_(initial_epoch) {}

ExecutionRecord& ExecutionEngine::require(const ExecutionId& id) {
    return records_.at(id);
}

const ExecutionRecord* ExecutionEngine::try_find(const ExecutionId& id) const noexcept {
    const auto it = records_.find(id);
    return it == records_.end() ? nullptr : &it->second;
}

ExecutionRecord* ExecutionEngine::find_mut(const ExecutionId& id) noexcept {
    const auto it = records_.find(id);
    return it == records_.end() ? nullptr : &it->second;
}

const ExecutionRecord* ExecutionEngine::find(const ExecutionId& id) const noexcept {
    return try_find(id);
}

std::vector<ExecutionId> ExecutionEngine::all_executions() const {
    std::vector<ExecutionId> out;
    out.reserve(records_.size());
    for (const auto& kv : records_) { out.push_back(kv.first); }
    return out;
}

void ExecutionEngine::replace_records(std::vector<ExecutionRecord>&& records) {
    records_.clear();
    for (auto& r : records) {
        records_.emplace(r.id, std::move(r));
    }
    // Adopt the highest epoch recorded so recovered authority does not go backward.
    for (const auto& kv : records_) {
        if (kv.second.coordinator_epoch > epoch_) { epoch_ = kv.second.coordinator_epoch; }
    }
}

void ExecutionEngine::raise_epoch(const CoordinatorEpoch& e) { adopt_epoch(e); }

std::vector<ExecutionRecord> ExecutionEngine::records() const {
    std::vector<ExecutionRecord> out;
    out.reserve(records_.size());
    for (const auto& kv : records_) { out.push_back(kv.second); }
    std::sort(out.begin(), out.end(), [](const ExecutionRecord& a, const ExecutionRecord& b) {
        return a.id < b.id;
    });
    return out;
}

// ---------------------------------------------------------------------------
// Transitions
// ---------------------------------------------------------------------------
Decision ExecutionEngine::transition(ExecutionRecord& rec, ExecutionState to) {
    if (!ExecutionStateMachine::from(rec.state, to)) {
        Decision d;
        d.code = DecisionCode::REJECT_ALREADY_TERMINAL;
        d.reason = "invalid execution state transition " + std::string(to_string(rec.state)) +
                   " -> " + std::string(to_string(to));
        fill_context(d, rec);
        return d;
    }
    rec.state = to;
    Decision d;
    d.code = DecisionCode::ALLOW;
    d.reason = "execution transitioned " + std::string(to_string(to));
    fill_context(d, rec);
    return d;
}

Decision ExecutionEngine::transition_attempt(AttemptRecord& at, AttemptState to) {
    if (!AttemptStateMachine::from(at.state, to)) {
        Decision d;
        d.code = DecisionCode::REJECT_NO_CURRENT_ATTEMPT;
        d.reason = "invalid attempt state transition " + std::string(to_string(at.state)) +
                   " -> " + std::string(to_string(to));
        return d;
    }
    at.state = to;
    Decision d;
    d.code = DecisionCode::ALLOW;
    d.reason = "attempt transitioned " + std::string(to_string(to));
    return d;
}

// ---------------------------------------------------------------------------
// Create / activate
// ---------------------------------------------------------------------------
Decision ExecutionEngine::create(const ExecutionId& id, const CoordinatorEpoch& epoch) {
    if (epoch < epoch_) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "create rejected: coordinator epoch " + std::to_string(epoch.value()) +
                   " is older than current " + std::to_string(epoch_.value());
        d.execution_id = id;
        d.coordinator_epoch = epoch_;
        return d;
    }
    adopt_epoch(epoch);
    if (records_.count(id) != 0) {
        Decision d;
        d.code = DecisionCode::REJECT_EXISTS;
        d.reason = "create rejected: execution already exists";
        d.execution_id = id;
        d.coordinator_epoch = epoch_;
        return d;
    }
    ExecutionRecord rec;
    rec.id = id;
    rec.generation = ExecutionGeneration(1);
    rec.state = ExecutionState::CREATED;
    rec.coordinator_epoch = epoch_;
    rec.created_at = now_str();
    records_.emplace(id, std::move(rec));
    Decision d;
    d.code = DecisionCode::ALLOW;
    d.reason = "execution created";
    d.execution_id = id;
    d.coordinator_epoch = epoch_;
    return d;
}

Decision ExecutionEngine::activate(const ExecutionId& id, const CoordinatorEpoch& epoch) {
    ExecutionRecord& rec = require(id);
    if (epoch < epoch_ || epoch < rec.coordinator_epoch) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "activate rejected: stale coordinator epoch";
        fill_context(d, rec);
        return d;
    }
    adopt_epoch(epoch);
    if (rec.state != ExecutionState::CREATED) {
        Decision d;
        d.code = DecisionCode::REJECT_ALREADY_TERMINAL;
        d.reason = "activate rejected: execution is not in CREATED state";
        fill_context(d, rec);
        return d;
    }
    // Establish initial authority.
    rec.ownership_generation = OwnershipGeneration(1);
    rec.fence_generation = FenceGeneration(1);
    return transition(rec, ExecutionState::READY);
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------
Decision ExecutionEngine::dispatch(const ExecutionId& id, const WorkerId& worker,
                                   const WorkerBootId& boot, const CoordinatorEpoch& epoch,
                                   DispatchTicket& out_ticket) {
    ExecutionRecord& rec = require(id);
    if (epoch < epoch_ || epoch < rec.coordinator_epoch) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "dispatch rejected: stale coordinator epoch";
        fill_context(d, rec);
        return d;
    }
    adopt_epoch(epoch);
    if (rec.state == ExecutionState::COMMITTED) {
        Decision d;
        d.code = DecisionCode::REJECT_ALREADY_COMMITTED;
        d.reason = "dispatch rejected: execution already committed";
        fill_context(d, rec);
        return d;
    }
    if (rec.state == ExecutionState::CANCELLED) {
        Decision d;
        d.code = DecisionCode::REJECT_CANCELLED;
        d.reason = "dispatch rejected: execution is cancelled";
        fill_context(d, rec);
        return d;
    }
    if (rec.state == ExecutionState::FAILED || rec.state == ExecutionState::SUPERSEDED ||
        rec.state == ExecutionState::TERMINAL) {
        Decision d;
        d.code = DecisionCode::REJECT_ALREADY_TERMINAL;
        d.reason = "dispatch rejected: execution is terminal";
        fill_context(d, rec);
        return d;
    }
    if (rec.state == ExecutionState::PREEMPTED || rec.state == ExecutionState::RESUMABLE) {
        Decision d;
        d.code = DecisionCode::REJECT_PREEMPTED;
        d.reason = "dispatch rejected: execution is preempted; it must be resumed, not re-dispatched";
        fill_context(d, rec);
        return d;
    }
    if (rec.state == ExecutionState::CANCELLATION_REQUESTED || rec.state == ExecutionState::PREEMPTION_REQUESTED) {
        Decision d;
        d.code = DecisionCode::DEFER;
        d.reason = "dispatch deferred: control operation in flight";
        fill_context(d, rec);
        return d;
    }
    if (rec.state == ExecutionState::COMPLETED) {
        Decision d;
        d.code = DecisionCode::REJECT_ALREADY_COMMITTED;
        d.reason = "dispatch rejected: execution already completed";
        fill_context(d, rec);
        return d;
    }
    // Allow from READY, DISPATCHED (supersede), RUNNING (supersede), AMBIGUOUS (retry), RESUMING.
    const bool from_ok = (rec.state == ExecutionState::READY || rec.state == ExecutionState::DISPATCHED ||
                          rec.state == ExecutionState::RUNNING || rec.state == ExecutionState::AMBIGUOUS ||
                          rec.state == ExecutionState::RESUMING);
    if (!from_ok) {
        Decision d;
        d.code = DecisionCode::REJECT_NOT_OWNER;
        d.reason = "dispatch rejected: execution not in a dispatchable state";
        fill_context(d, rec);
        return d;
    }

    // Supersede any prior current attempt.
    if (rec.current_attempt_id) {
        for (auto& at : rec.attempts) {
            if (at.id == *rec.current_attempt_id && !AttemptStateMachine::is_terminal(at.state)) {
                transition_attempt(at, AttemptState::SUPERSEDED);
                break;
            }
        }
    }

    // Build the new attempt.
    AttemptRecord at;
    at.id = AttemptId::random();
    at.generation = rec.current_attempt_generation ? rec.current_attempt_generation->next()
                                                   : AttemptGeneration(1);
    at.worker_id = worker;
    at.worker_boot_id = boot;
    at.dispatch_id = DispatchId::random();
    at.dispatch_generation = DispatchGeneration(1);
    at.ownership_generation = rec.ownership_generation.next();
    at.fence_generation = rec.fence_generation.next();
    at.execution_state_at_dispatch = rec.state;
    at.state = AttemptState::DISPATCHED;

    rec.current_attempt_id = at.id;
    rec.current_attempt_generation = at.generation;
    rec.owner_worker = worker;
    rec.owner_worker_boot = boot;
    rec.ownership_generation = at.ownership_generation;
    rec.fence_generation = at.fence_generation;
    rec.attempts.push_back(at);

    Decision tr = transition(rec, ExecutionState::DISPATCHED);
    if (tr.code != DecisionCode::ALLOW) {
        // Should not happen given the allowed from-states above, but guard anyway.
        return tr;
    }

    const AttemptRecord& made = rec.attempts.back();
    out_ticket = DispatchTicket{};
    out_ticket.execution_id = rec.id;
    out_ticket.execution_generation = rec.generation;
    out_ticket.attempt_id = made.id;
    out_ticket.attempt_generation = made.generation;
    out_ticket.worker_id = made.worker_id;
    out_ticket.worker_boot_id = made.worker_boot_id;
    out_ticket.epoch = epoch_;
    out_ticket.dispatch_id = made.dispatch_id;
    out_ticket.dispatch_generation = made.dispatch_generation;
    out_ticket.ownership_generation = made.ownership_generation;
    out_ticket.fence_generation = made.fence_generation;

    Decision d;
    d.code = DecisionCode::ALLOW;
    d.reason = "dispatch authorised: attempt supersedes prior authority; attempt generation " +
               std::to_string(made.generation.value());
    fill_context(d, rec);
    return d;
}

// ---------------------------------------------------------------------------
// mark_running
// ---------------------------------------------------------------------------
Decision ExecutionEngine::validate_start(const ExecutionRecord& rec, const StartTicket& t) const {
    if (t.epoch != epoch_) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "start rejected: stale coordinator epoch";
        return d;
    }
    if (t.execution_generation != rec.generation) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_GENERATION;
        d.reason = "start rejected: execution generation mismatch";
        return d;
    }
    if (!rec.current_attempt_id || !rec.current_attempt_generation) {
        Decision d;
        d.code = DecisionCode::REJECT_NO_CURRENT_ATTEMPT;
        d.reason = "start rejected: no current attempt";
        return d;
    }
    if (t.attempt_id != *rec.current_attempt_id || t.attempt_generation != *rec.current_attempt_generation) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_ATTEMPT;
        d.reason = "start rejected: attempt is not the current attempt";
        return d;
    }
    if (t.worker_id != rec.owner_worker || t.worker_boot_id != rec.owner_worker_boot) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_BOOT;
        d.reason = "start rejected: worker/boot does not own the attempt";
        return d;
    }
    if (t.ownership_generation != rec.ownership_generation) {
        Decision d;
        d.code = DecisionCode::REJECT_NOT_OWNER;
        d.reason = "start rejected: ownership generation mismatch";
        return d;
    }
    if (t.fence_generation != rec.fence_generation) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_GENERATION;
        d.reason = "start rejected: fence generation mismatch";
        return d;
    }
    Decision d;
    d.code = DecisionCode::ALLOW;
    return d;
}

Decision ExecutionEngine::mark_running(const StartTicket& ticket) {
    ExecutionRecord* prec = find_mut(ticket.execution_id);
    if (!prec) {
        Decision d;
        d.code = DecisionCode::REJECT_UNKNOWN_EXECUTION;
        d.reason = "start rejected: unknown execution";
        return d;
    }
    ExecutionRecord& rec = *prec;
    if (rec.state != ExecutionState::DISPATCHED && rec.state != ExecutionState::RESUMING) {
        Decision d;
        d.code = DecisionCode::REJECT_ALREADY_TERMINAL;
        d.reason = "start rejected: execution not waiting on a running attempt";
        fill_context(d, rec);
        return d;
    }
    Decision v = validate_start(rec, ticket);
    if (v.code != DecisionCode::ALLOW) { fill_context(v, rec); return v; }

    // Find the attempt and transition it.
    for (auto& at : rec.attempts) {
        if (at.id == ticket.attempt_id) {
            transition_attempt(at, AttemptState::RUNNING);
            break;
        }
    }
    Decision tr = transition(rec, ExecutionState::RUNNING);
    fill_context(tr, rec);
    return tr;
}

// ---------------------------------------------------------------------------
// complete
// ---------------------------------------------------------------------------
Decision ExecutionEngine::validate_ticket(const ExecutionRecord& rec, const CompletionTicket& t) const {
    // Insufficient authority: required identity fields missing.
    if (t.execution_id.is_nil() || t.attempt_id.is_nil() || t.worker_id.is_nil() ||
        t.worker_boot_id.is_nil() || t.dispatch_id.is_nil()) {
        Decision d;
        d.code = DecisionCode::REJECT_INSUFFICIENT_AUTHORITY;
        d.reason = "completion rejected: ticket lacks a required identity field";
        return d;
    }
    if (!t.attempt_generation.is_set() || !t.execution_generation.is_set() ||
        !t.ownership_generation.is_set() || !t.fence_generation.is_set()) {
        Decision d;
        d.code = DecisionCode::REJECT_INSUFFICIENT_AUTHORITY;
        d.reason = "completion rejected: ticket lacks a required generation counter";
        return d;
    }
    if (t.epoch != epoch_ || t.epoch != rec.coordinator_epoch) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "completion rejected: coordinator epoch mismatch";
        return d;
    }
    if (t.execution_generation != rec.generation) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_GENERATION;
        d.reason = "completion rejected: execution generation mismatch";
        return d;
    }
    if (!rec.current_attempt_id || !rec.current_attempt_generation) {
        Decision d;
        d.code = DecisionCode::REJECT_NO_CURRENT_ATTEMPT;
        d.reason = "completion rejected: no current attempt";
        return d;
    }
    if (t.attempt_id != *rec.current_attempt_id || t.attempt_generation != *rec.current_attempt_generation) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_ATTEMPT;
        d.reason = "completion rejected: attempt is not the current attempt; a newer attempt has authority";
        return d;
    }
    if (t.worker_id != rec.owner_worker) {
        Decision d;
        d.code = DecisionCode::REJECT_NOT_OWNER;
        d.reason = "completion rejected: worker does not own the attempt";
        return d;
    }
    if (t.worker_boot_id != rec.owner_worker_boot) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_BOOT;
        d.reason = "completion rejected: WorkerBootId is not the live incarnation";
        return d;
    }
    if (t.ownership_generation != rec.ownership_generation) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_GENERATION;
        d.reason = "completion rejected: ownership generation mismatch";
        return d;
    }
    if (t.fence_generation != rec.fence_generation) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_GENERATION;
        d.reason = "completion rejected: fence generation mismatch";
        return d;
    }
    // Dispatch identity must belong to the attempt.
    for (const auto& at : rec.attempts) {
        if (at.id == t.attempt_id && at.dispatch_id != t.dispatch_id) {
            Decision d;
            d.code = DecisionCode::REJECT_STALE_ATTEMPT;
            d.reason = "completion rejected: dispatch identity does not match the attempt";
            return d;
        }
    }
    Decision d;
    d.code = DecisionCode::ALLOW;
    return d;
}

Decision ExecutionEngine::complete(const CompletionTicket& ticket) {
    ExecutionRecord* prec = find_mut(ticket.execution_id);
    if (!prec) {
        Decision d;
        d.code = DecisionCode::REJECT_UNKNOWN_EXECUTION;
        d.reason = "completion rejected: unknown execution";
        return d;
    }
    ExecutionRecord& rec = *prec;

    // Validate authority FIRST. A stale authority (stale epoch, boot, attempt,
    // or generation) must be rejected regardless of the current terminal state,
    // so old authority can never revive or mutate current state.
    Decision v = validate_ticket(rec, ticket);
    if (v.code != DecisionCode::ALLOW) { fill_context(v, rec); return v; }

    // Already committed: idempotent recognition or conflicting duplicate.
    if (rec.state == ExecutionState::COMMITTED) {
        Decision d;
        d.code = DecisionCode::REJECT_ALREADY_COMMITTED;
        if (ticket.has_result && rec.committed_digest && ticket.result_digest == *rec.committed_digest) {
            d.reason = "completion is a duplicate of the committed result; no double commit (idempotent)";
        } else {
            d.code = DecisionCode::REJECT_CONFLICTING_COMPLETION;
            d.reason = "completion conflicts with the already-committed result";
        }
        fill_context(d, rec);
        return d;
    }

    // Terminal states other than COMMITTED.
    if (ExecutionStateMachine::is_terminal(rec.state)) {
        Decision d;
        if (rec.state == ExecutionState::CANCELLED) {
            d.code = DecisionCode::REJECT_CANCELLED;
            d.reason = "completion rejected: execution is cancelled; late completion after cancellation";
        } else {
            d.code = DecisionCode::REJECT_ALREADY_TERMINAL;
            d.reason = "completion rejected: execution is in a terminal state";
        }
        fill_context(d, rec);
        return d;
    }

    // Preempted: an old preempted attempt may never regain authority.
    if (rec.state == ExecutionState::PREEMPTED) {
        Decision d;
        d.code = DecisionCode::REJECT_PREEMPTED;
        d.reason = "completion rejected: attempt was preempted; only a post-resume attempt may complete";
        fill_context(d, rec);
        return d;
    }

    if (!ticket.has_result || ticket.result_digest.is_nil()) {
        Decision d;
        d.code = DecisionCode::REJECT_INSUFFICIENT_AUTHORITY;
        d.reason = "completion rejected: authoritative completion must carry a result digest";
        fill_context(d, rec);
        return d;
    }

    // Cancellation race: a completion from the current authority that reaches
    // the engine while cancellation/preemption is only *requested* (not yet
    // terminal) wins deterministically, because the engine's serial event
    // ordering defines the winner. If cancellation was already terminalized,
    // the state above was CANCELLED and we returned REJECT_CANCELLED.
    if (rec.state == ExecutionState::CANCELLATION_REQUESTED ||
        rec.state == ExecutionState::PREEMPTION_REQUESTED) {
        // Fall through: completion wins the race.
    }

    // Mark attempt complete.
    for (auto& at : rec.attempts) {
        if (at.id == ticket.attempt_id) {
            transition_attempt(at, AttemptState::COMPLETED);
            at.result_digest = ticket.result_digest;
            break;
        }
    }

    // Two-stage: physical completion -> authority validation -> logical commit.
    Decision c = transition(rec, ExecutionState::COMPLETED);
    if (c.code != DecisionCode::ALLOW) { return c; }
    rec.committed_digest = ticket.result_digest;
    rec.commit_generation = rec.commit_generation.next();
    rec.committed_at = now_str();
    c = transition(rec, ExecutionState::COMMITTED);
    fill_context(c, rec);
    return c;
}

// ---------------------------------------------------------------------------
// Control: cancel / preempt / resume
// ---------------------------------------------------------------------------
Decision ExecutionEngine::cancel(const ControlRequest& req) {
    ExecutionRecord* prec = find_mut(req.execution_id);
    if (!prec) {
        Decision d;
        d.code = DecisionCode::REJECT_UNKNOWN_EXECUTION;
        d.reason = "cancel rejected: unknown execution";
        return d;
    }
    ExecutionRecord& rec = *prec;
    if (req.epoch != epoch_ || req.epoch != rec.coordinator_epoch) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "cancel rejected: stale coordinator epoch";
        fill_context(d, rec);
        return d;
    }
    if (req.authority_generation != rec.ownership_generation) {
        Decision d;
        d.code = DecisionCode::REJECT_NOT_OWNER;
        d.reason = "cancel rejected: caller does not hold current ownership authority";
        fill_context(d, rec);
        return d;
    }
    if (ExecutionStateMachine::is_terminal(rec.state)) {
        Decision d;
        d.code = DecisionCode::REJECT_ALREADY_TERMINAL;
        d.reason = "cancel rejected: execution is terminal";
        fill_context(d, rec);
        return d;
    }
    if (rec.state == ExecutionState::CANCELLATION_REQUESTED) {
        Decision d;
        d.code = DecisionCode::ALLOW;
        d.reason = "cancel is already requested (idempotent)";
        fill_context(d, rec);
        return d;
    }
    rec.cancellation_generation = rec.cancellation_generation.next();
    if (rec.state == ExecutionState::CREATED) {
        return transition(rec, ExecutionState::CANCELLED);
    }
    if (rec.state == ExecutionState::READY || rec.state == ExecutionState::DISPATCHED ||
        rec.state == ExecutionState::RUNNING) {
        return transition(rec, ExecutionState::CANCELLATION_REQUESTED);
    }
    Decision d;
    d.code = DecisionCode::REJECT_NOT_OWNER;
    d.reason = "cancel rejected: execution not in a cancellable state";
    fill_context(d, rec);
    return d;
}

Decision ExecutionEngine::finalize_cancel(const ExecutionId& id, const CoordinatorEpoch& epoch) {
    ExecutionRecord* prec = find_mut(id);
    if (!prec) {
        Decision d;
        d.code = DecisionCode::REJECT_UNKNOWN_EXECUTION;
        d.reason = "finalize_cancel rejected: unknown execution";
        return d;
    }
    ExecutionRecord& rec = *prec;
    if (epoch < epoch_ || epoch < rec.coordinator_epoch) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "finalize_cancel rejected: stale coordinator epoch";
        fill_context(d, rec);
        return d;
    }
    if (rec.state == ExecutionState::CANCELLED) {
        Decision d;
        d.code = DecisionCode::ALLOW;
        d.reason = "cancellation already terminal (idempotent)";
        fill_context(d, rec);
        return d;
    }
    if (rec.state != ExecutionState::CANCELLATION_REQUESTED) {
        Decision d;
        d.code = DecisionCode::REJECT_NOT_OWNER;
        d.reason = "finalize_cancel rejected: cancellation not in flight";
        fill_context(d, rec);
        return d;
    }
    // Terminalize the execution and the attempt.
    for (auto& at : rec.attempts) {
        if (at.id == rec.current_attempt_id && !AttemptStateMachine::is_terminal(at.state)) {
            transition_attempt(at, AttemptState::CANCELLED);
        }
    }
    return transition(rec, ExecutionState::CANCELLED);
}

Decision ExecutionEngine::preempt(const ControlRequest& req) {
    ExecutionRecord* prec = find_mut(req.execution_id);
    if (!prec) {
        Decision d;
        d.code = DecisionCode::REJECT_UNKNOWN_EXECUTION;
        d.reason = "preempt rejected: unknown execution";
        return d;
    }
    ExecutionRecord& rec = *prec;
    if (req.epoch != epoch_ || req.epoch != rec.coordinator_epoch) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "preempt rejected: stale coordinator epoch";
        fill_context(d, rec);
        return d;
    }
    if (req.authority_generation != rec.ownership_generation) {
        Decision d;
        d.code = DecisionCode::REJECT_NOT_OWNER;
        d.reason = "preempt rejected: caller does not hold current ownership authority";
        fill_context(d, rec);
        return d;
    }
    if (ExecutionStateMachine::is_terminal(rec.state)) {
        Decision d;
        d.code = DecisionCode::REJECT_ALREADY_TERMINAL;
        d.reason = "preempt rejected: execution is terminal";
        fill_context(d, rec);
        return d;
    }
    if (rec.state == ExecutionState::PREEMPTION_REQUESTED) {
        Decision d;
        d.code = DecisionCode::ALLOW;
        d.reason = "preemption already requested (idempotent)";
        fill_context(d, rec);
        return d;
    }
    if (rec.state == ExecutionState::DISPATCHED || rec.state == ExecutionState::RUNNING) {
        rec.preemption_generation = rec.preemption_generation.next();
        return transition(rec, ExecutionState::PREEMPTION_REQUESTED);
    }
    Decision d;
    d.code = DecisionCode::REJECT_NOT_OWNER;
    d.reason = "preempt rejected: execution not running or dispatched";
    fill_context(d, rec);
    return d;
}

Decision ExecutionEngine::acknowledge_preempt(const ExecutionId& id, const CoordinatorEpoch& epoch) {
    ExecutionRecord* prec = find_mut(id);
    if (!prec) {
        Decision d;
        d.code = DecisionCode::REJECT_UNKNOWN_EXECUTION;
        d.reason = "acknowledge_preempt rejected: unknown execution";
        return d;
    }
    ExecutionRecord& rec = *prec;
    if (epoch < epoch_ || epoch < rec.coordinator_epoch) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "acknowledge_preempt rejected: stale coordinator epoch";
        fill_context(d, rec);
        return d;
    }
    if (rec.state != ExecutionState::PREEMPTION_REQUESTED) {
        Decision d;
        d.code = DecisionCode::REJECT_NOT_OWNER;
        d.reason = "acknowledge_preempt rejected: preemption not in flight";
        fill_context(d, rec);
        return d;
    }
    for (auto& at : rec.attempts) {
        if (at.id == rec.current_attempt_id && !AttemptStateMachine::is_terminal(at.state)) {
            transition_attempt(at, AttemptState::PREEMPTED);
        }
    }
    return transition(rec, ExecutionState::PREEMPTED);
}

Decision ExecutionEngine::mark_resumable(const ExecutionId& id, const CoordinatorEpoch& epoch) {
    ExecutionRecord* prec = find_mut(id);
    if (!prec) {
        Decision d;
        d.code = DecisionCode::REJECT_UNKNOWN_EXECUTION;
        d.reason = "mark_resumable rejected: unknown execution";
        return d;
    }
    ExecutionRecord& rec = *prec;
    if (epoch < epoch_ || epoch < rec.coordinator_epoch) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "mark_resumable rejected: stale coordinator epoch";
        fill_context(d, rec);
        return d;
    }
    if (rec.state != ExecutionState::PREEMPTED) {
        Decision d;
        d.code = DecisionCode::REJECT_NOT_OWNER;
        d.reason = "mark_resumable rejected: execution is not preempted";
        fill_context(d, rec);
        return d;
    }
    return transition(rec, ExecutionState::RESUMABLE);
}

Decision ExecutionEngine::resume(const ExecutionId& id, const WorkerId& worker,
                                 const WorkerBootId& boot, const CoordinatorEpoch& epoch,
                                 const ResumeGeneration& resume_gen, DispatchTicket& out_ticket) {
    ExecutionRecord* prec = find_mut(id);
    if (!prec) {
        Decision d;
        d.code = DecisionCode::REJECT_UNKNOWN_EXECUTION;
        d.reason = "resume rejected: unknown execution";
        return d;
    }
    ExecutionRecord& rec = *prec;
    if (epoch != epoch_ || epoch != rec.coordinator_epoch) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "resume rejected: stale coordinator epoch";
        fill_context(d, rec);
        return d;
    }
    if (rec.state != ExecutionState::RESUMABLE) {
        Decision d;
        d.code = DecisionCode::REJECT_PREEMPTED;
        d.reason = "resume rejected: execution is not resumable";
        fill_context(d, rec);
        return d;
    }
    if (resume_gen != rec.resume_generation.next()) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_GENERATION;
        d.reason = "resume rejected: resume generation is stale (must advance once per resume)";
        fill_context(d, rec);
        return d;
    }
    rec.resume_generation = resume_gen;

    // Supersede the preempted attempt.
    for (auto& at : rec.attempts) {
        if (at.id == rec.current_attempt_id && at.state == AttemptState::PREEMPTED) {
            transition_attempt(at, AttemptState::SUPERSEDED);
            break;
        }
    }

    // Create a fresh attempt, mirroring dispatch authority, but under RESUMING.
    AttemptRecord at;
    at.id = AttemptId::random();
    at.generation = rec.current_attempt_generation ? rec.current_attempt_generation->next()
                                                   : AttemptGeneration(1);
    at.worker_id = worker;
    at.worker_boot_id = boot;
    at.dispatch_id = DispatchId::random();
    at.dispatch_generation = DispatchGeneration(1);
    at.ownership_generation = rec.ownership_generation.next();
    at.fence_generation = rec.fence_generation.next();
    at.execution_state_at_dispatch = rec.state;
    at.state = AttemptState::DISPATCHED;

    rec.current_attempt_id = at.id;
    rec.current_attempt_generation = at.generation;
    rec.owner_worker = worker;
    rec.owner_worker_boot = boot;
    rec.ownership_generation = at.ownership_generation;
    rec.fence_generation = at.fence_generation;
    rec.attempts.push_back(at);

    Decision tr = transition(rec, ExecutionState::RESUMING);
    if (tr.code != DecisionCode::ALLOW) { return tr; }

    const AttemptRecord& made = rec.attempts.back();
    out_ticket = DispatchTicket{};
    out_ticket.execution_id = rec.id;
    out_ticket.execution_generation = rec.generation;
    out_ticket.attempt_id = made.id;
    out_ticket.attempt_generation = made.generation;
    out_ticket.worker_id = made.worker_id;
    out_ticket.worker_boot_id = made.worker_boot_id;
    out_ticket.epoch = epoch_;
    out_ticket.dispatch_id = made.dispatch_id;
    out_ticket.dispatch_generation = made.dispatch_generation;
    out_ticket.ownership_generation = made.ownership_generation;
    out_ticket.fence_generation = made.fence_generation;

    Decision d;
    d.code = DecisionCode::RESUME_ALLOWED;
    d.reason = "resume authorised: fresh attempt supersedes preempted attempt";
    fill_context(d, rec);
    return d;
}

// ---------------------------------------------------------------------------
// Worker loss
// ---------------------------------------------------------------------------
Decision ExecutionEngine::mark_worker_lost(const ExecutionId& id, const WorkerId& worker,
                                           const WorkerBootId& boot, const CoordinatorEpoch& epoch) {
    ExecutionRecord* prec = find_mut(id);
    if (!prec) {
        Decision d;
        d.code = DecisionCode::REJECT_UNKNOWN_EXECUTION;
        d.reason = "worker_lost rejected: unknown execution";
        return d;
    }
    ExecutionRecord& rec = *prec;
    if (epoch < epoch_ || epoch < rec.coordinator_epoch) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "worker_lost rejected: stale coordinator epoch";
        fill_context(d, rec);
        return d;
    }
    if (ExecutionStateMachine::is_terminal(rec.state)) {
        Decision d;
        d.code = DecisionCode::REJECT_ALREADY_TERMINAL;
        d.reason = "worker_lost rejected: execution is terminal";
        fill_context(d, rec);
        return d;
    }
    // Only the current owner's loss is consequential.
    if (rec.owner_worker != worker || rec.owner_worker_boot != boot) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_BOOT;
        d.reason = "worker_lost rejected: this worker is not the current owner (stale boot/incarnation)";
        fill_context(d, rec);
        return d;
    }
    if (rec.state == ExecutionState::DISPATCHED) {
        // Worker died before it executed: known NOT completed -> back to READY.
        for (auto& at : rec.attempts) {
            if (at.id == rec.current_attempt_id) { transition_attempt(at, AttemptState::LOST); }
        }
        return transition(rec, ExecutionState::READY);
    }
    if (rec.state == ExecutionState::RUNNING) {
        // Worker died while running: outcome unknown -> AMBIGUOUS.
        for (auto& at : rec.attempts) {
            if (at.id == rec.current_attempt_id) { transition_attempt(at, AttemptState::LOST); }
        }
        Decision d = transition(rec, ExecutionState::AMBIGUOUS);
        fill_context(d, rec);
        return d;
    }
    if (rec.state == ExecutionState::CANCELLATION_REQUESTED) {
        // Cancellation was already in flight; worker death terminalizes it.
        for (auto& at : rec.attempts) {
            if (at.id == rec.current_attempt_id) { transition_attempt(at, AttemptState::LOST); }
        }
        return transition(rec, ExecutionState::CANCELLED);
    }
    if (rec.state == ExecutionState::PREEMPTION_REQUESTED) {
        for (auto& at : rec.attempts) {
            if (at.id == rec.current_attempt_id) { transition_attempt(at, AttemptState::LOST); }
        }
        return transition(rec, ExecutionState::PREEMPTED);
    }
    Decision d;
    d.code = DecisionCode::DEFER;
    d.reason = "worker_lost: execution is not in a state where loss is authoritative";
    fill_context(d, rec);
    return d;
}

// ---------------------------------------------------------------------------
// Supersede (logical generation)
// ---------------------------------------------------------------------------
Decision ExecutionEngine::supersede(const ExecutionId& id, const CoordinatorEpoch& epoch) {
    ExecutionRecord* prec = find_mut(id);
    if (!prec) {
        Decision d;
        d.code = DecisionCode::REJECT_UNKNOWN_EXECUTION;
        d.reason = "supersede rejected: unknown execution";
        return d;
    }
    ExecutionRecord& rec = *prec;
    if (epoch != epoch_ || epoch != rec.coordinator_epoch) {
        Decision d;
        d.code = DecisionCode::REJECT_STALE_EPOCH;
        d.reason = "supersede rejected: stale coordinator epoch";
        fill_context(d, rec);
        return d;
    }
    if (ExecutionStateMachine::is_terminal(rec.state) && rec.state != ExecutionState::AMBIGUOUS) {
        Decision d;
        d.code = DecisionCode::REJECT_ALREADY_TERMINAL;
        d.reason = "supersede rejected: execution is terminal";
        fill_context(d, rec);
        return d;
    }
    for (auto& at : rec.attempts) {
        if (at.id == rec.current_attempt_id && !AttemptStateMachine::is_terminal(at.state)) {
            transition_attempt(at, AttemptState::SUPERSEDED);
        }
    }
    rec.generation = rec.generation.next();
    rec.current_attempt_id.reset();
    rec.current_attempt_generation.reset();
    rec.owner_worker.reset();
    rec.owner_worker_boot.reset();
    return transition(rec, ExecutionState::SUPERSEDED);
}

}  // namespace execution_fabric