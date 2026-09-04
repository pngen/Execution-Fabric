#include "execution_fabric/engine.hpp"
#include "execution_fabric/persistence.hpp"
#include "execution_fabric/state.hpp"
#include "test_util.hpp"
#include <cstdio>

using namespace execution_fabric;

// Build a completion ticket from a dispatch ticket plus a result payload.
static CompletionTicket make_complete(const DispatchTicket& d, const void* payload, std::size_t n) {
    CompletionTicket t;
    t.execution_id = d.execution_id;
    t.execution_generation = d.execution_generation;
    t.attempt_id = d.attempt_id;
    t.attempt_generation = d.attempt_generation;
    t.worker_id = d.worker_id;
    t.worker_boot_id = d.worker_boot_id;
    t.epoch = d.epoch;
    t.dispatch_id = d.dispatch_id;
    t.dispatch_generation = d.dispatch_generation;
    t.ownership_generation = d.ownership_generation;
    t.fence_generation = d.fence_generation;
    t.completion_generation = CompletionGeneration(1);
    t.has_result = true;
    t.result_digest = ResultDigest::of(payload, n);
    return t;
}

int main() {
    const CoordinatorEpoch EPOCH(1);
    ExecutionEngine eng(EPOCH);

    // --- Happy path: create -> activate -> dispatch -> running -> complete -> commit.
    const ExecutionId ex = ExecutionId::random();
    CHECK_EQ(eng.create(ex, EPOCH).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.activate(ex, EPOCH).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex)->state, ExecutionState::READY);

    const WorkerId wA = WorkerId::random();
    const WorkerBootId bA = WorkerBootId::random();
    DispatchTicket dA;
    CHECK_EQ(eng.dispatch(ex, wA, bA, EPOCH, dA).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex)->state, ExecutionState::DISPATCHED);
    CHECK(dA.attempt_id == eng.find(ex)->current_attempt_id.value());

    StartTicket st;
    // Build start ticket explicitly.
    {
        StartTicket s;
        s.execution_id = dA.execution_id;
        s.execution_generation = dA.execution_generation;
        s.attempt_id = dA.attempt_id;
        s.attempt_generation = dA.attempt_generation;
        s.worker_id = dA.worker_id;
        s.worker_boot_id = dA.worker_boot_id;
        s.epoch = dA.epoch;
        s.dispatch_id = dA.dispatch_id;
        s.dispatch_generation = dA.dispatch_generation;
        s.ownership_generation = dA.ownership_generation;
        s.fence_generation = dA.fence_generation;
        st = s;
    }
    CHECK_EQ(eng.mark_running(st).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex)->state, ExecutionState::RUNNING);

    const char payload[] = "a-real-result";
    CompletionTicket cA = make_complete(dA, payload, sizeof(payload));
    Decision c = eng.complete(cA);
    CHECK_EQ(c.code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex)->state, ExecutionState::COMMITTED);
    CHECK(eng.find(ex)->committed_digest.has_value());
    const auto committed_digest = *eng.find(ex)->committed_digest;

    // --- Duplicate identical completion: idempotent, never double-commit.
    Decision dup = eng.complete(cA);
    CHECK_EQ(dup.code, DecisionCode::REJECT_ALREADY_COMMITTED);
    // Still exactly one committed record.
    CHECK_EQ(eng.find(ex)->commit_generation.value(), 1u);

    // --- Conflicting completion on the committed generation.
    const char conflict[] = "a-different-result";
    CompletionTicket cB = make_complete(dA, conflict, sizeof(conflict));
    Decision conf = eng.complete(cB);
    CHECK_EQ(conf.code, DecisionCode::REJECT_CONFLICTING_COMPLETION);
    CHECK(*eng.find(ex)->committed_digest == committed_digest);

    // --- Stale completion/start rejected.
    CompletionTicket stale = cA;
    stale.epoch = CoordinatorEpoch(0);
    CHECK_EQ(eng.complete(stale).code, DecisionCode::REJECT_STALE_EPOCH);
    stale = cA;
    stale.worker_boot_id = WorkerBootId::random();
    CHECK_EQ(eng.complete(stale).code, DecisionCode::REJECT_STALE_BOOT);

    // --- Fresh execution: retry after worker loss.
    const ExecutionId ex2 = ExecutionId::random();
    CHECK_EQ(eng.create(ex2, EPOCH).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.activate(ex2, EPOCH).code, DecisionCode::ALLOW);
    DispatchTicket a1;
    CHECK_EQ(eng.dispatch(ex2, wA, bA, EPOCH, a1).code, DecisionCode::ALLOW);
    {
        StartTicket s;
        s.execution_id = a1.execution_id; s.execution_generation = a1.execution_generation;
        s.attempt_id = a1.attempt_id; s.attempt_generation = a1.attempt_generation;
        s.worker_id = a1.worker_id; s.worker_boot_id = a1.worker_boot_id;
        s.epoch = a1.epoch; s.dispatch_id = a1.dispatch_id; s.dispatch_generation = a1.dispatch_generation;
        s.ownership_generation = a1.ownership_generation; s.fence_generation = a1.fence_generation;
        CHECK_EQ(eng.mark_running(s).code, DecisionCode::ALLOW);
    }
    // Worker A dies while running -> ambiguous.
    CHECK_EQ(eng.mark_worker_lost(ex2, wA, bA, EPOCH).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex2)->state, ExecutionState::AMBIGUOUS);
    // Retry on worker B.
    const WorkerId wB = WorkerId::random();
    const WorkerBootId bB = WorkerBootId::random();
    DispatchTicket a2;
    CHECK_EQ(eng.dispatch(ex2, wB, bB, EPOCH, a2).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex2)->state, ExecutionState::DISPATCHED);
    {
        StartTicket s; s.execution_id = a2.execution_id; s.execution_generation = a2.execution_generation;
        s.attempt_id = a2.attempt_id; s.attempt_generation = a2.attempt_generation;
        s.worker_id = a2.worker_id; s.worker_boot_id = a2.worker_boot_id;
        s.epoch = a2.epoch; s.dispatch_id = a2.dispatch_id; s.dispatch_generation = a2.dispatch_generation;
        s.ownership_generation = a2.ownership_generation; s.fence_generation = a2.fence_generation;
        CHECK_EQ(eng.mark_running(s).code, DecisionCode::ALLOW);
    }
    // Replay stale completion from attempt A (now superseded). Must be rejected.
    CompletionTicket staleA = make_complete(a1, payload, sizeof(payload));
    staleA.attempt_id = a1.attempt_id; staleA.attempt_generation = a1.attempt_generation;
    staleA.worker_id = a1.worker_id; staleA.worker_boot_id = a1.worker_boot_id;
    staleA.dispatch_id = a1.dispatch_id; staleA.ownership_generation = a1.ownership_generation;
    staleA.fence_generation = a1.fence_generation;
    CHECK_EQ(eng.complete(staleA).code, DecisionCode::REJECT_STALE_ATTEMPT);
    // Fresh completion from attempt B commits.
    const char payload2[] = "b-result";
    CompletionTicket cA2 = make_complete(a2, payload2, sizeof(payload2));
    CHECK_EQ(eng.complete(cA2).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex2)->state, ExecutionState::COMMITTED);
    // Replaying the stale attempt A completion again must never commit.
    CHECK_EQ(eng.complete(staleA).code, DecisionCode::REJECT_STALE_ATTEMPT);

    // --- Cancellation before dispatch (from CREATED).
    const ExecutionId ex3 = ExecutionId::random();
    CHECK_EQ(eng.create(ex3, EPOCH).code, DecisionCode::ALLOW);
    ControlRequest cancelReq; cancelReq.execution_id = ex3; cancelReq.epoch = EPOCH;
    cancelReq.authority_generation = OwnershipGeneration(0);  // CREATED has no ownership yet
    // From CREATED, cancel requires generation 0 matches; use a cancel path.
    Decision cn = eng.cancel(cancelReq);
    CHECK_EQ(cn.code, DecisionCode::ALLOW);
    // Wait: cancel req with authority_generation 0 but record ownership is 0 in CREATED -> should work.
    CHECK_EQ(eng.find(ex3)->state, ExecutionState::CANCELLED);

    // --- Cancellation while running vs completion race (deterministic).
    const ExecutionId ex4 = ExecutionId::random();
    CHECK_EQ(eng.create(ex4, EPOCH).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.activate(ex4, EPOCH).code, DecisionCode::ALLOW);
    DispatchTicket a4;
    CHECK_EQ(eng.dispatch(ex4, wA, bA, EPOCH, a4).code, DecisionCode::ALLOW);
    {
        StartTicket s; s.execution_id = a4.execution_id; s.execution_generation = a4.execution_generation;
        s.attempt_id = a4.attempt_id; s.attempt_generation = a4.attempt_generation;
        s.worker_id = a4.worker_id; s.worker_boot_id = a4.worker_boot_id;
        s.epoch = a4.epoch; s.dispatch_id = a4.dispatch_id; s.dispatch_generation = a4.dispatch_generation;
        s.ownership_generation = a4.ownership_generation; s.fence_generation = a4.fence_generation;
        CHECK_EQ(eng.mark_running(s).code, DecisionCode::ALLOW);
    }
    ControlRequest cReq; cReq.execution_id = ex4; cReq.epoch = EPOCH;
    cReq.authority_generation = eng.find(ex4)->ownership_generation;
    CHECK_EQ(eng.cancel(cReq).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex4)->state, ExecutionState::CANCELLATION_REQUESTED);
    // Completion arrives before cancellation is terminalized: completion wins.
    CompletionTicket c4 = make_complete(a4, payload, sizeof(payload));
    CHECK_EQ(eng.complete(c4).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex4)->state, ExecutionState::COMMITTED);

    // --- Cancellation terminalized then late completion rejected.
    const ExecutionId ex5 = ExecutionId::random();
    CHECK_EQ(eng.create(ex5, EPOCH).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.activate(ex5, EPOCH).code, DecisionCode::ALLOW);
    DispatchTicket a5;
    CHECK_EQ(eng.dispatch(ex5, wA, bA, EPOCH, a5).code, DecisionCode::ALLOW);
    {
        StartTicket s; s.execution_id = a5.execution_id; s.execution_generation = a5.execution_generation;
        s.attempt_id = a5.attempt_id; s.attempt_generation = a5.attempt_generation;
        s.worker_id = a5.worker_id; s.worker_boot_id = a5.worker_boot_id;
        s.epoch = a5.epoch; s.dispatch_id = a5.dispatch_id; s.dispatch_generation = a5.dispatch_generation;
        s.ownership_generation = a5.ownership_generation; s.fence_generation = a5.fence_generation;
        CHECK_EQ(eng.mark_running(s).code, DecisionCode::ALLOW);
    }
    ControlRequest c5; c5.execution_id = ex5; c5.epoch = EPOCH;
    c5.authority_generation = eng.find(ex5)->ownership_generation;
    CHECK_EQ(eng.cancel(c5).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.finalize_cancel(ex5, EPOCH).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex5)->state, ExecutionState::CANCELLED);
    CompletionTicket late = make_complete(a5, payload, sizeof(payload));
    CHECK_EQ(eng.complete(late).code, DecisionCode::REJECT_CANCELLED);

    // --- Preemption then resume under a fresh attempt.
    const ExecutionId ex6 = ExecutionId::random();
    CHECK_EQ(eng.create(ex6, EPOCH).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.activate(ex6, EPOCH).code, DecisionCode::ALLOW);
    DispatchTicket a6;
    CHECK_EQ(eng.dispatch(ex6, wA, bA, EPOCH, a6).code, DecisionCode::ALLOW);
    {
        StartTicket s; s.execution_id = a6.execution_id; s.execution_generation = a6.execution_generation;
        s.attempt_id = a6.attempt_id; s.attempt_generation = a6.attempt_generation;
        s.worker_id = a6.worker_id; s.worker_boot_id = a6.worker_boot_id;
        s.epoch = a6.epoch; s.dispatch_id = a6.dispatch_id; s.dispatch_generation = a6.dispatch_generation;
        s.ownership_generation = a6.ownership_generation; s.fence_generation = a6.fence_generation;
        CHECK_EQ(eng.mark_running(s).code, DecisionCode::ALLOW);
    }
    ControlRequest p6; p6.execution_id = ex6; p6.epoch = EPOCH;
    p6.authority_generation = eng.find(ex6)->ownership_generation;
    CHECK_EQ(eng.preempt(p6).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex6)->state, ExecutionState::PREEMPTION_REQUESTED);
    CHECK_EQ(eng.acknowledge_preempt(ex6, EPOCH).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex6)->state, ExecutionState::PREEMPTED);
    CHECK_EQ(eng.mark_resumable(ex6, EPOCH).code, DecisionCode::ALLOW);
    // Resume with a new worker/boot under a fresh attempt.
    const WorkerId wC = WorkerId::random();
    const WorkerBootId bC = WorkerBootId::random();
    DispatchTicket a6r;
    ResumeGeneration rg = eng.find(ex6)->resume_generation.next();
    CHECK_EQ(eng.resume(ex6, wC, bC, EPOCH, rg, a6r).code, DecisionCode::RESUME_ALLOWED);
    CHECK_EQ(eng.find(ex6)->state, ExecutionState::RESUMING);
    {
        StartTicket s; s.execution_id = a6r.execution_id; s.execution_generation = a6r.execution_generation;
        s.attempt_id = a6r.attempt_id; s.attempt_generation = a6r.attempt_generation;
        s.worker_id = a6r.worker_id; s.worker_boot_id = a6r.worker_boot_id;
        s.epoch = a6r.epoch; s.dispatch_id = a6r.dispatch_id; s.dispatch_generation = a6r.dispatch_generation;
        s.ownership_generation = a6r.ownership_generation; s.fence_generation = a6r.fence_generation;
        CHECK_EQ(eng.mark_running(s).code, DecisionCode::ALLOW);
    }
    // The old preempted attempt must never regain authority.
    CompletionTicket stalePrev = make_complete(a6, payload2, sizeof(payload2));
    stalePrev.attempt_id = a6.attempt_id; stalePrev.attempt_generation = a6.attempt_generation;
    stalePrev.worker_id = a6.worker_id; stalePrev.worker_boot_id = a6.worker_boot_id;
    stalePrev.dispatch_id = a6.dispatch_id; stalePrev.ownership_generation = a6.ownership_generation;
    stalePrev.fence_generation = a6.fence_generation;
    CHECK_EQ(eng.complete(stalePrev).code, DecisionCode::REJECT_STALE_ATTEMPT);
    CompletionTicket c6 = make_complete(a6r, payload2, sizeof(payload2));
    CHECK_EQ(eng.complete(c6).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.find(ex6)->state, ExecutionState::COMMITTED);

    // --- Lifecycle guards: operations on terminal state rejected.
    CHECK_EQ(eng.dispatch(ex, wA, bA, EPOCH, dA).code, DecisionCode::REJECT_ALREADY_COMMITTED);
    ControlRequest termReq; termReq.execution_id = ex; termReq.epoch = EPOCH;
    termReq.authority_generation = eng.find(ex)->ownership_generation;
    CHECK_EQ(eng.cancel(termReq).code, DecisionCode::REJECT_ALREADY_TERMINAL);

    // Unknown execution.
    CompletionTicket unk = make_complete(dA, payload, sizeof(payload));
    unk.execution_id = ExecutionId::random();
    CHECK_EQ(eng.complete(unk).code, DecisionCode::REJECT_UNKNOWN_EXECUTION);

    std::printf("engine_test: %d checks, %d failures\n", eftest::checks, eftest::failures);
    return TEST_MAIN_RETURN();
}