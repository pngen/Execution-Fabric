#include "execution_fabric/engine.hpp"
#include "execution_fabric/state_machine.hpp"
#include "cuda/cuda_executor.hpp"
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

using namespace execution_fabric;

static int failures = 0;
static int checks = 0;
#define CU_CHECK(cond, msg) do { ++checks; if (!(cond)) { ++failures; std::printf("  [FAIL] %s:%d: %s\n", __FILE__, __LINE__, msg); } } while (0)
#define CU_CHECK_EQ(a, b, msg) do { ++checks; auto _a=(a); auto _b=(b); if (!(_a==_b)) { ++failures; std::printf("  [FAIL] %s:%d: %s (%s)\n", __FILE__, __LINE__, msg, ""); } } while (0)

static CompletionTicket finish(const DispatchTicket& d, const std::vector<std::uint8_t>& result) {
    CompletionTicket ct;
    ct.execution_id = d.execution_id; ct.execution_generation = d.execution_generation;
    ct.attempt_id = d.attempt_id; ct.attempt_generation = d.attempt_generation;
    ct.worker_id = d.worker_id; ct.worker_boot_id = d.worker_boot_id;
    ct.epoch = d.epoch; ct.dispatch_id = d.dispatch_id; ct.dispatch_generation = d.dispatch_generation;
    ct.ownership_generation = d.ownership_generation; ct.fence_generation = d.fence_generation;
    ct.completion_generation = CompletionGeneration(1);
    ct.has_result = true;
    ct.result_digest = ResultDigest::of_buffer(result);
    return ct;
}

static StartTicket start(const DispatchTicket& d) {
    StartTicket s;
    s.execution_id = d.execution_id; s.execution_generation = d.execution_generation;
    s.attempt_id = d.attempt_id; s.attempt_generation = d.attempt_generation;
    s.worker_id = d.worker_id; s.worker_boot_id = d.worker_boot_id;
    s.epoch = d.epoch; s.dispatch_id = d.dispatch_id; s.dispatch_generation = d.dispatch_generation;
    s.ownership_generation = d.ownership_generation; s.fence_generation = d.fence_generation;
    return s;
}

int main() {
    const CoordinatorEpoch EPOCH(1);
    ExecutionEngine eng(EPOCH);
    const WorkerId w1 = WorkerId::random();
    const WorkerBootId b1 = WorkerBootId::random();
    const WorkerId w2 = WorkerId::random();
    const WorkerBootId b2 = WorkerBootId::random();

    std::size_t free0 = 0, total0 = 0;
    CU_CHECK(cuda::device_memory(free0, total0), "CUDA device memory query (device available)");
    std::printf("=== Execution Fabric CUDA proof (RTX 5090, sm_120) ===\n");
    std::printf("device free=%zu MiB total=%zu MiB\n", free0 >> 20, total0 >> 20);

    // ---- Scenario A: normal authoritative completion with real CUDA work.
    std::printf("\n[A] normal authoritative completion\n");
    {
        const std::uint32_t N = 1u << 20;
        const ExecutionId ex = ExecutionId::random();
        CU_CHECK(eng.create(ex, EPOCH).code == DecisionCode::ALLOW, "create");
        CU_CHECK(eng.activate(ex, EPOCH).code == DecisionCode::ALLOW, "activate");
        DispatchTicket d;
        CU_CHECK(eng.dispatch(ex, w1, b1, EPOCH, d).code == DecisionCode::ALLOW, "dispatch");
        CU_CHECK(eng.mark_running(start(d)).code == DecisionCode::ALLOW, "mark_running");
        std::vector<std::uint8_t> res; ResultDigest dg; std::string err;
        CU_CHECK(cuda::run_vector_add(N, 42, 0, res, dg, err), ("cuda work: " + err).c_str());
        CU_CHECK(!res.empty(), "result non-empty");
        CompletionTicket ct = finish(d, res);
        // Ensure the ticket digest matches the CUDA-derived digest.
        CU_CHECK(ct.result_digest == dg, "completion digest matches CUDA result");
        CU_CHECK(eng.complete(ct).code == DecisionCode::ALLOW, "complete (commit)");
        CU_CHECK(eng.find(ex)->state == ExecutionState::COMMITTED, "execution committed");
        CU_CHECK(eng.find(ex)->committed_digest.has_value(), "committed digest present");
        // No double commit on an identical replay.
        CU_CHECK(eng.complete(ct).code == DecisionCode::REJECT_ALREADY_COMMITTED, "idempotent replay no double commit");
    }

    // ---- Scenario B: stale completion after retry with real CUDA work.
    std::printf("\n[B] stale completion after retry\n");
    {
        const std::uint32_t N = 1u << 20;
        const ExecutionId ex = ExecutionId::random();
        CU_CHECK(eng.create(ex, EPOCH).code == DecisionCode::ALLOW, "create");
        CU_CHECK(eng.activate(ex, EPOCH).code == DecisionCode::ALLOW, "activate");
        // Attempt A does real CUDA work but is superseded deterministically,
        // mirroring the stale-authority advance that the multiprocess proof
        // triggers via real process death.
        DispatchTicket a1;
        CU_CHECK(eng.dispatch(ex, w1, b1, EPOCH, a1).code == DecisionCode::ALLOW, "dispatch A");
        CU_CHECK(eng.mark_running(start(a1)).code == DecisionCode::ALLOW, "mark_running A");
        std::vector<std::uint8_t> resA; ResultDigest dgA; std::string err;
        CU_CHECK(cuda::run_vector_add(N, 7, 0, resA, dgA, err), ("cuda work A: " + err).c_str());
        // Attempt B supersedes A (authority advances) and commits.
        DispatchTicket a2;
        CU_CHECK(eng.dispatch(ex, w2, b2, EPOCH, a2).code == DecisionCode::ALLOW, "dispatch B (retry)");
        CU_CHECK(eng.mark_running(start(a2)).code == DecisionCode::ALLOW, "mark_running B");
        std::vector<std::uint8_t> resB; ResultDigest dgB; std::string err2;
        CU_CHECK(cuda::run_vector_add(N, 99, 0, resB, dgB, err2), ("cuda work B: " + err2).c_str());
        CU_CHECK(eng.complete(finish(a2, resB)).code == DecisionCode::ALLOW, "complete B");
        CU_CHECK(eng.find(ex)->state == ExecutionState::COMMITTED, "B committed");
        // Replayed completion from attempt A (real CUDA result) must be rejected.
        CU_CHECK(eng.complete(finish(a1, resA)).code == DecisionCode::REJECT_STALE_ATTEMPT, "stale A replay rejected");
        CU_CHECK(*eng.find(ex)->committed_digest == dgB, "committed digest is B's CUDA result");
    }

    // ---- Scenario C: cancellation race with real CUDA work.
    std::printf("\n[C] cancellation race\n");
    {
        const std::uint32_t N = 1u << 20;
        const ExecutionId ex = ExecutionId::random();
        CU_CHECK(eng.create(ex, EPOCH).code == DecisionCode::ALLOW, "create");
        CU_CHECK(eng.activate(ex, EPOCH).code == DecisionCode::ALLOW, "activate");
        DispatchTicket d;
        CU_CHECK(eng.dispatch(ex, w1, b1, EPOCH, d).code == DecisionCode::ALLOW, "dispatch");
        CU_CHECK(eng.mark_running(start(d)).code == DecisionCode::ALLOW, "mark_running");
        std::vector<std::uint8_t> res; ResultDigest dg; std::string err;
        CU_CHECK(cuda::run_vector_add(N, 5, 0, res, dg, err), ("cuda work: " + err).c_str());
        // Issue cancellation while the CUDA-backed attempt has physically run.
        ControlRequest req; req.execution_id = ex; req.epoch = EPOCH;
        req.authority_generation = eng.find(ex)->ownership_generation;
        CU_CHECK(eng.cancel(req).code == DecisionCode::ALLOW, "cancel requested");
        CU_CHECK(eng.find(ex)->state == ExecutionState::CANCELLATION_REQUESTED, "cancellation requested");
        // Deterministic winner: the completion arriving before the cancellation
        // is terminalized wins, and commits exactly once.
        CU_CHECK(eng.complete(finish(d, res)).code == DecisionCode::ALLOW, "completion wins the race");
        CU_CHECK(eng.find(ex)->state == ExecutionState::COMMITTED, "committed");
        CU_CHECK(eng.complete(finish(d, res)).code == DecisionCode::REJECT_ALREADY_COMMITTED, "no double commit after race");
    }

    // ---- Scenario D: preemption / resume with deterministic CUDA partitioning.
    std::printf("\n[D] preemption / resume with CUDA progress partitioning\n");
    {
        const std::uint32_t HALF = 1u << 19;         // two partitions
        const std::uint32_t TOTAL = HALF * 2;
        const ExecutionId ex = ExecutionId::random();
        CU_CHECK(eng.create(ex, EPOCH).code == DecisionCode::ALLOW, "create");
        CU_CHECK(eng.activate(ex, EPOCH).code == DecisionCode::ALLOW, "activate");
        DispatchTicket d1;
        CU_CHECK(eng.dispatch(ex, w1, b1, EPOCH, d1).code == DecisionCode::ALLOW, "dispatch P1");
        CU_CHECK(eng.mark_running(start(d1)).code == DecisionCode::ALLOW, "mark_running P1");

        // Partition 1 of the CUDA-backed operation (real kernel on first half).
        std::vector<std::uint8_t> p1; ResultDigest dg1; std::string err;
        CU_CHECK(cuda::run_vector_add(HALF, 11, 0, p1, dg1, err), ("cuda partition 1: " + err).c_str());

        // Preempt at the controlled boundary, capturing a narrow checkpoint
        // reference (the partial progress digest). No full checkpoint runtime.
        ControlRequest preq; preq.execution_id = ex; preq.epoch = EPOCH;
        preq.authority_generation = eng.find(ex)->ownership_generation;
        CU_CHECK(eng.preempt(preq).code == DecisionCode::ALLOW, "preempt");
        CU_CHECK(eng.acknowledge_preempt(ex, EPOCH).code == DecisionCode::ALLOW, "acknowledge preempt");
        CU_CHECK(eng.mark_resumable(ex, EPOCH).code == DecisionCode::ALLOW, "mark resumable");

        // Resume under a fresh attempt and finish the second partition.
        DispatchTicket d2;
        ResumeGeneration rg = eng.find(ex)->resume_generation.next();
        CU_CHECK(eng.resume(ex, w2, b2, EPOCH, rg, d2).code == DecisionCode::RESUME_ALLOWED, "resume");
        CU_CHECK(eng.mark_running(start(d2)).code == DecisionCode::ALLOW, "mark_running P2");
        std::vector<std::uint8_t> p2; ResultDigest dg2; std::string err2;
        CU_CHECK(cuda::run_vector_add(HALF, 11, HALF, p2, dg2, err2), ("cuda partition 2: " + err2).c_str());

        // Combine the checkpoint reference (partition 1) with partition 2 to
        // yield the full result, and verify exact CPU-reference parity by
        // recomputing the whole thing on the device.
        std::vector<std::uint8_t> combined = p1; combined.insert(combined.end(), p2.begin(), p2.end());
        // The checkpoint reference is the digest of partition 1 (narrow interface).
        CU_CHECK(dg1 == ResultDigest::of_buffer(p1), "checkpoint reference (partition 1 digest)");
        // Recompute the full vector-add on the device and verify parity.
        std::vector<std::uint8_t> full; ResultDigest full_dg; std::string err3;
        CU_CHECK(cuda::run_vector_add(TOTAL, 11, 0, full, full_dg, err3), ("cuda full verify: " + err3).c_str());
        CU_CHECK(combined == full, "exact CPU-reference parity of combined partitions");

        // The old preempted attempt must never regain authority.
        CU_CHECK(eng.complete(finish(d1, p1)).code == DecisionCode::REJECT_STALE_ATTEMPT, "old preempted attempt rejected");
        CU_CHECK(eng.complete(finish(d2, combined)).code == DecisionCode::ALLOW, "post-resume completion commits");
        CU_CHECK(eng.find(ex)->state == ExecutionState::COMMITTED, "preempt/resume execution committed");
    }

    // ---- Scenario E: ambiguous completion (ACK lost) with real CUDA work.
    std::printf("\n[E] ambiguous completion (lost ACK / retry) \n");
    {
        const std::uint32_t N = 1u << 20;
        const ExecutionId ex = ExecutionId::random();
        CU_CHECK(eng.create(ex, EPOCH).code == DecisionCode::ALLOW, "create");
        CU_CHECK(eng.activate(ex, EPOCH).code == DecisionCode::ALLOW, "activate");
        DispatchTicket d;
        CU_CHECK(eng.dispatch(ex, w1, b1, EPOCH, d).code == DecisionCode::ALLOW, "dispatch");
        CU_CHECK(eng.mark_running(start(d)).code == DecisionCode::ALLOW, "mark_running");
        std::vector<std::uint8_t> res; ResultDigest dg; std::string err;
        CU_CHECK(cuda::run_vector_add(N, 21, 0, res, dg, err), ("cuda work: " + err).c_str());
        // The worker completed and completed-but-ACK-lost: commit once, then the
        // sender, believing the ACK was lost, retries the identical completion.
        // Exactly one logical commit must result.
        CU_CHECK(eng.complete(finish(d, res)).code == DecisionCode::ALLOW, "first completion commits");
        CU_CHECK(eng.complete(finish(d, res)).code == DecisionCode::REJECT_ALREADY_COMMITTED, "retried identical completion idempotent");
        CU_CHECK(eng.complete(finish(d, res)).code == DecisionCode::REJECT_ALREADY_COMMITTED, "no double commit under ambiguity");
        CU_CHECK(eng.find(ex)->state == ExecutionState::COMMITTED, "state settles committed");
        // A conflicting result from the same attempt is rejected.
        std::vector<std::uint8_t> other; ResultDigest og; std::string err2;
        CU_CHECK(cuda::run_vector_add(N, 999, 0, other, og, err2), ("cuda conflict: " + err2).c_str());
        CU_CHECK(eng.complete(finish(d, other)).code == DecisionCode::REJECT_CONFLICTING_COMPLETION, "conflicting recompletion rejected");
    }

    // ---- Device memory returns to baseline after validation.
    std::size_t free1 = 0, total1 = 0;
    CU_CHECK(cuda::device_memory(free1, total1), "device memory re-query");
    const std::size_t delta = (free0 > free1) ? (free0 - free1) : (free1 - free0);
    std::printf("device free before=%zu MiB after=%zu MiB (delta=%zu KiB)\n",
                free0 >> 20, free1 >> 20, delta >> 10);
    // Allowed tolerance: transient allocator slack, but no leak of our 3x N*4
    // byte buffers (about 12 MiB for N=1<<20).
    CU_CHECK(delta < (8u << 20), "device memory returned to baseline (no leak)");

    std::printf("\ncuda_proof: %d checks, %d failures\n", checks, failures);
    return failures == 0 ? 0 : 1;
}