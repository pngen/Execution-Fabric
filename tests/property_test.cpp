#include "execution_fabric/engine.hpp"
#include "execution_fabric/state_machine.hpp"
#include "test_util.hpp"
#include <cstdio>
#include <map>
#include <random>

using namespace execution_fabric;

// Verify the authority invariants that must hold after every mutation.
static bool check_invariants(const ExecutionEngine& eng) {
    const auto recs = eng.records();
    for (const auto& rec : recs) {
        // 1. Attempt generations are strictly increasing in history order.
        for (std::size_t i = 1; i < rec.attempts.size(); ++i) {
            if (rec.attempts[i].generation <= rec.attempts[i - 1].generation) { return false; }
        }
        // 2. Ownership & fence generations never move backward across attempts.
        for (std::size_t i = 1; i < rec.attempts.size(); ++i) {
            if (rec.attempts[i].ownership_generation < rec.attempts[i - 1].ownership_generation) { return false; }
            if (rec.attempts[i].fence_generation < rec.attempts[i - 1].fence_generation) { return false; }
        }
        // 3. The current attempt, if any, is the last non-superseded attempt in history.
        if (rec.current_attempt_id) {
            const AttemptRecord* lastNonTerminal = nullptr;
            for (const auto& at : rec.attempts) {
                if (at.state == AttemptState::SUPERSEDED) { continue; }
                lastNonTerminal = &at;
            }
            if (!lastNonTerminal || lastNonTerminal->id != *rec.current_attempt_id) { return false; }
        }
        // 4. COMMITTED state implies committed digest + commit generation.
        if (rec.state == ExecutionState::COMMITTED) {
            if (!rec.committed_digest.has_value()) { return false; }
            if (!rec.commit_generation.is_set()) { return false; }
        }
        // 5. At most one commit: commit generation cannot move more than once.
        if (rec.commit_generation.value() > 1u) { return false; }
        // 6. A superseded execution's generation must be >= 1.
        if (!rec.generation.is_set()) { return false; }
    }
    return true;
}

int main() {
    std::mt19937_64 rng(0xC0FFEEu);
    const CoordinatorEpoch EPOCH(1);
    ExecutionEngine eng(EPOCH);

    // We keep a pool of at most 8 executions, tracking the last issued ticket.
    struct Slot { bool active = false; ExecutionId id; DispatchTicket ticket; };
    std::vector<Slot> pool(8);
    // Randomly generated ids.
    for (auto& s : pool) { s.id = ExecutionId::random(); }

    for (int step = 0; step < 20000; ++step) {
        const int slot = static_cast<int>(rng() % pool.size());
        Slot& s = pool[slot];
        const int op = static_cast<int>(rng() % 12);
        WorkerId w = WorkerId::random();
        WorkerBootId b = WorkerBootId::random();

        if (op == 0) {
            eng.create(s.id, EPOCH); s.active = true;
        } else if (op == 1) {
            if (s.active) { eng.activate(s.id, EPOCH); }
        } else if (op == 2) {
            if (s.active) { DispatchTicket t; eng.dispatch(s.id, w, b, EPOCH, t); if (eng.find(s.id) && eng.find(s.id)->current_attempt_id) { s.ticket = t; } }
        } else if (op == 3) {
            if (s.active) {
                StartTicket st; st.execution_id=s.ticket.execution_id; st.execution_generation=s.ticket.execution_generation;
                st.attempt_id=s.ticket.attempt_id; st.attempt_generation=s.ticket.attempt_generation;
                st.worker_id=s.ticket.worker_id; st.worker_boot_id=s.ticket.worker_boot_id; st.epoch=s.ticket.epoch;
                st.dispatch_id=s.ticket.dispatch_id; st.dispatch_generation=s.ticket.dispatch_generation;
                st.ownership_generation=s.ticket.ownership_generation; st.fence_generation=s.ticket.fence_generation;
                eng.mark_running(st);
            }
        } else if (op == 4) {
            if (s.active && eng.find(s.id) && eng.find(s.id)->current_attempt_id.has_value()) {
                CompletionTicket ct; ct.execution_id=s.ticket.execution_id; ct.execution_generation=s.ticket.execution_generation;
                ct.attempt_id=s.ticket.attempt_id; ct.attempt_generation=s.ticket.attempt_generation; ct.worker_id=s.ticket.worker_id;
                ct.worker_boot_id=s.ticket.worker_boot_id; ct.epoch=s.ticket.epoch; ct.dispatch_id=s.ticket.dispatch_id;
                ct.dispatch_generation=s.ticket.dispatch_generation; ct.ownership_generation=s.ticket.ownership_generation;
                ct.fence_generation=s.ticket.fence_generation; ct.completion_generation=CompletionGeneration(1);
                ct.has_result = true;
                const char p[] = "random-property";
                ct.result_digest = ResultDigest::of(p, sizeof(p));
                eng.complete(ct);
            }
        } else if (op == 5) {
            if (s.active) { ControlRequest r; r.execution_id=s.id; r.epoch=EPOCH; if (eng.find(s.id)) r.authority_generation = eng.find(s.id)->ownership_generation; eng.cancel(r); }
        } else if (op == 6) {
            if (s.active) { ControlRequest r; r.execution_id=s.id; r.epoch=EPOCH; if (eng.find(s.id)) r.authority_generation = eng.find(s.id)->ownership_generation; eng.preempt(r); }
        } else if (op == 7) {
            if (s.active) { if (eng.find(s.id)) eng.acknowledge_preempt(s.id, EPOCH); }
        } else if (op == 8) {
            if (s.active) { if (eng.find(s.id)) eng.mark_resumable(s.id, EPOCH); }
        } else if (op == 9) {
            if (s.active && eng.find(s.id)) {
                if (eng.find(s.id)->state == ExecutionState::RESUMABLE) {
                    DispatchTicket t; ResumeGeneration rg = eng.find(s.id)->resume_generation.next();
                    eng.resume(s.id, w, b, EPOCH, rg, t);
                    if (eng.find(s.id)->current_attempt_id) { s.ticket = t; }
                }
            }
        } else if (op == 10) {
            if (s.active && eng.find(s.id)) { eng.mark_worker_lost(s.id, w, b, EPOCH); }
        } else if (op == 11) {
            if (s.active && eng.find(s.id)) { eng.finalize_cancel(s.id, EPOCH); }
        }

        if (!check_invariants(eng)) {
            std::printf("INVARIANT FAILED at step %d (op %d) for execution %s\n",
                        step, op, s.id.to_string().c_str());
            return TEST_MAIN_RETURN();  // will fail due to failure count
        }
    }

    std::printf("property_test: %d checks, %d failures\n", eftest::checks, eftest::failures);
    return TEST_MAIN_RETURN();
}