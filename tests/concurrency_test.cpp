#include "execution_fabric/engine.hpp"
#include "execution_fabric/state_machine.hpp"
#include "test_util.hpp"
#include <atomic>
#include <cstdio>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

using namespace execution_fabric;

// The coordinator serialises all authority decisions behind a single decision
// thread. Here we emulate that: many threads genuinely race to submit
// authority operations, but a mutex serialises the engine exactly as the
// coordinator's single decision thread does. The assertion we test is the
// invariant the whole runtime rests on: with real concurrent producers,
// still at most one authoritative commit per logical execution, and no
// state corruption.
static bool check(const ExecutionEngine& eng) {
    for (const auto& rec : eng.records()) {
        if (rec.state == ExecutionState::COMMITTED) {
            if (!rec.committed_digest.has_value()) { return false; }
            if (rec.commit_generation.value() != 1u) { return false; }
        }
        if (rec.commit_generation.value() > 1u) { return false; }
    }
    return true;
}

int main() {
    const CoordinatorEpoch EPOCH(1);
    ExecutionEngine eng(EPOCH);
    std::mutex mu;
    std::atomic<int> committed{0};
    std::atomic<bool> failed{false};

    const int num_threads = 8;
    const int execs_per_thread = 40;
    std::vector<std::thread> threads;
    std::vector<std::atomic<int>> per_thread_commit(num_threads);

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < execs_per_thread; ++i) {
                const ExecutionId ex = ExecutionId::random();
                const WorkerId w = WorkerId::random();
                const WorkerBootId b = WorkerBootId::random();

                std::unique_lock<std::mutex> lk(mu);
                auto dec = eng.create(ex, EPOCH);
                if (dec.code != DecisionCode::ALLOW) { failed = true; return; }
                dec = eng.activate(ex, EPOCH);
                DispatchTicket d;
                dec = eng.dispatch(ex, w, b, EPOCH, d);
                if (dec.code != DecisionCode::ALLOW) { failed = true; return; }
                StartTicket s; s.execution_id=d.execution_id; s.execution_generation=d.execution_generation;
                s.attempt_id=d.attempt_id; s.attempt_generation=d.attempt_generation; s.worker_id=d.worker_id;
                s.worker_boot_id=d.worker_boot_id; s.epoch=d.epoch; s.dispatch_id=d.dispatch_id;
                s.dispatch_generation=d.dispatch_generation; s.ownership_generation=d.ownership_generation;
                s.fence_generation=d.fence_generation;
                dec = eng.mark_running(s);
                if (dec.code != DecisionCode::ALLOW) { failed = true; return; }
                CompletionTicket c; c.execution_id=d.execution_id; c.execution_generation=d.execution_generation;
                c.attempt_id=d.attempt_id; c.attempt_generation=d.attempt_generation; c.worker_id=d.worker_id;
                c.worker_boot_id=d.worker_boot_id; c.epoch=d.epoch; c.dispatch_id=d.dispatch_id;
                c.dispatch_generation=d.dispatch_generation; c.ownership_generation=d.ownership_generation;
                c.fence_generation=d.fence_generation; c.completion_generation=CompletionGeneration(1);
                c.has_result = true;
                const std::uint8_t pd[16] = {(std::uint8_t)t, (std::uint8_t)i};
                c.result_digest = ResultDigest::of(pd, sizeof(pd));
                dec = eng.complete(c);
                if (dec.code != DecisionCode::ALLOW) { failed = true; return; }
                ++per_thread_commit[t];
                if (!check(eng)) { failed = true; return; }
                lk.unlock();
                // Also verify no double commit by re-issuing the same completion
                // without the lock held (still serialised).
                lk.lock();
                auto dup = eng.complete(c);
                if (dup.code != DecisionCode::REJECT_ALREADY_COMMITTED) { failed = true; return; }
            }
        });
    }
    for (auto& th : threads) { th.join(); }

    CHECK(!failed.load());
    int total = 0;
    for (int t = 0; t < num_threads; ++t) { total += per_thread_commit[t].load(); }
    CHECK_EQ(total, num_threads * execs_per_thread);
    CHECK_EQ(eng.execution_count(), static_cast<std::size_t>(total));
    CHECK(check(eng));

    std::printf("concurrency_test: %d checks, %d failures\n", eftest::checks, eftest::failures);
    return TEST_MAIN_RETURN();
}