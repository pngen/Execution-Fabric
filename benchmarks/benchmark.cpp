// benchmark.cpp
//
// Honest micro-benchmarks of the runtime's authority primitives, with enough
// workload detail to interpret the rates. We measure what the runtime actually
// does (identity + generation allocation, authority validation, logical commit)
// rather than vanity totals.
#include "execution_fabric/engine.hpp"
#include "execution_fabric/persistence.hpp"
#include "protocol/protocol.hpp"
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace execution_fabric;

namespace {
using Clock = std::chrono::steady_clock;
struct Stats { double ops; double secs; };
Stats run(std::size_t iterations, const std::function<void(std::size_t)>& body) {
    const auto t0 = Clock::now();
    body(iterations);
    const auto t1 = Clock::now();
    const double secs = std::chrono::duration<double>(t1 - t0).count();
    return {static_cast<double>(iterations), secs};
}
void report(const char* name, const Stats& s) {
    std::printf("%-42s %10.0f ops   %8.3f s   %12.0f ops/s\n",
                name, s.ops, s.secs, s.ops / (s.secs > 0 ? s.secs : 1e-9));
}
}  // namespace

static StartTicket start(const DispatchTicket& d) {
    StartTicket s;
    s.execution_id=d.execution_id; s.execution_generation=d.execution_generation;
    s.attempt_id=d.attempt_id; s.attempt_generation=d.attempt_generation;
    s.worker_id=d.worker_id; s.worker_boot_id=d.worker_boot_id;
    s.epoch=d.epoch; s.dispatch_id=d.dispatch_id; s.dispatch_generation=d.dispatch_generation;
    s.ownership_generation=d.ownership_generation; s.fence_generation=d.fence_generation;
    return s;
}
static CompletionTicket finish(const DispatchTicket& d, const char* p) {
    CompletionTicket ct;
    ct.execution_id=d.execution_id; ct.execution_generation=d.execution_generation;
    ct.attempt_id=d.attempt_id; ct.attempt_generation=d.attempt_generation;
    ct.worker_id=d.worker_id; ct.worker_boot_id=d.worker_boot_id;
    ct.epoch=d.epoch; ct.dispatch_id=d.dispatch_id; ct.dispatch_generation=d.dispatch_generation;
    ct.ownership_generation=d.ownership_generation; ct.fence_generation=d.fence_generation;
    ct.completion_generation=CompletionGeneration(1); ct.has_result=true;
    ct.result_digest=ResultDigest::of(p, std::strlen(p));
    return ct;
}

int main() {
    const std::size_t N = 100000;
    const CoordinatorEpoch EPOCH(1);
    const WorkerId w = WorkerId::random();
    const WorkerBootId b = WorkerBootId::random();

    std::printf("Execution Fabric benchmarks\n");
    std::printf("  (single decision thread; N=%zu executions; RTT=in-memory; protocol over loopback not measured here)\n\n", N);

    // Pre-build execution ids to avoid RNG dominating the timing.
    std::vector<ExecutionId> ids(N);
    for (auto& id : ids) { id = ExecutionId::random(); }

    // Execution creation.
    {
        ExecutionEngine eng(EPOCH);
        report("execution create (identity + record)", run(N, [&](std::size_t n){
            for (std::size_t i = 0; i < n; ++i) { eng.create(ids[i], EPOCH); }
        }));
    }
    // Attempt dispatch (authority advance + ownership transfer).
    {
        ExecutionEngine eng(EPOCH);
        for (std::size_t i = 0; i < N; ++i) { eng.create(ids[i], EPOCH); eng.activate(ids[i], EPOCH); }
        report("attempt dispatch (ownership transfer)", run(N, [&](std::size_t n){
            DispatchTicket d;
            for (std::size_t i = 0; i < n; ++i) { eng.dispatch(ids[i], w, b, EPOCH, d); }
        }));
    }
    // Authority validation + logical commit.
    {
        ExecutionEngine eng(EPOCH);
        std::vector<DispatchTicket> tickets(N);
        for (std::size_t i = 0; i < N; ++i) {
            eng.create(ids[i], EPOCH); eng.activate(ids[i], EPOCH);
            eng.dispatch(ids[i], w, b, EPOCH, tickets[i]);
            eng.mark_running(start(tickets[i]));
        }
        const char p[] = "bench";
        report("completion validate + logical commit", run(N, [&](std::size_t n){
            for (std::size_t i = 0; i < n; ++i) { eng.complete(finish(tickets[i], p)); }
        }));
    }
    // Persistence save / recover.
    {
        ExecutionEngine eng(EPOCH);
        std::vector<DispatchTicket> tickets(N);
        for (std::size_t i = 0; i < N; ++i) {
            eng.create(ids[i], EPOCH); eng.activate(ids[i], EPOCH);
            eng.dispatch(ids[i], w, b, EPOCH, tickets[i]);
            eng.mark_running(start(tickets[i]));
            eng.complete(finish(tickets[i], "r"));
        }
        const auto recs = eng.records();
        {
            FilePersistenceStore store("bench_store.efdl");
            report("persistence save (N records + crc)", run(1, [&](std::size_t){
                store.save(recs, EPOCH);
            }));
            std::vector<ExecutionRecord> loaded;
            CoordinatorEpoch ep(0);
            report("persistence recover (decode + validate)", run(1, [&](std::size_t){
                FilePersistenceStore s("bench_store.efdl");
                s.load(loaded, ep);
            }));
        }
        std::remove("bench_store.efdl");
    }
    // Protocol encode/decode (frame + ticket codec).
    {
        DispatchTicket t;
        t.execution_id = ExecutionId::random(); t.attempt_id = AttemptId::random();
        t.worker_id = WorkerId::random(); t.worker_boot_id = WorkerBootId::random();
        t.epoch = EPOCH; t.attempt_generation = AttemptGeneration(1);
        t.ownership_generation = OwnershipGeneration(1); t.fence_generation = FenceGeneration(1);
        t.dispatch_generation = DispatchGeneration(1);
        const auto packed = pack_dispatch_ack(DispatchAckPayload{true, DecisionCode::ALLOW, "ok", t});
        report("protocol frame encode + decode", run(N, [&](std::size_t n){
            for (std::size_t i = 0; i < n; ++i) {
                const auto frame = encode_frame(MsgType::DISPATCH_ACK, packed);
                DecodedFrame out; std::size_t c; std::string e;
                decode_frame(frame.data(), frame.size(), out, c, e);
            }
        }));
    }
    // Concurrent event ingestion (genuine producer contention; engine serialised).
    {
        ExecutionEngine eng(EPOCH);
        std::mutex mu;
        const int T = 8;
        std::atomic<std::size_t> done{0};
        const std::size_t per = N / T;
        report("concurrent event ingestion (8 threads, serialised)", run(N, [&](std::size_t){
            std::vector<std::thread> ts;
            for (int t = 0; t < T; ++t) {
                ts.emplace_back([&, t](){
                    std::size_t k = 0;
                    for (std::size_t i = 0; i < per; ++i) {
                        std::unique_lock<std::mutex> lk(mu);
                        const std::size_t idx = static_cast<std::size_t>(t) * per + i;
                        eng.create(ids[idx % ids.size()], EPOCH);
                        ++k;
                    }
                    done += k;
                });
            }
            for (auto& th : ts) { th.join(); }
        }));
    }

    std::printf("\nDone.\n");
    return 0;
}