// basic_authority.cpp
//
// Demonstrates the public execution-authority API: creating a logical
// execution, dispatching physical attempts, validating completions, and
// committing exactly one logical result. It also shows how every decision
// carries a machine-readable code and an explanation of the authority that
// would need to change.
#include "execution_fabric/engine.hpp"
#include "execution_fabric/state.hpp"
#include <cstdio>
#include <string>

using namespace execution_fabric;

static StartTicket start(const DispatchTicket& d) {
    StartTicket s;
    s.execution_id = d.execution_id; s.execution_generation = d.execution_generation;
    s.attempt_id = d.attempt_id; s.attempt_generation = d.attempt_generation;
    s.worker_id = d.worker_id; s.worker_boot_id = d.worker_boot_id;
    s.epoch = d.epoch; s.dispatch_id = d.dispatch_id; s.dispatch_generation = d.dispatch_generation;
    s.ownership_generation = d.ownership_generation; s.fence_generation = d.fence_generation;
    return s;
}

static CompletionTicket complete(const DispatchTicket& d, const char* payload) {
    CompletionTicket ct;
    ct.execution_id = d.execution_id; ct.execution_generation = d.execution_generation;
    ct.attempt_id = d.attempt_id; ct.attempt_generation = d.attempt_generation;
    ct.worker_id = d.worker_id; ct.worker_boot_id = d.worker_boot_id;
    ct.epoch = d.epoch; ct.dispatch_id = d.dispatch_id; ct.dispatch_generation = d.dispatch_generation;
    ct.ownership_generation = d.ownership_generation; ct.fence_generation = d.fence_generation;
    ct.completion_generation = CompletionGeneration(1);
    ct.has_result = true;
    ct.result_digest = ResultDigest::of(payload, std::strlen(payload));
    return ct;
}

int main() {
    const CoordinatorEpoch EPOCH(1);
    ExecutionEngine eng(EPOCH);
    const WorkerId w = WorkerId::random();
    const WorkerBootId b = WorkerBootId::random();

    const ExecutionId ex = ExecutionId::random();
    auto pr = [&](const char* what, const Decision& d) {
        std::printf("  %-28s => %s\n", what, d.to_string().c_str());
    };

    std::printf("Execution Fabric — authority lifecycle example\n\n");
    pr("create", eng.create(ex, EPOCH));
    pr("activate", eng.activate(ex, EPOCH));

    DispatchTicket d;
    pr("dispatch(worker A)", eng.dispatch(ex, w, b, EPOCH, d));
    std::printf("  attempt generation = %llu\n", (unsigned long long)d.attempt_generation.value());
    std::printf("  ownership gen      = %llu\n", (unsigned long long)d.ownership_generation.value());
    pr("mark_running", eng.mark_running(start(d)));

    const char result[] = "authoritative-result";
    pr("complete", eng.complete(complete(d, result)));
    pr("state", Decision{DecisionCode::ALLOW, to_string(eng.find(ex)->state)});

    // The same physical work may run again as a duplicate; the logical result
    // is committed exactly once.
    pr("duplicate complete", eng.complete(complete(d, result)));

    // A conflicting result from the same attempt is rejected loudly.
    pr("conflicting result", eng.complete(complete(d, "different-result")));

    // A stale attempt (from a displaced worker incarnation) is refused.
    CompletionTicket stale = complete(d, "late");
    stale.worker_boot_id = WorkerBootId::random();
    pr("stale boot complete", eng.complete(stale));

    std::printf("\nFinal state: %s\n", to_string(eng.find(ex)->state));
    const auto* rec = eng.find(ex);
    if (rec->committed_digest) {
        std::printf("Committed digest: %s\n", rec->committed_digest->to_hex().c_str());
    }
    std::printf("Commit generation: %llu  (exactly-once logical commit)\n",
                (unsigned long long)rec->commit_generation.value());
    return 0;
}
