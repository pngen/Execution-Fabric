// consumer/main.cpp
//
// A downstream consumer of the *installed* Execution Fabric package. It only
// depends on the public API surface and links via find_package(ExecutionFabric
// CONFIG REQUIRED) and the ExecutionFabric::ExecutionFabric target.
#include "execution_fabric/engine.hpp"
#include "execution_fabric/state.hpp"
#include <cstdio>
#include <cstring>

int main() {
    using namespace execution_fabric;
    const CoordinatorEpoch EPOCH(1);
    ExecutionEngine eng(EPOCH);
    const WorkerId w = WorkerId::random();
    const WorkerBootId b = WorkerBootId::random();

    const ExecutionId ex = ExecutionId::random();
    if (eng.create(ex, EPOCH).code != DecisionCode::ALLOW) { return 1; }
    if (eng.activate(ex, EPOCH).code != DecisionCode::ALLOW) { return 1; }
    DispatchTicket d;
    if (eng.dispatch(ex, w, b, EPOCH, d).code != DecisionCode::ALLOW) { return 1; }
    StartTicket s;
    s.execution_id=d.execution_id; s.execution_generation=d.execution_generation;
    s.attempt_id=d.attempt_id; s.attempt_generation=d.attempt_generation;
    s.worker_id=d.worker_id; s.worker_boot_id=d.worker_boot_id;
    s.epoch=d.epoch; s.dispatch_id=d.dispatch_id; s.dispatch_generation=d.dispatch_generation;
    s.ownership_generation=d.ownership_generation; s.fence_generation=d.fence_generation;
    eng.mark_running(s);
    CompletionTicket c;
    c.execution_id=d.execution_id; c.execution_generation=d.execution_generation;
    c.attempt_id=d.attempt_id; c.attempt_generation=d.attempt_generation;
    c.worker_id=d.worker_id; c.worker_boot_id=d.worker_boot_id;
    c.epoch=d.epoch; c.dispatch_id=d.dispatch_id; c.dispatch_generation=d.dispatch_generation;
    c.ownership_generation=d.ownership_generation; c.fence_generation=d.fence_generation;
    c.completion_generation=CompletionGeneration(1); c.has_result=true;
    const char p[] = "downstream-result";
    c.result_digest = ResultDigest::of(p, std::strlen(p));
    auto dec = eng.complete(c);
    std::printf("downstream consumer: complete -> %s, state=%s\n",
                to_string(dec.code), to_string(eng.find(ex)->state));
    return (dec.code == DecisionCode::ALLOW &&
            eng.find(ex)->state == ExecutionState::COMMITTED) ? 0 : 1;
}
