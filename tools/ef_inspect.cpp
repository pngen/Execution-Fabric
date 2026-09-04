// ef_inspect.cpp
//
// A CLI inspection tool. Loads a persistent Execution Fabric store and prints
// the recovered authority record of every logical execution, including its
// attempt history and committed outcome.
#include "execution_fabric/persistence.hpp"
#include "execution_fabric/state.hpp"
#include <cstdio>
#include <string>

using namespace execution_fabric;

int main(int argc, char** argv) {
    if (argc < 2) {
        std::printf("usage: ef_inspect <persistence-file>\n");
        return 1;
    }
    FilePersistenceStore store(argv[1]);
    std::vector<ExecutionRecord> recs;
    CoordinatorEpoch epoch(0);
    if (!store.load(recs, epoch)) {
        std::printf("inspect failed: %s\n", store.last_error().c_str());
        return 1;
    }
    std::printf("Executor Fabric store: %s\n", argv[1]);
    std::printf("Coordinator epoch: %llu, executions: %zu\n\n",
                (unsigned long long)epoch.value(), recs.size());
    for (const auto& r : recs) {
        std::printf("Execution %s  gen=%llu  state=%s  epoch=%llu\n",
                    r.id.to_string().c_str(), (unsigned long long)r.generation.value(),
                    to_string(r.state), (unsigned long long)r.coordinator_epoch.value());
        std::printf("  owns=%s boot=%s  attempts=%zu",
                    r.owner_worker ? r.owner_worker->to_string().c_str() : "<none>",
                    r.owner_worker_boot ? r.owner_worker_boot->to_string().c_str() : "<none>",
                    r.attempts.size());
        if (r.committed_digest) {
            std::printf("  committed=%s", r.committed_digest->to_hex().c_str());
        }
        std::printf("\n");
        for (const auto& a : r.attempts) {
            std::printf("    attempt %s gen=%llu state=%s worker=%s\n",
                        a.id.to_string().c_str(), (unsigned long long)a.generation.value(),
                        to_string(a.state), a.worker_id.to_string().c_str());
        }
    }
    return 0;
}
