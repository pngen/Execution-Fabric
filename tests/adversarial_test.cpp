#include "execution_fabric/engine.hpp"
#include "execution_fabric/persistence.hpp"
#include "execution_fabric/binary.hpp"
#include "execution_fabric/state_machine.hpp"
#include "execution_fabric/checksum.hpp"
#include "test_util.hpp"
#include <cstdio>
#include <fstream>

using namespace execution_fabric;

namespace {
const std::uint8_t kMagic[] = {0x45,0x46,0x41,0x42,0x53,0x54,0x4F,0x52};
const std::uint8_t kTr[] = {0x45,0x46,0x54,0x52,0x41,0x49,0x4C,0x52};
std::vector<std::uint8_t> build_file(const std::vector<ExecutionRecord>& recs, const CoordinatorEpoch& ep) {
    ByteWriter body;
    ByteWriter h; h.bytes(kMagic,8); h.u32(1); h.u64(ep.value()); h.u32((std::uint32_t)recs.size());
    body.bytes(h.data());
    for (const auto& r : recs) {
        auto p = FilePersistenceStore::encode_record(r);
        ByteWriter rw; rw.u32((std::uint32_t)p.size()); rw.u64(Crc64::compute(p.data(), p.size())); rw.bytes(p.data(), p.size());
        body.bytes(rw.data());
    }
    std::vector<std::uint8_t> out(body.data().begin(), body.data().end());
    ByteWriter tr; tr.bytes(kTr,8); tr.u64(Crc64::compute(out.data(), out.size()));
    out.insert(out.end(), tr.data().begin(), tr.data().end());
    return out;
}
void wf(const std::string& p, const std::vector<std::uint8_t>& d){
    std::ofstream f(p, std::ios::binary|std::ios::trunc); f.write((const char*)d.data(), (std::streamsize)d.size());
}
}  // namespace

int main() {
    const CoordinatorEpoch EPOCH(1);
    const CoordinatorEpoch EPOCH2(2);
    ExecutionEngine eng(EPOCH);
    const WorkerId w = WorkerId::random(); const WorkerBootId b = WorkerBootId::random();

    // --- Duplicate identity rejected at create.
    const ExecutionId ex = ExecutionId::random();
    CHECK_EQ(eng.create(ex, EPOCH).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.create(ex, EPOCH).code, DecisionCode::REJECT_EXISTS);

    // --- Old coordinator authority never revives after rollover.
    DispatchTicket oldTicket;
    CHECK_EQ(eng.activate(ex, EPOCH).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.dispatch(ex, w, b, EPOCH, oldTicket).code, DecisionCode::ALLOW);
    // Rollover to epoch 2.
    eng.raise_epoch(EPOCH2);
    // A completion carrying the old (epoch 1) ticket must be rejected.
    CompletionTicket oldC; oldC.execution_id=oldTicket.execution_id; oldC.execution_generation=oldTicket.execution_generation;
    oldC.attempt_id=oldTicket.attempt_id; oldC.attempt_generation=oldTicket.attempt_generation; oldC.worker_id=oldTicket.worker_id;
    oldC.worker_boot_id=oldTicket.worker_boot_id; oldC.epoch=EPOCH; oldC.dispatch_id=oldTicket.dispatch_id;
    oldC.dispatch_generation=oldTicket.dispatch_generation; oldC.ownership_generation=oldTicket.ownership_generation;
    oldC.fence_generation=oldTicket.fence_generation; oldC.completion_generation=CompletionGeneration(1);
    oldC.has_result=true; oldC.result_digest=ResultDigest::of("x",1);
    CHECK_EQ(eng.complete(oldC).code, DecisionCode::REJECT_STALE_EPOCH);
    // Old coordinator cancel authority rejected.
    ControlRequest oldCancel; oldCancel.execution_id=ex; oldCancel.epoch=EPOCH;
    oldCancel.authority_generation=eng.find(ex)->ownership_generation;
    CHECK_EQ(eng.cancel(oldCancel).code, DecisionCode::REJECT_STALE_EPOCH);

    // --- Insufficient authority ticket.
    const ExecutionId ex2 = ExecutionId::random();
    CHECK_EQ(eng.create(ex2, EPOCH2).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.activate(ex2, EPOCH2).code, DecisionCode::ALLOW);
    CompletionTicket nilC;
    nilC.execution_id = ex2;
    CHECK_EQ(eng.complete(nilC).code, DecisionCode::REJECT_INSUFFICIENT_AUTHORITY);

    // --- Late owner mutation: a superseded owner cannot act.
    DispatchTicket a1; CHECK_EQ(eng.dispatch(ex2, w, b, EPOCH2, a1).code, DecisionCode::ALLOW);
    DispatchTicket a2;
    const WorkerId w2 = WorkerId::random(); const WorkerBootId b2 = WorkerBootId::random();
    CHECK_EQ(eng.dispatch(ex2, w2, b2, EPOCH2, a2).code, DecisionCode::ALLOW);  // supersedes owner w
    ControlRequest lateCancel; lateCancel.execution_id=ex2; lateCancel.epoch=EPOCH2;
    lateCancel.authority_generation = a1.ownership_generation;  // stale authority
    CHECK_EQ(eng.cancel(lateCancel).code, DecisionCode::REJECT_NOT_OWNER);

    // --- Terminal committed state cannot be reopened.
    DispatchTicket a3; CHECK_EQ(eng.dispatch(ex2, w, b, EPOCH2, a3).code, DecisionCode::ALLOW);
    StartTicket s; s.execution_id=a3.execution_id; s.execution_generation=a3.execution_generation;
    s.attempt_id=a3.attempt_id; s.attempt_generation=a3.attempt_generation; s.worker_id=a3.worker_id;
    s.worker_boot_id=a3.worker_boot_id; s.epoch=a3.epoch; s.dispatch_id=a3.dispatch_id;
    s.dispatch_generation=a3.dispatch_generation; s.ownership_generation=a3.ownership_generation;
    s.fence_generation=a3.fence_generation;
    CHECK_EQ(eng.mark_running(s).code, DecisionCode::ALLOW);
    CompletionTicket c3; c3.execution_id=a3.execution_id; c3.execution_generation=a3.execution_generation;
    c3.attempt_id=a3.attempt_id; c3.attempt_generation=a3.attempt_generation; c3.worker_id=a3.worker_id;
    c3.worker_boot_id=a3.worker_boot_id; c3.epoch=a3.epoch; c3.dispatch_id=a3.dispatch_id;
    c3.dispatch_generation=a3.dispatch_generation; c3.ownership_generation=a3.ownership_generation;
    c3.fence_generation=a3.fence_generation; c3.completion_generation=CompletionGeneration(1);
    c3.has_result=true; c3.result_digest=ResultDigest::of("z",1);
    CHECK_EQ(eng.complete(c3).code, DecisionCode::ALLOW);
    CHECK(ExecutionStateMachine::is_terminal(eng.find(ex2)->state));
    ControlRequest reopen; reopen.execution_id=ex2; reopen.epoch=EPOCH2;
    reopen.authority_generation=eng.find(ex2)->ownership_generation;
    CHECK_EQ(eng.cancel(reopen).code, DecisionCode::REJECT_ALREADY_TERMINAL);
    CHECK_EQ(eng.preempt(reopen).code, DecisionCode::REJECT_ALREADY_TERMINAL);
    DispatchTicket dbl; CHECK_EQ(eng.dispatch(ex2, w, b, EPOCH2, dbl).code, DecisionCode::REJECT_ALREADY_COMMITTED);

    // --- Persistence: duplicate execution identity in a file.
    {
        std::vector<ExecutionRecord> dup;
        ExecutionRecord r1; r1.id = ExecutionId::random(); r1.generation=ExecutionGeneration(1); r1.state=ExecutionState::CREATED; r1.coordinator_epoch=EPOCH;
        ExecutionRecord r2 = r1;  // same id
        dup.push_back(r1); dup.push_back(r2);
        auto bytes = build_file(dup, EPOCH);
        wf("adv_dup.efdl", bytes);
        FilePersistenceStore store("adv_dup.efdl");
        std::vector<ExecutionRecord> o; CoordinatorEpoch e(0);
        CHECK(!store.load(o, e));
        CHECK(store.last_error().find("duplicate execution identity") != std::string::npos);
        std::remove("adv_dup.efdl");
    }

    // --- Persistence: impossible state (COMMITTED without committed digest).
    {
        ExecutionRecord r; r.id = ExecutionId::random(); r.generation=ExecutionGeneration(1); r.state=ExecutionState::COMMITTED; r.coordinator_epoch=EPOCH;
        r.commit_generation = CommitGeneration(1);  // but no digest
        std::vector<ExecutionRecord> one{r};
        auto bytes = build_file(one, EPOCH);
        wf("adv_state.efdl", bytes);
        FilePersistenceStore store("adv_state.efdl");
        std::vector<ExecutionRecord> o; CoordinatorEpoch e(0);
        CHECK(!store.load(o, e));
        CHECK(store.last_error().find("committed state without committed digest") != std::string::npos);
        std::remove("adv_state.efdl");
    }

    std::printf("adversarial_test: %d checks, %d failures\n", eftest::checks, eftest::failures);
    return TEST_MAIN_RETURN();
}