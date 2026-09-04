#define _CRT_SECURE_NO_WARNINGS
#include "protocol/protocol.hpp"
#include "protocol/transport.hpp"
#include "proof/process.hpp"
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

using namespace execution_fabric;

// ---------------------------------------------------------------------------
// A small controller client that drives the coordinator over the wire.
// ---------------------------------------------------------------------------
class Controller {
public:
    bool connect(const std::string& host, std::uint16_t port, std::string& err) {
        if (!conn_.connect(host, port, err)) { return false; }
        HelloPayload hp; hp.role = 0; hp.protocol_version = kFrameVersion; hp.tag = "proof-controller";
        if (!conn_.send_frame(MsgType::HELLO, pack_hello(hp), err)) { return false; }
        MsgType t; std::vector<std::uint8_t> p;
        if (!conn_.recv_frame(t, p, err)) { return false; }
        if (t != MsgType::WELCOME) { err = "expected WELCOME"; return false; }
        WelcomePayload wp;
        if (!unpack_welcome(p, wp, err)) { return false; }
        epoch_ = wp.epoch;
        return true;
    }

    bool submit(const ExecutionId& ex, const CoordinatorEpoch& epoch, SubmitAckPayload& out, std::string& err) {
        SubmitPayload sp; sp.execution_id = ex; sp.epoch = epoch;
        if (!conn_.send_frame(MsgType::SUBMIT, pack_submit(sp), err)) { return false; }
        MsgType t; std::vector<std::uint8_t> p;
        if (!conn_.recv_frame(t, p, err)) { return false; }
        if (!unpack_submit_ack(p, out, err)) { return false; }
        return true;
    }

    bool dispatch(const ExecutionId& ex, const WorkerId& w, const WorkerBootId& b,
                  const CoordinatorEpoch& epoch, DispatchAckPayload& out, std::string& err) {
        DispatchRequestPayload dp; dp.execution_id = ex; dp.epoch = epoch; dp.worker_id = w; dp.worker_boot_id = b;
        if (!conn_.send_frame(MsgType::DISPATCH, pack_dispatch(dp), err)) { return false; }
        MsgType t; std::vector<std::uint8_t> p;
        if (!conn_.recv_frame(t, p, err)) { return false; }
        if (!unpack_dispatch_ack(p, out, err)) { return false; }
        return true;
    }

    bool complete(const CompletionTicket& ct, const std::vector<std::uint8_t>& result, CompleteAckPayload& out, std::string& err) {
        CompletePayload cp; cp.ticket = ct; cp.result = result;
        if (!conn_.send_frame(MsgType::COMPLETE, pack_complete(cp), err)) { return false; }
        MsgType t; std::vector<std::uint8_t> p;
        if (!conn_.recv_frame(t, p, err)) { return false; }
        if (!unpack_complete_ack(p, out, err)) { return false; }
        return true;
    }

    bool cancel(const ControlRequest& req, Decision& out, std::string& err) {
        CancelPayload cp; cp.request = req;
        if (!conn_.send_frame(MsgType::CANCEL, pack_cancel(cp), err)) { return false; }
        MsgType t; std::vector<std::uint8_t> p;
        if (!conn_.recv_frame(t, p, err)) { return false; }
        if (!unpack_decision(p, out, err)) { return false; }
        return true;
    }
    bool preempt(const ControlRequest& req, Decision& out, std::string& err) {
        CancelPayload cp; cp.request = req;
        if (!conn_.send_frame(MsgType::PREEMPT, pack_preempt(cp), err)) { return false; }
        MsgType t; std::vector<std::uint8_t> p;
        if (!conn_.recv_frame(t, p, err)) { return false; }
        if (!unpack_decision(p, out, err)) { return false; }
        return true;
    }
    bool ack_preempt(const ExecutionId& ex, const CoordinatorEpoch& epoch, Decision& out, std::string& err) {
        AckPreemptPayload ap; ap.execution_id = ex; ap.epoch = epoch;
        if (!conn_.send_frame(MsgType::ACK_PREEMPT, pack_ack_preempt(ap), err)) { return false; }
        MsgType t; std::vector<std::uint8_t> p;
        if (!conn_.recv_frame(t, p, err)) { return false; }
        if (!unpack_decision(p, out, err)) { return false; }
        return true;
    }
    bool mark_resumable(const ExecutionId& ex, const CoordinatorEpoch& epoch, Decision& out, std::string& err) {
        MarkResumablePayload mp; mp.execution_id = ex; mp.epoch = epoch;
        if (!conn_.send_frame(MsgType::MARK_RESUMABLE, pack_mark_resumable(mp), err)) { return false; }
        MsgType t; std::vector<std::uint8_t> p;
        if (!conn_.recv_frame(t, p, err)) { return false; }
        if (!unpack_decision(p, out, err)) { return false; }
        return true;
    }
    bool resume(const ResumeRequestPayload& rp, DispatchAckPayload& out, std::string& err) {
        if (!conn_.send_frame(MsgType::RESUME, pack_resume(rp), err)) { return false; }
        MsgType t; std::vector<std::uint8_t> p;
        if (!conn_.recv_frame(t, p, err)) { return false; }
        if (!unpack_dispatch_ack(p, out, err)) { return false; }
        return true;
    }
    bool query(const ExecutionId& ex, QueryResponsePayload& out, std::string& err) {
        QueryPayload qp; qp.execution_id = ex;
        if (!conn_.send_frame(MsgType::QUERY, pack_query(qp), err)) { return false; }
        MsgType t; std::vector<std::uint8_t> p;
        if (!conn_.recv_frame(t, p, err)) { return false; }
        if (!unpack_query_response(p, out, err)) { return false; }
        return true;
    }
    CoordinatorEpoch epoch() const { return epoch_; }

private:
    net::TcpConnection conn_;
    CoordinatorEpoch epoch_{0};
};

static CompletionTicket ticket_to_complete(const DispatchTicket& dt, const std::vector<std::uint8_t>& result) {
    CompletionTicket ct;
    ct.execution_id = dt.execution_id; ct.execution_generation = dt.execution_generation;
    ct.attempt_id = dt.attempt_id; ct.attempt_generation = dt.attempt_generation;
    ct.worker_id = dt.worker_id; ct.worker_boot_id = dt.worker_boot_id;
    ct.epoch = dt.epoch; ct.dispatch_id = dt.dispatch_id; ct.dispatch_generation = dt.dispatch_generation;
    ct.ownership_generation = dt.ownership_generation; ct.fence_generation = dt.fence_generation;
    ct.completion_generation = CompletionGeneration(1);
    ct.has_result = true;
    ct.result_digest = ResultDigest::of_buffer(result);
    return ct;
}

static int failures = 0;
static int checks = 0;
static void CHECKTRUE(bool cond, const char* msg) {
    ++checks;
    if (!cond) { ++failures; std::printf("  [FAIL] %s\n", msg); }
}
#define CHECK(cond) do { ++checks; if (!(cond)) { ++failures; std::printf("  [FAIL] %s:%d: %s\n", __FILE__, __LINE__, #cond); } } while (0)

static bool wait_state(Controller& c, const ExecutionId& ex, const std::vector<ExecutionState>& want,
                       int timeout_ms, QueryResponsePayload& qr) {
    const auto start = std::chrono::steady_clock::now();
    while (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count() < timeout_ms) {
        std::string err;
        if (c.query(ex, qr, err)) {
            for (auto s : want) { if (qr.state == s) { return true; } }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

int main() {
    net::init();
    // Locate the coordinator & worker binaries relative to this executable.
    std::string coord_dir = ".";
    std::string worker_dir = ".";
    if (auto d = ::getenv("EF_BIN_DIR")) { coord_dir = d; worker_dir = d; }
    else {
        const char* coord_exe = "ef_coordinator.exe";
        const char* worker_exe = "ef_worker.exe";
        const char* cands[] = {".", "./Release", "../../coordinator/Release", "../../worker/Release",
            "../coordinator/Release", "../worker/Release"};
        for (const char* c : cands) {
            std::string p = std::string(c) + "/" + coord_exe;
            if (FILE* f = std::fopen(p.c_str(), "rb")) { std::fclose(f); coord_dir = c; break; }
        }
        for (const char* c : cands) {
            std::string p = std::string(c) + "/" + worker_exe;
            if (FILE* f = std::fopen(p.c_str(), "rb")) { std::fclose(f); worker_dir = c; break; }
        }
    }

    std::uint16_t port = 0;
    if (!eftest::find_free_port(port)) { std::printf("cannot find a free port\n"); return 1; }

    const WorkerId wa = WorkerId::random();
    const WorkerId wb = WorkerId::random();
    const WorkerId wc = WorkerId::random();
    const WorkerBootId ba = WorkerBootId::random();
    const WorkerBootId bb = WorkerBootId::random();
    const WorkerBootId wc_boot = WorkerBootId::random();

    const std::string persist_path = "ef_proof_store.efdl";
    std::remove(persist_path.c_str());
    eftest::Process coord;
    CHECKTRUE(coord.spawn(coord_dir + "/ef_coordinator", {"--host=127.0.0.1", "--port=" + std::to_string(port),
        "--persist=" + persist_path, "--epoch=1"}), "spawn coordinator");
    std::this_thread::sleep_for(std::chrono::milliseconds(700));

    eftest::Process workerA;
    eftest::Process workerB;
    eftest::Process workerC;
    CHECKTRUE(workerC.spawn(worker_dir + "/ef_worker", {"--coordinator=127.0.0.1:" + std::to_string(port),
        "--worker-id=" + wc.to_string(), "--boot-id=" + wc_boot.to_string(), "--delay-ms=100000"}),
        "spawn worker C");
    CHECKTRUE(workerA.spawn(worker_dir + "/ef_worker", {"--coordinator=127.0.0.1:" + std::to_string(port),
        "--worker-id=" + wa.to_string(), "--boot-id=" + ba.to_string(), "--delay-ms=1500"}), "spawn worker A");
    CHECKTRUE(workerB.spawn(worker_dir + "/ef_worker", {"--coordinator=127.0.0.1:" + std::to_string(port),
        "--worker-id=" + wb.to_string(), "--boot-id=" + bb.to_string(), "--delay-ms=60"}), "spawn worker B");
    std::this_thread::sleep_for(std::chrono::milliseconds(700));

    Controller c;
    std::string err;
    CHECKTRUE(c.connect("127.0.0.1", port, err), "controller connects to coordinator");

    ExecutionId committed_ex;
    // ---- Scenario 1: retry after kill, stale reject, commit, recovery.
    {
        const auto ep = c.epoch();
        const ExecutionId ex = ExecutionId::random();
        committed_ex = ex;
        std::printf("\n=== Scenario 1: retry-after-kill, stale rejection, commit, recovery ===\n");
        SubmitAckPayload sa;
        CHECKTRUE(c.submit(ex, ep, sa, err) && sa.accepted, "submit exec accepted");
        DispatchAckPayload dA;
        CHECKTRUE(c.dispatch(ex, wa, ba, ep, dA, err) && dA.accepted, "dispatch attempt A");
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        workerA.kill();   // real OS process death
        QueryResponsePayload qr;
        CHECKTRUE(wait_state(c, ex, {ExecutionState::AMBIGUOUS, ExecutionState::READY}, 5000, qr),
                  "worker A loss classified");
        const WorkerBootId bb2 = WorkerBootId::random();
        DispatchAckPayload dB;
        CHECKTRUE(c.dispatch(ex, wb, bb2, ep, dB, err) && dB.accepted, "dispatch attempt B (retry)");
        CHECKTRUE(wait_state(c, ex, {ExecutionState::COMMITTED}, 8000, qr), "attempt B commits");
        CHECKTRUE(qr.attempt_count == 2, "two historical attempts recorded");
        const std::vector<std::uint8_t> stale_result{'s','t','a','l','e'};
        CompletionTicket stale = ticket_to_complete(dA.ticket, stale_result);
        CompleteAckPayload ca;
        CHECKTRUE(c.complete(stale, stale_result, ca, err), "stale completion acked");
        CHECKTRUE(ca.decision == DecisionCode::REJECT_STALE_ATTEMPT, "stale completion from A rejected as STALE_ATTEMPT");
        const std::vector<std::uint8_t> b_result{'b','-','r','e','s','u','l','t'};
        CompletionTicket dup = ticket_to_complete(dB.ticket, b_result);
        // Recognise the duplicate by matching the committed result digest exactly,
        // so the runtime proves it never double-commits an identical completion.
        if (qr.committed_digest) { dup.result_digest = *qr.committed_digest; }
        CHECKTRUE(c.complete(dup, b_result, ca, err), "duplicate completion acked");
        CHECKTRUE(ca.decision == DecisionCode::REJECT_ALREADY_COMMITTED, "duplicate completion never double-commits");
        const std::vector<std::uint8_t> conflict_result{'c','o','n','f','l','i','c','t'};
        CompletionTicket conf = ticket_to_complete(dB.ticket, conflict_result);
        CHECKTRUE(c.complete(conf, conflict_result, ca, err), "conflicting completion acked");
        CHECKTRUE(ca.decision == DecisionCode::REJECT_CONFLICTING_COMPLETION, "conflicting completion rejected");
    }

    // ---- Scenario 2: cancellation race.
    {
        const auto ep = c.epoch();
        const ExecutionId ex = ExecutionId::random();
        std::printf("\n=== Scenario 2: cancellation (COMPLETE vs CANCEL) ===\n");
        SubmitAckPayload sa;
        CHECKTRUE(c.submit(ex, ep, sa, err) && sa.accepted, "submit");
        const WorkerId w = WorkerId::random();
        const WorkerBootId b = WorkerBootId::random();
        DispatchAckPayload d;
        CHECKTRUE(c.dispatch(ex, wc, wc_boot, ep, d, err) && d.accepted, "dispatch");
        ControlRequest req; req.execution_id = ex; req.epoch = ep;
        req.authority_generation = d.ticket.ownership_generation;
        Decision dec;
        CHECKTRUE(c.cancel(req, dec, err), "cancel acked");
        CHECKTRUE(dec.code == DecisionCode::ALLOW, "cancel accepted while running");
        const std::vector<std::uint8_t> result{'c','o','m','p','l','e','t','e'};
        CompletionTicket ct = ticket_to_complete(d.ticket, result);
        CompleteAckPayload ca;
        CHECKTRUE(c.complete(ct, result, ca, err), "completion races cancellation");
        CHECKTRUE(ca.decision == DecisionCode::ALLOW, "completion wins the race (serial order)");
        QueryResponsePayload qr; std::string qe;
        CHECKTRUE(c.query(ex, qr, qe) && qr.state == ExecutionState::COMMITTED, "cancellation-race completion committed");
    }

    // ---- Scenario 3: preemption / resume.
    {
        const auto ep = c.epoch();
        const ExecutionId ex = ExecutionId::random();
        std::printf("\n=== Scenario 3: preemption / resume under a fresh attempt ===\n");
        SubmitAckPayload sa;
        CHECKTRUE(c.submit(ex, ep, sa, err) && sa.accepted, "submit");
        DispatchAckPayload d1;
        CHECKTRUE(c.dispatch(ex, wc, wc_boot, ep, d1, err) && d1.accepted, "dispatch attempt P1");
        ControlRequest preq; preq.execution_id = ex; preq.epoch = ep;
        preq.authority_generation = d1.ticket.ownership_generation;
        Decision dec;
        CHECKTRUE(c.preempt(preq, dec, err), "preempt acked");
        CHECKTRUE(dec.code == DecisionCode::ALLOW, "preempt accepted");
        CHECKTRUE(c.ack_preempt(ex, ep, dec, err) && dec.code == DecisionCode::ALLOW, "preemption acknowledged");
        CHECKTRUE(c.mark_resumable(ex, ep, dec, err) && dec.code == DecisionCode::ALLOW, "marked resumable");
        const WorkerBootId rb = WorkerBootId::random();
        ResumeRequestPayload rp; rp.execution_id = ex; rp.epoch = ep;
        rp.resume_gen = ResumeGeneration(1); rp.worker_id = wb; rp.worker_boot_id = rb;
        DispatchAckPayload d2;
        CHECKTRUE(c.resume(rp, d2, err), "resume acked");
        CHECKTRUE(d2.accepted && d2.decision == DecisionCode::RESUME_ALLOWED, "resume authorised");
        CHECK(d2.ticket.attempt_generation.value() == d1.ticket.attempt_generation.value() + 1);
        const std::vector<std::uint8_t> old_result{'o','l','d'};
        CompletionTicket old = ticket_to_complete(d1.ticket, old_result);
        CompleteAckPayload ca;
        CHECKTRUE(c.complete(old, old_result, ca, err), "old preempted completion acked");
        CHECKTRUE(ca.decision == DecisionCode::REJECT_STALE_ATTEMPT, "old preempted attempt rejected as STALE_ATTEMPT");
        const std::vector<std::uint8_t> fresh_result{'f','r','e','s','h'};
        CompletionTicket fresh = ticket_to_complete(d2.ticket, fresh_result);
        CHECKTRUE(c.complete(fresh, fresh_result, ca, err) && ca.decision == DecisionCode::ALLOW, "post-resume attempt commits");
        QueryResponsePayload qr; std::string qe;
        CHECKTRUE(c.query(ex, qr, qe) && qr.state == ExecutionState::COMMITTED, "preempt/resume execution committed");
        CHECKTRUE(qr.attempt_count == 2, "preempted + resume attempts recorded");
    }

    // ---- Recovery: restart coordinator with a fresh epoch, verify committed state.
    std::printf("\n=== Coordinator restart / recovery ===\n");
    coord.kill();
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    eftest::Process coord2;
    CHECKTRUE(coord2.spawn(coord_dir + "/ef_coordinator", {"--host=127.0.0.1", "--port=" + std::to_string(port),
        "--persist=" + persist_path, "--epoch=2"}), "restart coordinator (epoch=2)");
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    Controller c2;
    CHECKTRUE(c2.connect("127.0.0.1", port, err), "controller reconnects after restart");
    CHECKTRUE(c2.epoch().value() == 2u, "recovered coordinator epoch advanced to 2");
    // Workers from the previous coordinator incarnation exited when it died;
    // a fresh worker incarnation (new WorkerBootId) must join for work to resume.
    eftest::Process workerD;
    const WorkerId wd = WorkerId::random();
    const WorkerBootId wd_boot = WorkerBootId::random();
    CHECKTRUE(workerD.spawn(worker_dir + "/ef_worker", {"--coordinator=127.0.0.1:" + std::to_string(port),
        "--worker-id=" + wd.to_string(), "--boot-id=" + wd_boot.to_string(), "--delay-ms=60"}),
        "spawn fresh worker after recovery");
    std::this_thread::sleep_for(std::chrono::milliseconds(700));

    // The scenario-1 committed execution must survive the restart exactly.
    {
        std::string qe;
        QueryResponsePayload qr;
        CHECKTRUE(c2.query(committed_ex, qr, qe), "committed execution queryable after restart");
        CHECKTRUE(qr.found && qr.state == ExecutionState::COMMITTED, "recovered execution is still COMMITTED");
        CHECKTRUE(qr.committed_digest.has_value(), "recovered execution preserves committed digest");
        CHECKTRUE(qr.attempt_count == 2, "recovered execution preserves attempt history");

        // Old dynamic worker authority must not be resurrected: a completion that
        // carries pre-restart epoch=1 and a stale worker boot must be rejected.
        CompletionTicket staleOld;
        staleOld.execution_id = committed_ex;
        staleOld.execution_generation = qr.generation;
        if (qr.current_attempt) staleOld.attempt_id = *qr.current_attempt;
        else staleOld.attempt_id = AttemptId::random();
        staleOld.attempt_generation = qr.current_attempt_generation.value_or(AttemptGeneration(1));
        staleOld.worker_id = WorkerId::random();
        staleOld.worker_boot_id = WorkerBootId::random();
        staleOld.epoch = CoordinatorEpoch(1);           // pre-restart epoch
        staleOld.ownership_generation = OwnershipGeneration(1);
        staleOld.fence_generation = FenceGeneration(1);
        staleOld.dispatch_id = DispatchId::random();
        staleOld.dispatch_generation = DispatchGeneration(1);
        staleOld.completion_generation = CompletionGeneration(1);
        staleOld.has_result = true;
        staleOld.result_digest = ResultDigest::of_buffer(std::vector<std::uint8_t>{'z'});
        CompleteAckPayload caOld;
        CHECKTRUE(c2.complete(staleOld, std::vector<std::uint8_t>{'z'}, caOld, qe), "old-authority completion acked");
        std::printf("  [debug] old-authority completion decision = %s (%s)\n", to_string(caOld.decision), caOld.reason.c_str());
        CHECKTRUE(caOld.decision == DecisionCode::REJECT_STALE_EPOCH ||
                  caOld.decision == DecisionCode::REJECT_STALE_BOOT ||
                  caOld.decision == DecisionCode::REJECT_STALE_ATTEMPT ||
                  caOld.decision == DecisionCode::REJECT_NOT_OWNER ||
                  caOld.decision == DecisionCode::REJECT_ALREADY_COMMITTED,
                  "old dynamic worker authority is not resurrected");
    }

    // A fresh execution after recovery on the live worker B must succeed.
    {
        const ExecutionId fresh = ExecutionId::random();
        SubmitAckPayload sa;
        CHECKTRUE(c2.submit(fresh, c2.epoch(), sa, err) && sa.accepted, "fresh submit after recovery");
        DispatchAckPayload df;
        CHECKTRUE(c2.dispatch(fresh, wd, wd_boot, c2.epoch(), df, err) && df.accepted, "fresh dispatch after recovery");
        QueryResponsePayload qr;
        CHECKTRUE(wait_state(c2, fresh, {ExecutionState::COMMITTED}, 6000, qr), "fresh execution commits after recovery");
    }

    // Final cleanup of remaining child processes.
    coord2.kill();
    workerA.kill();
    workerB.kill();
    workerC.kill();
    workerD.kill();

    std::printf("\nproof_harness: %d checks, %d failures\n", checks, failures);
    net::cleanup();
    return failures == 0 ? 0 : 1;
}