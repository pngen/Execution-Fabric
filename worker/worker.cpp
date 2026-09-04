#include "worker/worker.hpp"
#include "execution_fabric/checksum.hpp"
#include "execution_fabric/sha256.hpp"
#include <utility>

namespace execution_fabric {

WorkerRuntime::WorkerRuntime(std::string host, std::uint16_t port, WorkerId worker_id, WorkerBootId boot)
    : host_(std::move(host)), port_(port), worker_id_(worker_id), boot_(boot) {}

bool WorkerRuntime::start(std::string& err) {
    if (!conn_.connect(host_, port_, err)) { return false; }
    HelloPayload hp;
    hp.role = 1;   // worker
    hp.protocol_version = kFrameVersion;
    hp.worker_id = worker_id_;
    hp.worker_boot_id = boot_;
    hp.tag = "EF-worker";
    if (!conn_.send_frame(MsgType::HELLO, pack_hello(hp), err)) { return false; }
    // Wait for WELCOME.
    MsgType type;
    std::vector<std::uint8_t> payload;
    if (!conn_.recv_frame(type, payload, err)) { return false; }
    if (type != MsgType::WELCOME) { err = "expected WELCOME, got " + std::string(to_string(type)); return false; }
    WelcomePayload wp;
    std::string uerr;
    if (!unpack_welcome(payload, wp, uerr)) { err = "bad welcome: " + uerr; return false; }
    connected_ = true;
    return true;
}

bool WorkerRuntime::run(std::string& err) {
    if (!connected_) { err = "not connected"; return false; }
    running_.store(true);
    while (running_.load()) {
        MsgType type;
        std::vector<std::uint8_t> payload;
        if (!conn_.recv_frame(type, payload, err)) { return false; }
        if (type == MsgType::WORKER_DISPATCH) {
            WorkerDispatchPayload wdp;
            std::string uerr;
            if (!unpack_worker_dispatch(payload, wdp, uerr)) { err = "bad WORKER_DISPATCH: " + uerr; return false; }
            on_worker_dispatch(wdp);
        } else if (type == MsgType::PING) {
            conn_.send_frame(MsgType::PONG, payload, err);
        } else if (type == MsgType::SHUTDOWN) {
            return true;
        } else if (type == MsgType::COMPLETE_ACK || type == MsgType::DISPATCH_ACK) {
            // Acknowledgement for a previous START/COMPLETE. Not actionable here.
        }
    }
    return true;
}

void WorkerRuntime::on_worker_dispatch(const WorkerDispatchPayload& wdp) {
    // Build a start ticket from the dispatch ticket (identical leading fields).
    StartTicket st;
    st.execution_id = wdp.ticket.execution_id;
    st.execution_generation = wdp.ticket.execution_generation;
    st.attempt_id = wdp.ticket.attempt_id;
    st.attempt_generation = wdp.ticket.attempt_generation;
    st.worker_id = wdp.ticket.worker_id;
    st.worker_boot_id = wdp.ticket.worker_boot_id;
    st.epoch = wdp.ticket.epoch;
    st.dispatch_id = wdp.ticket.dispatch_id;
    st.dispatch_generation = wdp.ticket.dispatch_generation;
    st.ownership_generation = wdp.ticket.ownership_generation;
    st.fence_generation = wdp.ticket.fence_generation;

    // Signal we are running.
    StartPayload sp; sp.ticket = st;
    std::string err;
    conn_.send_frame(MsgType::START, pack_start(sp), err);

    // Execute physical work.
    Executor exec = executor_;
    std::vector<std::uint8_t> result;
    if (exec) { result = exec(wdp.work_bytes, wdp.work_seed); }
    else {
        // Fallback synthetic: digest of a generated buffer.
        std::vector<std::uint8_t> buf(4096, static_cast<std::uint8_t>(wdp.work_seed & 0xFF));
        for (std::size_t i = 0; i < buf.size(); ++i) { buf[i] = static_cast<std::uint8_t>((wdp.work_seed + i) & 0xFF); }
        result = buf;
    }

    // Report completion.
    CompletionTicket ct;
    ct.execution_id = wdp.ticket.execution_id;
    ct.execution_generation = wdp.ticket.execution_generation;
    ct.attempt_id = wdp.ticket.attempt_id;
    ct.attempt_generation = wdp.ticket.attempt_generation;
    ct.worker_id = wdp.ticket.worker_id;
    ct.worker_boot_id = wdp.ticket.worker_boot_id;
    ct.epoch = wdp.ticket.epoch;
    ct.dispatch_id = wdp.ticket.dispatch_id;
    ct.dispatch_generation = wdp.ticket.dispatch_generation;
    ct.ownership_generation = wdp.ticket.ownership_generation;
    ct.fence_generation = wdp.ticket.fence_generation;
    ct.completion_generation = CompletionGeneration(1);
    ct.has_result = true;
    ct.result_digest = ResultDigest::of_buffer(result);
    CompletePayload cp; cp.ticket = ct; cp.result = result;
    conn_.send_frame(MsgType::COMPLETE, pack_complete(cp), err);
}

}  // namespace execution_fabric
