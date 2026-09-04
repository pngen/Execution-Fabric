#include "coordinator/coordinator.hpp"
#include "execution_fabric/checksum.hpp"
#include "execution_fabric/state.hpp"
#include <algorithm>
#include <utility>

namespace execution_fabric {

Coordinator::Coordinator(std::string host, std::uint16_t port, std::string persistence_path,
                         CoordinatorEpoch epoch)
    : host_(std::move(host)), port_(port), persistence_path_(std::move(persistence_path)),
      epoch_(epoch), engine_(epoch) {
    if (!persistence_path_.empty()) {
        store_ = std::make_shared<FilePersistenceStore>(persistence_path_);
        std::vector<ExecutionRecord> recovered;
        CoordinatorEpoch rec_epoch(0);
        if (store_->load(recovered, rec_epoch)) {
            engine_.replace_records(std::move(recovered));
            if (rec_epoch > epoch_) { epoch_ = rec_epoch; engine_.raise_epoch(epoch_); }
        }
    }
}

Coordinator::~Coordinator() { stop(); }

bool Coordinator::start(std::string& err) {
    if (!listener_.listen(host_, port_, err)) { return false; }
    port_ = listener_.bound_port();
    running_ = true;
    acceptor_thread_ = std::thread([this]() { acceptor_loop(); });
    decision_thread_ = std::thread([this]() { decision_loop(); });
    return true;
}

void Coordinator::stop() {
    if (!running_.exchange(false)) { return; }
    listener_.close();
    if (acceptor_thread_.joinable()) { acceptor_thread_.join(); }
    // Wake the decision thread.
    {
        std::lock_guard<std::mutex> lk(q_mu_);
        queue_.push_back(Inbound{0, MsgType::SHUTDOWN, {}});
    }
    q_cv_.notify_one();
    if (decision_thread_.joinable()) { decision_thread_.join(); }
    for (auto& th : reader_threads_) { if (th.joinable()) { th.join(); } }
    reader_threads_.clear();
}

bool Coordinator::send_to(std::uint64_t conn_id, MsgType type, const std::vector<std::uint8_t>& payload) {
    std::lock_guard<std::mutex> lk(conns_mu_);
    const auto it = conns_.find(conn_id);
    if (it == conns_.end() || !it->second.conn) { return false; }
    std::string err;
    return it->second.conn->send_frame(type, payload, err);
}

void Coordinator::acceptor_loop() {
    while (running_.load()) {
        std::string err;
        auto conn = std::make_shared<net::TcpConnection>();
        if (!listener_.accept(*conn, err)) {
            if (!running_.load()) { break; }
            continue;
        }
        const std::uint64_t id = next_conn_id_++;
        {
            std::lock_guard<std::mutex> lk(conns_mu_);
            conns_[id] = ConnEntry{conn, false, WorkerId{}, WorkerBootId{}, std::nullopt, std::nullopt};
        }
        reader_threads_.emplace_back([this, id, conn]() { reader_loop(id, conn); });
    }
}

void Coordinator::reader_loop(std::uint64_t conn_id, std::shared_ptr<net::TcpConnection> conn) {
    const auto keep = conn;
    while (running_.load()) {
        MsgType type;
        std::vector<std::uint8_t> payload;
        std::string err;
        if (!conn->recv_frame(type, payload, err)) {
            break;
        }
        {
            std::lock_guard<std::mutex> lk(q_mu_);
            Inbound in; in.conn_id = conn_id; in.type = type; in.payload = std::move(payload);
            queue_.push_back(std::move(in));
        }
        q_cv_.notify_one();
    }
    // Connection closed.
    {
        std::lock_guard<std::mutex> lk(q_mu_);
        Inbound in; in.conn_id = conn_id;
        // Marker for closure: use a special type by wrapping in SHUTDOWN? We use a flag.
        in.type = MsgType::SHUTDOWN;
        in.payload = std::vector<std::uint8_t>{0xFF, 0xFF, 0xFF, 0xFF};
        queue_.push_back(std::move(in));
    }
    q_cv_.notify_one();
}

void Coordinator::persist() {
    if (store_) { store_->save(engine_.records(), epoch_); }
}

void Coordinator::decision_loop() {
    while (true) {
        Inbound in;
        {
            std::unique_lock<std::mutex> lk(q_mu_);
            q_cv_.wait(lk, [this]() { return !queue_.empty(); });
            in = std::move(queue_.front());
            queue_.erase(queue_.begin());
        }
        if (in.type == MsgType::SHUTDOWN && in.payload.size() == 4 &&
            in.payload[0] == 0xFF && in.payload[1] == 0xFF && in.payload[2] == 0xFF && in.payload[3] == 0xFF) {
            // Connection-closed marker.
            on_conn_closed(in.conn_id);
            continue;
        }
        handle_frame(in.conn_id, in.type, in.payload);
    }
}

void Coordinator::on_conn_closed(std::uint64_t conn_id) {
    ConnEntry entry;
    {
        std::lock_guard<std::mutex> lk(conns_mu_);
        const auto it = conns_.find(conn_id);
        if (it == conns_.end()) { return; }
        entry = it->second;
        conns_.erase(it);
    }
    if (entry.is_worker && entry.active_execution) {
        // The worker that owned the current attempt is gone: classify loss.
        Decision d = engine_.mark_worker_lost(*entry.active_execution, entry.worker_id, entry.boot, epoch_);
        (void)d;
        persist();
    }
}

void Coordinator::handle_frame(std::uint64_t conn_id, MsgType type, const std::vector<std::uint8_t>& payload) {
    std::string err;

    if (type == MsgType::HELLO) {
        HelloPayload hp;
        if (!unpack_hello(payload, hp, err)) { return; }
        {
            std::lock_guard<std::mutex> lk(conns_mu_);
            conns_[conn_id].is_worker = (hp.role == 1);
            conns_[conn_id].worker_id = hp.worker_id;
            conns_[conn_id].boot = hp.worker_boot_id;
            if (hp.role == 1) {
                // Welcome a fresh worker incarnation.
                conns_[conn_id].boot = hp.worker_boot_id;
            }
        }
        WelcomePayload wp;
        wp.epoch = epoch_;
        wp.coordinator_id = "ExecutionFabric-coordinator";
        wp.protocol_version = kFrameVersion;
        send_to(conn_id, MsgType::WELCOME, pack_welcome(wp));
        return;
    }

    if (type == MsgType::REGISTER_WORKER) {
        // Re-registration / explicit register.
        RegisterAckPayload ack; ack.accepted = true; ack.reason = "registered";
        send_to(conn_id, MsgType::REGISTER_ACK, pack_register_ack(ack));
        return;
    }

    if (type == MsgType::SUBMIT) {
        SubmitPayload sp;
        if (!unpack_submit(payload, sp, err)) { return; }
        Decision d = engine_.create(sp.execution_id, sp.epoch);
        if (d.code == DecisionCode::ALLOW) {
            d = engine_.activate(sp.execution_id, sp.epoch);
        }
        SubmitAckPayload ack;
        ack.accepted = (d.code == DecisionCode::ALLOW);
        ack.execution_id = sp.execution_id;
        const ExecutionRecord* rec = engine_.find(sp.execution_id);
        if (rec) { ack.generation = rec->generation; ack.state = rec->state; }
        ack.reason = d.to_string();
        persist();
        send_to(conn_id, MsgType::SUBMIT_ACK, pack_submit_ack(ack));
        return;
    }

    if (type == MsgType::DISPATCH) {
        DispatchRequestPayload dp;
        if (!unpack_dispatch(payload, dp, err)) { return; }
        DispatchAckPayload ack;
        std::uint64_t worker_conn = 0;
        {
            std::lock_guard<std::mutex> lk(conns_mu_);
            for (const auto& kv : conns_) {
                if (kv.second.is_worker && kv.second.worker_id == dp.worker_id) { worker_conn = kv.first; break; }
            }
        }
        if (worker_conn == 0) {
            ack.accepted = false; ack.decision = DecisionCode::REJECT_NOT_OWNER;
            ack.reason = "worker not connected";
            send_to(conn_id, MsgType::DISPATCH_ACK, pack_dispatch_ack(ack));
            return;
        }
        Decision d = engine_.dispatch(dp.execution_id, dp.worker_id, dp.worker_boot_id, dp.epoch, ack.ticket);
        ack.accepted = (d.code == DecisionCode::ALLOW);
        ack.decision = d.code;
        ack.reason = d.to_string();
        if (ack.accepted) {
            WorkerDispatchPayload wdp;
            wdp.ticket = ack.ticket;
            wdp.work_bytes = 4096;   // default synthetic work size
            wdp.work_seed = 0;
            {
                std::lock_guard<std::mutex> lk(conns_mu_);
                if (conns_.count(worker_conn)) {
                    conns_[worker_conn].active_execution = ack.ticket.execution_id;
                    conns_[worker_conn].active_attempt = ack.ticket.attempt_id;
                }
            }
            send_to(worker_conn, MsgType::WORKER_DISPATCH, pack_worker_dispatch(wdp));
            persist();
        }
        send_to(conn_id, MsgType::DISPATCH_ACK, pack_dispatch_ack(ack));
        return;
    }

    if (type == MsgType::WORKER_DISPATCH_ACK) { return; }

    if (type == MsgType::START) {
        StartPayload sp;
        if (!unpack_start(payload, sp, err)) { return; }
        Decision d = engine_.mark_running(sp.ticket);
        send_to(conn_id, MsgType::COMPLETE_ACK, pack_decision(d));
        return;
    }

    if (type == MsgType::COMPLETE) {
        CompletePayload cp;
        if (!unpack_complete(payload, cp, err)) { return; }
        if (cp.ticket.has_result && cp.ticket.result_digest.is_nil()) {
            cp.ticket.result_digest = ResultDigest::of_buffer(cp.result);
        }
        Decision d = engine_.complete(cp.ticket);
        CompleteAckPayload ack;
        ack.decision = d.code;
        ack.reason = d.to_string();
        ack.committed = (d.code == DecisionCode::ALLOW);
        if (d.code == DecisionCode::ALLOW) {
            const ExecutionRecord* rec = engine_.find(cp.ticket.execution_id);
            if (rec && rec->committed_digest) { ack.committed_digest = *rec->committed_digest; }
            persist();
        }
        send_to(conn_id, MsgType::COMPLETE_ACK, pack_complete_ack(ack));
        return;
    }

    if (type == MsgType::CANCEL) {
        CancelPayload cp;
        if (!unpack_cancel(payload, cp, err)) { return; }
        Decision d = engine_.cancel(cp.request);
        persist();
        send_to(conn_id, MsgType::CANCEL_ACK, pack_decision(d));
        return;
    }

    if (type == MsgType::PREEMPT) {
        CancelPayload cp;
        if (!unpack_cancel(payload, cp, err)) { return; }
        Decision d = engine_.preempt(cp.request);
        persist();
        send_to(conn_id, MsgType::PREEMPT_ACK, pack_decision(d));
        return;
    }

    if (type == MsgType::ACK_PREEMPT) {
        AckPreemptPayload ap;
        if (!unpack_ack_preempt(payload, ap, err)) { return; }
        Decision d = engine_.acknowledge_preempt(ap.execution_id, ap.epoch);
        persist();
        send_to(conn_id, MsgType::PREEMPT_ACK, pack_decision(d));
        return;
    }

    if (type == MsgType::MARK_RESUMABLE) {
        MarkResumablePayload mp;
        if (!unpack_mark_resumable(payload, mp, err)) { return; }
        Decision d = engine_.mark_resumable(mp.execution_id, mp.epoch);
        persist();
        send_to(conn_id, MsgType::PREEMPT_ACK, pack_decision(d));
        return;
    }

    if (type == MsgType::RESUME) {
        ResumeRequestPayload rp;
        if (!unpack_resume(payload, rp, err)) { return; }
        DispatchAckPayload ack;
        std::uint64_t worker_conn = 0;
        {
            std::lock_guard<std::mutex> lk(conns_mu_);
            for (const auto& kv : conns_) {
                if (kv.second.is_worker && kv.second.worker_id == rp.worker_id) { worker_conn = kv.first; break; }
            }
        }
        Decision d = engine_.resume(rp.execution_id, rp.worker_id, rp.worker_boot_id, rp.epoch,
                                    rp.resume_gen, ack.ticket);
        ack.accepted = (d.code == DecisionCode::RESUME_ALLOWED || d.code == DecisionCode::ALLOW);
        ack.decision = d.code;
        ack.reason = d.to_string();
        if (ack.decision == DecisionCode::RESUME_ALLOWED && worker_conn != 0) {
            WorkerDispatchPayload wdp; wdp.ticket = ack.ticket; wdp.work_bytes = 4096; wdp.work_seed = 7;
            send_to(worker_conn, MsgType::WORKER_DISPATCH, pack_worker_dispatch(wdp));
            persist();
        }
        send_to(conn_id, MsgType::RESUME_ACK, pack_dispatch_ack(ack));
        return;
    }

    if (type == MsgType::WORKER_LOST) {
        WorkerLostPayload wp;
        if (!unpack_worker_lost(payload, wp, err)) { return; }
        Decision d = engine_.mark_worker_lost(wp.execution_id, wp.worker_id, wp.worker_boot_id, wp.epoch);
        persist();
        send_to(conn_id, MsgType::COMPLETE_ACK, pack_decision(d));
        return;
    }

    if (type == MsgType::QUERY) {
        QueryPayload qp;
        if (!unpack_query(payload, qp, err)) { return; }
        QueryResponsePayload qr;
        const ExecutionRecord* rec = engine_.find(qp.execution_id);
        qr.execution_id = qp.execution_id;
        if (rec) {
            qr.found = true;
            qr.state = rec->state;
            qr.generation = rec->generation;
            qr.epoch = rec->coordinator_epoch;
            qr.current_attempt = rec->current_attempt_id;
            qr.current_attempt_generation = rec->current_attempt_generation;
            qr.attempt_count = rec->attempts.size();
            qr.committed_digest = rec->committed_digest;
            qr.detail = "state=" + std::string(to_string(rec->state));
        } else {
            qr.detail = "not found";
        }
        send_to(conn_id, MsgType::QUERY_RESPONSE, pack_query_response(qr));
        return;
    }

    if (type == MsgType::PING) {
        send_to(conn_id, MsgType::PONG, payload);
        return;
    }

    if (type == MsgType::SHUTDOWN) {
        running_ = false;
        return;
    }
}

}  // namespace execution_fabric
