#pragma once
#include "execution_fabric/engine.hpp"
#include "execution_fabric/persistence.hpp"
#include "protocol/protocol.hpp"
#include "protocol/transport.hpp"
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace execution_fabric {

// ---------------------------------------------------------------------------
// Coordinator
//
// The authoritative runtime process. It owns the single ExecutionEngine (one
// decision thread), the durable persistence store, and the set of connected
// workers and controllers. All authority decisions are serialised through the
// single decision thread, so winner semantics are deterministic. Network IO
// is genuinely concurrent: one reader thread per client, one decision thread.
// ---------------------------------------------------------------------------
class Coordinator {
public:
    Coordinator(std::string host, std::uint16_t port, std::string persistence_path,
                CoordinatorEpoch epoch);
    ~Coordinator();

    Coordinator(const Coordinator&) = delete;
    Coordinator& operator=(const Coordinator&) = delete;

    bool start(std::string& err);
    void stop();
    std::uint16_t port() const noexcept { return port_; }

    // Introspection for the proof harness / tests.
    const ExecutionEngine& engine() const noexcept { return engine_; }
    bool is_running() const noexcept { return running_.load(); }

private:
    struct ConnEntry {
        std::shared_ptr<net::TcpConnection> conn;
        bool is_worker = false;
        WorkerId worker_id;
        WorkerBootId boot;
        std::optional<ExecutionId> active_execution;
        std::optional<AttemptId> active_attempt;
    };
    struct Inbound {
        std::uint64_t conn_id = 0;
        MsgType type = MsgType::HELLO;
        std::vector<std::uint8_t> payload;
    };

    void acceptor_loop();
    void reader_loop(std::uint64_t conn_id, std::shared_ptr<net::TcpConnection> conn);
    void decision_loop();
    void handle_frame(std::uint64_t conn_id, MsgType type, const std::vector<std::uint8_t>& payload);
    void on_conn_closed(std::uint64_t conn_id);

    void persist();
    bool send_to(std::uint64_t conn_id, MsgType type, const std::vector<std::uint8_t>& payload);

    std::string host_;
    std::uint16_t port_;
    std::string persistence_path_;
    CoordinatorEpoch epoch_;
    ExecutionEngine engine_;
    std::shared_ptr<FilePersistenceStore> store_;

    std::atomic<bool> running_{false};
    net::TcpListener listener_;
    std::thread acceptor_thread_;
    std::vector<std::thread> reader_threads_;

    std::mutex conns_mu_;
    std::uint64_t next_conn_id_ = 1;
    std::unordered_map<std::uint64_t, ConnEntry> conns_;

    // Message queue shared between reader threads and the decision thread.
    std::mutex q_mu_;
    std::condition_variable q_cv_;
    std::vector<Inbound> queue_;
    std::thread decision_thread_;

    std::atomic<std::uint64_t> connection_epoch_{0};
};

}  // namespace execution_fabric