#pragma once
#include "protocol/protocol.hpp"
#include "protocol/transport.hpp"
#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace execution_fabric {

// A pluggable executor: given work parameters, produce a physical result
// payload. The synthetic executor produces a deterministic digest; the CUDA
// executor performs real device work.
using Executor = std::function<std::vector<std::uint8_t>(std::uint32_t work_bytes, std::uint32_t seed)>;

class WorkerRuntime {
public:
    WorkerRuntime(std::string host, std::uint16_t port, WorkerId worker_id, WorkerBootId boot);

    // Connect + register + wait for welcome. On success, the worker is ready.
    bool start(std::string& err);
    // Run the receive loop until shutdown. Returns false on transport error.
    bool run(std::string& err);
    void request_stop() noexcept { running_.store(false); }

    void set_executor(Executor exec) { executor_ = std::move(exec); }

private:
    void on_worker_dispatch(const WorkerDispatchPayload& wdp);

    std::string host_;
    std::uint16_t port_;
    WorkerId worker_id_;
    WorkerBootId boot_;
    net::TcpConnection conn_;
    Executor executor_;
    std::atomic<bool> running_{false};
    bool connected_ = false;
};

}  // namespace execution_fabric
