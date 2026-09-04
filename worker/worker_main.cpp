#include "worker/worker.hpp"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <chrono>

using namespace execution_fabric;

int main(int argc, char** argv) {
    std::uint32_t delay_ms = 0;
    std::string host = "127.0.0.1";
    std::uint16_t port = 3000;
    WorkerId worker_id = WorkerId::random();
    WorkerBootId boot = WorkerBootId::random();

    for (int i = 1; i < argc; ++i) {
        if (std::strncmp(argv[i], "--coordinator=", 14) == 0) {
            std::string spec = argv[i] + 14;
            const auto colon = spec.rfind(':');
            if (colon != std::string::npos) { host = spec.substr(0, colon); port = (std::uint16_t)std::atoi(spec.c_str() + colon + 1); }
            else host = spec;
        } else if (std::strncmp(argv[i], "--worker-id=", 12) == 0) {
            auto u = Uuid::parse(argv[i] + 12); if (u) worker_id = WorkerId(*u);
        } else if (std::strncmp(argv[i], "--boot-id=", 10) == 0) {
            auto u = Uuid::parse(argv[i] + 10); if (u) boot = WorkerBootId(*u);
        } else if (std::strncmp(argv[i], "--delay-ms=", 11) == 0) {
            delay_ms = static_cast<std::uint32_t>(std::atoi(argv[i] + 11));
        }
    }

    net::init();
    WorkerRuntime worker(host, port, worker_id, boot);
    // The synthetic executor performs a deterministic computation after an
    // optional delay (used by the proof harness to create a kill window).
    worker.set_executor([delay_ms](std::uint32_t work_bytes, std::uint32_t seed) {
        if (delay_ms > 0) { std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms)); }
        std::vector<std::uint8_t> buf(work_bytes);
        for (std::size_t i = 0; i < buf.size(); ++i) {
            buf[i] = static_cast<std::uint8_t>((seed + i * 31u) & 0xFF);
        }
        return buf;
    });
    std::string err;
    if (!worker.start(err)) {
        std::fprintf(stderr, "worker(%s): %s\n", worker_id.to_string().c_str(), err.c_str());
        net::cleanup();
        return 1;
    }
    std::printf("worker %s (boot %s) connected to %s:%u\n",
                worker_id.to_string().c_str(), boot.to_string().c_str(), host.c_str(), (unsigned)port);
    std::fflush(stdout);
    if (!worker.run(err)) {
        std::fprintf(stderr, "worker(%s): %s\n", worker_id.to_string().c_str(), err.c_str());
        net::cleanup();
        return 1;
    }
    net::cleanup();
    return 0;
}