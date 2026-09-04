#include "coordinator/coordinator.hpp"
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <chrono>

using namespace execution_fabric;

static Coordinator* g_coordinator = nullptr;
static void on_signal(int) { if (g_coordinator) { g_coordinator->stop(); } }

int main(int argc, char** argv) {
    std::string host = "127.0.0.1";
    std::uint16_t port = 3000;
    std::string persist;
    CoordinatorEpoch epoch(1);

    for (int i = 1; i < argc; ++i) {
        if (std::strncmp(argv[i], "--host=", 7) == 0) host = argv[i] + 7;
        else if (std::strncmp(argv[i], "--port=", 7) == 0) port = static_cast<std::uint16_t>(std::atoi(argv[i] + 7));
        else if (std::strncmp(argv[i], "--persist=", 10) == 0) persist = argv[i] + 10;
        else if (std::strncmp(argv[i], "--epoch=", 8) == 0) epoch = CoordinatorEpoch(std::strtoull(argv[i] + 8, nullptr, 10));
    }

    net::init();
    Coordinator coord(host, port, persist, epoch);
    g_coordinator = &coord;
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    std::string err;
    if (!coord.start(err)) {
        std::fprintf(stderr, "coordinator: %s\n", err.c_str());
        net::cleanup();
        return 1;
    }
    std::printf("coordinator running on %s:%u (epoch=%llu, persist=%s)\n",
                host.c_str(), (unsigned)coord.port(), (unsigned long long)epoch.value(),
                persist.empty() ? "(none)" : persist.c_str());
    std::fflush(stdout);

    // Wait for the listener / decision threads to finish (SIGINT triggers stop).
    while (coord.is_running()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    net::cleanup();
    return 0;
}