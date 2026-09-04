#pragma once
// Minimal cross-platform child-process control for the multiprocess proof.
#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <process.h>
#endif
#include <string>
#include <vector>

namespace eftest {

class Process {
public:
    Process() = default;
    Process(const Process&) = delete;
    Process& operator=(const Process&) = delete;
    ~Process() { kill(); }

    // Spawn exe with args. Returns true if started.
    bool spawn(const std::string& exe, const std::vector<std::string>& args);
    // Hard-kill the process.
    void kill();
    bool alive() const;
    int wait();

private:
#ifdef _WIN32
    PROCESS_INFORMATION pi_{};
    bool started_ = false;
#else
    int pid_ = -1;
#endif
};

bool find_free_port(std::uint16_t& port);

}  // namespace eftest
