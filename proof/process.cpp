#include "proof/process.hpp"
#include <chrono>
#include <cstdio>
#include <thread>

#ifdef _WIN32
#include <winsock2.h>
#endif

namespace eftest {

bool Process::spawn(const std::string& exe, const std::vector<std::string>& args) {
#ifdef _WIN32
    std::string cmd;
    cmd += "\"" + exe + "\"";
    for (const auto& a : args) {
        cmd += " \"" + a + "\"";
    }
    STARTUPINFOA si{};
    si.cb = sizeof(si);
    si.dwFlags = STARTF_USESHOWWINDOW;
    si.wShowWindow = SW_HIDE;
    std::vector<char> cmdline(cmd.begin(), cmd.end());
    cmdline.push_back('\0');
    if (!::CreateProcessA(nullptr, cmdline.data(), nullptr, nullptr, FALSE,
                          CREATE_NO_WINDOW, nullptr, nullptr, &si, &pi_)) {
        std::fprintf(stderr, "Process::spawn failed for %s\n", exe.c_str());
        return false;
    }
    started_ = true;
    return true;
#else
    std::vector<std::string> argv{exe};
    for (const auto& a : args) { argv.push_back(a); }
    std::vector<char*> cargv;
    for (auto& a : argv) { cargv.push_back(const_cast<char*>(a.c_str())); }
    cargv.push_back(nullptr);
    pid_ = fork();
    if (pid_ == 0) { execv(exe.c_str(), cargv.data()); _exit(127); }
    return pid_ > 0;
#endif
}

void Process::kill() {
#ifdef _WIN32
    if (started_) {
        if (pi_.hProcess) { ::TerminateProcess(pi_.hProcess, 0); }
    }
#else
    if (pid_ > 0) { ::kill(pid_, SIGKILL); }
#endif
}

bool Process::alive() const {
#ifdef _WIN32
    if (!started_) { return false; }
    return ::WaitForSingleObject(pi_.hProcess, 0) == WAIT_TIMEOUT;
#else
    if (pid_ <= 0) { return false; }
    if (::kill(pid_, 0) == 0) { return true; }
    return false;
#endif
}

int Process::wait() {
#ifdef _WIN32
    if (!started_) { return -1; }
    ::WaitForSingleObject(pi_.hProcess, INFINITE);
    DWORD code = 0;
    ::GetExitCodeProcess(pi_.hProcess, &code);
    started_ = false;
    return static_cast<int>(code);
#else
    if (pid_ <= 0) { return -1; }
    int status = 0;
    ::waitpid(pid_, &status, 0);
    pid_ = -1;
    return status;
#endif
}

bool find_free_port(std::uint16_t& port) {
#ifdef _WIN32
    SOCKET s = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == INVALID_SOCKET) { return false; }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = 0;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::closesocket(s); return false;
    }
    int len = sizeof(addr);
    if (::getsockname(s, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::closesocket(s); return false;
    }
    port = ntohs(addr.sin_port);
    ::closesocket(s);
    return true;
#else
    return false;
#endif
}

}  // namespace eftest
