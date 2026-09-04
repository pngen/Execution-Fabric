#pragma once
#include "protocol/protocol.hpp"
#include <cstdint>
#include <string>
#include <vector>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace execution_fabric::net {

// One-time Winsock / system initialisation.
void init();
void cleanup();

// ---------------------------------------------------------------------------
// TcpConnection
//
// A single framing connection. send_frame forwards exactly one frame and
// recv_frame reads exactly one frame, both with partial-send / partial-receive
// handling so that a hostile or slow peer cannot corrupt the stream.
// ---------------------------------------------------------------------------
class TcpConnection {
public:
    TcpConnection() = default;
    explicit TcpConnection(std::uintptr_t fd);
    TcpConnection(const TcpConnection&) = delete;
    TcpConnection& operator=(const TcpConnection&) = delete;
    TcpConnection(TcpConnection&& other) noexcept;
    TcpConnection& operator=(TcpConnection&& other) noexcept;
    ~TcpConnection();

    bool connect(const std::string& host, std::uint16_t port, std::string& err);
    bool send_frame(MsgType type, const std::vector<std::uint8_t>& payload, std::string& err);
    bool recv_frame(MsgType& type, std::vector<std::uint8_t>& payload, std::string& err);
    void close() noexcept;
    bool is_open() const noexcept { return fd_ != kInvalid; }

private:
    bool send_all(const std::uint8_t* data, std::size_t n, bool& closed);
    bool recv_exact(std::uint8_t* data, std::size_t n, bool& closed);

    friend class TcpListener;
    static constexpr std::uintptr_t kInvalid = static_cast<std::uintptr_t>(-1);
    std::uintptr_t fd_ = kInvalid;
};

// ---------------------------------------------------------------------------
// TcpListener
// ---------------------------------------------------------------------------
class TcpListener {
public:
    TcpListener() = default;
    TcpListener(const TcpListener&) = delete;
    TcpListener& operator=(const TcpListener&) = delete;
    ~TcpListener();

    bool listen(const std::string& host, std::uint16_t port, std::string& err);
    bool accept(TcpConnection& conn, std::string& err);
    void close() noexcept;
    std::uint16_t bound_port() const noexcept { return port_; }
    bool is_open() const noexcept { return fd_ != kInvalid; }

private:
    static constexpr std::uintptr_t kInvalid = static_cast<std::uintptr_t>(-1);
    std::uintptr_t fd_ = kInvalid;
    std::uint16_t port_ = 0;
};

}  // namespace execution_fabric::net