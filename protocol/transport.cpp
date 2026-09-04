#include "protocol/transport.hpp"
#include <cstring>

namespace execution_fabric::net {

namespace {
#ifdef _WIN32
using sock_t = SOCKET;
constexpr sock_t kBadSock = INVALID_SOCKET;
bool close_sock(sock_t s) { return ::closesocket(s) == 0; }
bool would_block() { return WSAGetLastError() == WSAEWOULDBLOCK; }
#else
using sock_t = int;
constexpr sock_t kBadSock = -1;
bool close_sock(sock_t s) { return ::close(s) == 0; }
bool would_block() { return errno == EAGAIN || errno == EWOULDBLOCK; }
#endif

sock_t make_address(const std::string& host, std::uint16_t port, sockaddr_in& addr) {
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (host.empty() || host == "0.0.0.0") { addr.sin_addr.s_addr = INADDR_ANY; }
    else { ::inet_pton(AF_INET, host.c_str(), &addr.sin_addr); }
    return static_cast<sock_t>(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
}
}  // namespace

void init() {
#ifdef _WIN32
    WSADATA data;
    ::WSAStartup(MAKEWORD(2, 2), &data);
#endif
}

void cleanup() {
#ifdef _WIN32
    ::WSACleanup();
#endif
}

// ---------------------------------------------------------------------------
// TcpConnection
// ---------------------------------------------------------------------------
TcpConnection::TcpConnection(std::uintptr_t fd) : fd_(fd) {}

TcpConnection::TcpConnection(TcpConnection&& other) noexcept : fd_(other.fd_) { other.fd_ = kInvalid; }
TcpConnection& TcpConnection::operator=(TcpConnection&& other) noexcept {
    if (this != &other) { close(); fd_ = other.fd_; other.fd_ = kInvalid; }
    return *this;
}
TcpConnection::~TcpConnection() { close(); }

void TcpConnection::close() noexcept {
    if (fd_ != kInvalid) { close_sock(static_cast<sock_t>(fd_)); fd_ = kInvalid; }
}

bool TcpConnection::connect(const std::string& host, std::uint16_t port, std::string& err) {
    close();
    sockaddr_in addr;
    sock_t s = make_address(host, port, addr);
    if (s == kBadSock) { err = "socket() failed"; return false; }
    if (::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
#ifdef _WIN32
        err = "connect() failed with error " + std::to_string(WSAGetLastError());
#else
        err = "connect() failed";
#endif
        close_sock(s); return false;
    }
    fd_ = static_cast<std::uintptr_t>(s);
    return true;
}

bool TcpConnection::send_all(const std::uint8_t* data, std::size_t n, bool& closed) {
    if (fd_ == kInvalid) { closed = true; return false; }
    std::size_t off = 0;
    while (off < n) {
#ifdef _WIN32
        const int sent = ::send(static_cast<sock_t>(fd_), reinterpret_cast<const char*>(data + off),
                                static_cast<int>(n - off), 0);
#else
        const ssize_t sent = ::send(static_cast<sock_t>(fd_), data + off, n - off, MSG_NOSIGNAL);
#endif
        if (sent == 0) { closed = true; return false; }
        if (sent < 0) {
            if (would_block()) { continue; }
            closed = true; return false;
        }
        off += static_cast<std::size_t>(sent);
    }
    return true;
}

bool TcpConnection::recv_exact(std::uint8_t* data, std::size_t n, bool& closed) {
    if (fd_ == kInvalid) { closed = true; return false; }
    std::size_t off = 0;
    while (off < n) {
#ifdef _WIN32
        const int r = ::recv(static_cast<sock_t>(fd_), reinterpret_cast<char*>(data + off),
                             static_cast<int>(n - off), 0);
#else
        const ssize_t r = ::recv(static_cast<sock_t>(fd_), data + off, n - off, 0);
#endif
        if (r == 0) { closed = true; return false; }
        if (r < 0) {
            if (would_block()) { continue; }
            closed = true; return false;
        }
        off += static_cast<std::size_t>(r);
    }
    return true;
}

bool TcpConnection::send_frame(MsgType type, const std::vector<std::uint8_t>& payload, std::string& err) {
    const auto frame = encode_frame(type, payload);
    bool closed = false;
    if (!send_all(frame.data(), frame.size(), closed)) {
        err = closed ? "connection closed while sending" : "partial send failure";
        return false;
    }
    return true;
}

bool TcpConnection::recv_frame(MsgType& type, std::vector<std::uint8_t>& payload, std::string& err) {
    std::uint8_t hdr[kFrameHeaderLen];
    bool closed = false;
    if (!recv_exact(hdr, kFrameHeaderLen, closed)) {
        err = closed ? "connection closed while receiving header" : "partial receive failure";
        return false;
    }
    std::size_t consumed = 0;
    DecodedFrame frame;
    std::string derr;
    ByteReader hr(hdr, kFrameHeaderLen);
    std::uint32_t magic = 0; std::uint8_t ver = 0, ty = 0; std::uint32_t len = 0; std::uint64_t crc = 0;
    if (!hr.u32(magic) || !hr.u8(ver) || !hr.u8(ty) || !hr.u32(len) || !hr.u64(crc)) {
        err = "malformed frame header"; return false;
    }
    if (magic != kFrameMagic) { err = "bad frame magic"; return false; }
    if (ver != kFrameVersion) { err = "bad frame version"; return false; }
    if (len > kMaxPayload) { err = "frame payload exceeds limit"; return false; }
    // Read the payload, then decode the complete frame in one shot so that a
    // hostile length that would exceed the remaining stream can never trigger
    // an over-read: the length is bounded above, and recv_exact reads exactly
    // that many bytes.
    std::vector<std::uint8_t> payload_buf(len);
    if (len > 0) {
        if (!recv_exact(payload_buf.data(), len, closed)) {
            err = closed ? "connection closed while receiving payload" : "partial payload receive failure";
            return false;
        }
    }
    std::vector<std::uint8_t> full;
    full.reserve(kFrameHeaderLen + len);
    full.insert(full.end(), hdr, hdr + kFrameHeaderLen);
    full.insert(full.end(), payload_buf.begin(), payload_buf.end());
    if (!decode_frame(full.data(), full.size(), frame, consumed, derr)) { err = derr; return false; }
    type = frame.type;
    payload = std::move(frame.payload);
    return true;
}

// ---------------------------------------------------------------------------
// TcpListener
// ---------------------------------------------------------------------------
TcpListener::~TcpListener() { close(); }

bool TcpListener::listen(const std::string& host, std::uint16_t port, std::string& err) {
    close();
    sockaddr_in addr;
    sock_t s = make_address(host, port, addr);
    if (s == kBadSock) { err = "socket() failed"; return false; }
    // Allow quick reuse.
    int yes = 1;
    ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&yes), sizeof(yes));
    if (::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
#ifdef _WIN32
        err = "bind() failed with error " + std::to_string(WSAGetLastError());
#else
        err = "bind() failed";
#endif
        close_sock(s); return false;
    }
    if (::listen(s, SOMAXCONN) != 0) { err = "listen() failed"; close_sock(s); return false; }
    // Retrieve the actual bound port (for port 0).
    sockaddr_in bound; socklen_t blen = sizeof(bound);
    if (::getsockname(s, reinterpret_cast<sockaddr*>(&bound), &blen) == 0) {
        port_ = ntohs(bound.sin_port);
    }
    fd_ = static_cast<std::uintptr_t>(s);
    return true;
}

bool TcpListener::accept(TcpConnection& conn, std::string& err) {
    if (fd_ == kInvalid) { err = "listener not open"; return false; }
    sockaddr_in cli; socklen_t clen = sizeof(cli);
    sock_t c = ::accept(static_cast<sock_t>(fd_), reinterpret_cast<sockaddr*>(&cli), &clen);
    if (c == kBadSock) {
#ifdef _WIN32
        err = "accept() failed with error " + std::to_string(WSAGetLastError());
#else
        err = "accept() failed";
#endif
        return false;
    }
    conn.close();
    conn.fd_ = static_cast<std::uintptr_t>(c);
    return true;
}

void TcpListener::close() noexcept {
    if (fd_ != kInvalid) { close_sock(static_cast<sock_t>(fd_)); fd_ = kInvalid; }
}

}  // namespace execution_fabric::net