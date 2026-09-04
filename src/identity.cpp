#include "execution_fabric/identity.hpp"
#include <array>
#include <random>

namespace execution_fabric {

namespace {
std::mt19937_64& prng() {
    thread_local std::mt19937_64 rng{[]() {
        std::random_device rd;
        const std::uint64_t seed =
            (static_cast<std::uint64_t>(rd()) << 32) ^ static_cast<std::uint64_t>(rd());
        return std::mt19937_64{seed};
    }()};
    return rng;
}
}  // namespace

Uuid Uuid::random() {
    std::array<uint8_t, 16> b{};
    auto& rng = prng();
    for (int i = 0; i < 16; i += 8) {
        const std::uint64_t v = rng();
        b[i + 0] = static_cast<uint8_t>(v);
        b[i + 1] = static_cast<uint8_t>(v >> 8);
        b[i + 2] = static_cast<uint8_t>(v >> 16);
        b[i + 3] = static_cast<uint8_t>(v >> 24);
        b[i + 4] = static_cast<uint8_t>(v >> 32);
        b[i + 5] = static_cast<uint8_t>(v >> 40);
        b[i + 6] = static_cast<uint8_t>(v >> 48);
        b[i + 7] = static_cast<uint8_t>(v >> 56);
    }
    b[6] = static_cast<uint8_t>((b[6] & 0x0F) | 0x40);  // version 4
    b[8] = static_cast<uint8_t>((b[8] & 0x3F) | 0x80);  // variant 10
    return from_bytes(b);
}

std::string Uuid::to_string() const {
    static const char* hex = "0123456789abcdef";
    std::string out;
    out.reserve(36);
    for (int i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) { out.push_back('-'); }
        out.push_back(hex[bytes_[i] >> 4]);
        out.push_back(hex[bytes_[i] & 0x0F]);
    }
    return out;
}

std::optional<Uuid> Uuid::parse(std::string_view s) noexcept {
    std::array<uint8_t, 16> b{};
    int n = 0;
    for (char c : s) {
        if (c == '-') { continue; }
        int v;
        if (c >= '0' && c <= '9') { v = c - '0'; }
        else if (c >= 'a' && c <= 'f') { v = c - 'a' + 10; }
        else if (c >= 'A' && c <= 'F') { v = c - 'A' + 10; }
        else { return std::nullopt; }
        if (n >= 32) { return std::nullopt; }
        if ((n & 1) == 0) { b[n / 2] = static_cast<uint8_t>(v << 4); }
        else { b[n / 2] = static_cast<uint8_t>(b[n / 2] | (v & 0x0F)); }
        ++n;
    }
    if (n != 32) { return std::nullopt; }
    return Uuid::from_bytes(b);
}

}  // namespace execution_fabric