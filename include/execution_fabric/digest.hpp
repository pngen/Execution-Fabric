#pragma once
#include "execution_fabric/sha256.hpp"
#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace execution_fabric {

// A deterministic fingerprint of a physical result payload. The digest is
// used to recognise duplicate identical completions idempotently and to detect
// conflicting completions that claim different results for the same logical
// execution generation.
class ResultDigest {
public:
    static ResultDigest of(const void* data, std::size_t n) noexcept {
        return ResultDigest(Sha256::digest(data, n));
    }
    static ResultDigest of_buffer(const std::vector<uint8_t>& v) noexcept {
        return ResultDigest(Sha256::digest(v.data(), v.size()));
    }
    static ResultDigest nil() noexcept { return ResultDigest{}; }
    static ResultDigest from_bytes(const Sha256::Digest& d) noexcept { return ResultDigest(d); }

    const Sha256::Digest& bytes() const noexcept { return bytes_; }
    bool is_nil() const noexcept {
        for (auto b : bytes_) { if (b != 0) { return false; } }
        return true;
    }
    bool operator==(const ResultDigest& o) const noexcept { return bytes_ == o.bytes_; }
    bool operator!=(const ResultDigest& o) const noexcept { return !(*this == o); }
    bool operator<(const ResultDigest& o) const noexcept { return bytes_ < o.bytes_; }
    std::string to_hex() const;

public:
    ResultDigest() noexcept = default;
    explicit ResultDigest(const Sha256::Digest& d) noexcept : bytes_(d) {}
    Sha256::Digest bytes_{};
};

}  // namespace execution_fabric