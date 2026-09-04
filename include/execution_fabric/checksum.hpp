#pragma once
#include <array>
#include <cstddef>
#include <cstdint>

namespace execution_fabric {

// CRC-64 (ECMA-182) with a software table. Deterministic on all platforms and
// endiannesses for a given byte sequence, so persisted frames and wire frames
// agree wherever they are validated.
class Crc64 {
public:
    explicit Crc64(std::uint64_t seed = ~0ull) : state_(seed) {}

    Crc64& update(const std::uint8_t* data, std::size_t n) noexcept {
        for (std::size_t i = 0; i < n; ++i) { state_ = table_[(state_ ^ data[i]) & 0xFFull] ^ (state_ >> 8); }
        return *this;
    }
    Crc64& update(const void* data, std::size_t n) noexcept {
        return update(static_cast<const std::uint8_t*>(data), n);
    }

    std::uint64_t digest() const noexcept { return state_; }

    static std::uint64_t compute(const void* data, std::size_t n) noexcept {
        return Crc64().update(data, n).digest();
    }

private:
    static const std::array<std::uint64_t, 256> table_;
    std::uint64_t state_;
};

}  // namespace execution_fabric