#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace execution_fabric {

// ---------------------------------------------------------------------------
// ByteWriter / ByteReader
//
// A small, deterministic little-endian binary codec. The reader is bounds
// checked: every read validates that the requested bytes are in range before
// copying, and records a sticky failure flag so a malformed length can never
// cause an out-of-bounds access or an unbounded allocation.
// ---------------------------------------------------------------------------
class ByteWriter {
public:
    void u8(std::uint8_t v) { buf_.push_back(v); }
    void u16(std::uint16_t v) {
        buf_.push_back(static_cast<std::uint8_t>(v));
        buf_.push_back(static_cast<std::uint8_t>(v >> 8));
    }
    void u32(std::uint32_t v) {
        for (int i = 0; i < 4; ++i) { buf_.push_back(static_cast<std::uint8_t>(v >> (8 * i))); }
    }
    void u64(std::uint64_t v) {
        for (int i = 0; i < 8; ++i) { buf_.push_back(static_cast<std::uint8_t>(v >> (8 * i))); }
    }
    void bytes(const void* p, std::size_t n) {
        const auto* b = static_cast<const std::uint8_t*>(p);
        buf_.insert(buf_.end(), b, b + n);
    }
    void bytes(const std::vector<std::uint8_t>& v) { bytes(v.data(), v.size()); }
    void string(const std::string& s) {
        u32(static_cast<std::uint32_t>(s.size()));
        bytes(s.data(), s.size());
    }
    void bool_byte(bool b) { u8(b ? 1 : 0); }

    const std::vector<std::uint8_t>& data() const noexcept { return buf_; }
    std::size_t size() const noexcept { return buf_.size(); }
    std::vector<std::uint8_t> take() && { return std::move(buf_); }

private:
    std::vector<std::uint8_t> buf_;
};

class ByteReader {
public:
    static constexpr std::size_t kMaxString = 1u << 20;   // 1 MiB
    static constexpr std::size_t kMaxVector = 1u << 24;   // 16 MiB

    ByteReader(const std::uint8_t* data, std::size_t len) noexcept : data_(data), len_(len) {}

    bool u8(std::uint8_t& out) noexcept {
        if (!need(1)) { return false; }
        out = data_[pos_]; ++pos_;
        return true;
    }
    bool u16(std::uint16_t& out) noexcept {
        if (!need(2)) { return false; }
        out = static_cast<std::uint16_t>(data_[pos_]) |
              (static_cast<std::uint16_t>(data_[pos_ + 1]) << 8);
        pos_ += 2;
        return true;
    }
    bool u32(std::uint32_t& out) noexcept {
        if (!need(4)) { return false; }
        out = static_cast<std::uint32_t>(data_[pos_]) |
              (static_cast<std::uint32_t>(data_[pos_ + 1]) << 8) |
              (static_cast<std::uint32_t>(data_[pos_ + 2]) << 16) |
              (static_cast<std::uint32_t>(data_[pos_ + 3]) << 24);
        pos_ += 4;
        return true;
    }
    bool u64(std::uint64_t& out) noexcept {
        if (!need(8)) { return false; }
        out = 0;
        for (int i = 0; i < 8; ++i) { out |= (static_cast<std::uint64_t>(data_[pos_ + i]) << (8 * i)); }
        pos_ += 8;
        return true;
    }
    bool bytes(void* out, std::size_t n) noexcept {
        if (!need(n)) { return false; }
        std::memcpy(out, data_ + pos_, n);
        pos_ += n;
        return true;
    }
    bool bytes(std::vector<std::uint8_t>& out, std::size_t n) noexcept {
        if (n > kMaxVector) { fail(); return false; }
        if (!need(n)) { return false; }
        out.assign(data_ + pos_, data_ + pos_ + n);
        pos_ += n;
        return true;
    }
    bool string(std::string& out) noexcept {
        std::uint32_t n = 0;
        if (!u32(n)) { return false; }
        if (n > kMaxString) { fail(); return false; }
        if (!need(n)) { return false; }
        out.assign(reinterpret_cast<const char*>(data_ + pos_), n);
        pos_ += n;
        return true;
    }
    bool skip(std::size_t n) noexcept {
        if (!need(n)) { return false; }
        pos_ += n;
        return true;
    }
    bool bool_byte(bool& out) noexcept {
        std::uint8_t b = 0;
        if (!u8(b)) { return false; }
        out = (b != 0);
        return true;
    }

    bool ok() const noexcept { return ok_; }
    std::size_t remaining() const noexcept { return len_ - pos_; }
    void fail() noexcept { ok_ = false; }

private:
    bool need(std::size_t n) const noexcept {
        if (!ok_) { return false; }
        if (n > (len_ - pos_)) { return false; }
        return true;
    }
    const std::uint8_t* data_;
    std::size_t len_;
    std::size_t pos_ = 0;
    bool ok_ = true;
};

}  // namespace execution_fabric