#pragma once
#include <array>
#include <cstdint>
#include <functional>
#include <optional>
#include <ostream>
#include <random>
#include <string>
#include <string_view>

namespace execution_fabric {

// ---------------------------------------------------------------------------
// Uuid
//
// A 128-bit universally-unique identifier. Used to name the physical and
// logical objects of the authority model. Bytes are opaque to callers, but
// serialize deterministically as exactly 16 bytes and print as a canonical
// RFC-4122 hex string with hyphens.
// ---------------------------------------------------------------------------
class Uuid {
public:
    constexpr Uuid() noexcept = default;

    static Uuid nil() noexcept { return Uuid{}; }

    // Generate a v4-style (random) identifier. Thread-safe: uses a
    // thread-local generator.
    static Uuid random();

    static Uuid from_bytes(const std::array<uint8_t, 16>& b) noexcept {
        Uuid u; u.bytes_ = b; return u;
    }

    const std::array<uint8_t, 16>& bytes() const noexcept { return bytes_; }
    std::uint8_t* data() noexcept { return bytes_.data(); }
    const std::uint8_t* data() const noexcept { return bytes_.data(); }

    bool is_zero() const noexcept {
        for (auto b : bytes_) { if (b != 0) { return false; } }
        return true;
    }

    std::string to_string() const;
    static std::optional<Uuid> parse(std::string_view s) noexcept;

    bool operator==(const Uuid& o) const noexcept { return bytes_ == o.bytes_; }
    bool operator!=(const Uuid& o) const noexcept { return !(*this == o); }
    bool operator<(const Uuid& o) const noexcept { return bytes_ < o.bytes_; }

private:
    std::array<uint8_t, 16> bytes_{};
};

// Strongly-typed identity wrappers. Each distinct authority domain gets a
// unique C++ type so that, for example, an AttemptId can never be trivially
// supplied where a WorkerId is required, and an ExecutionId can never be
// confused with a DispatchId.
template <typename Tag> class Id {
public:
    constexpr Id() noexcept = default;
    explicit constexpr Id(Uuid v) noexcept : v_(v) {}
    const Uuid& value() const noexcept { return v_; }
    Uuid& value() noexcept { return v_; }
    bool is_nil() const noexcept { return v_.is_zero(); }
    static Id random() { return Id(Uuid::random()); }
    bool operator==(const Id& o) const noexcept { return v_ == o.v_; }
    bool operator!=(const Id& o) const noexcept { return !(*this == o); }
    bool operator<(const Id& o) const noexcept { return v_ < o.v_; }
    std::string to_string() const { return v_.to_string(); }
private:
    Uuid v_;
};

struct ExecutionIdTag {};
struct AttemptIdTag {};
struct WorkerIdTag {};
struct WorkerBootIdTag {};
struct DispatchIdTag {};
struct CoordinatorEpochTag_ {};  // placeholder (epoch handled in generation.hpp)
struct ResultIdTag {};

using ExecutionId = Id<ExecutionIdTag>;
using AttemptId = Id<AttemptIdTag>;
using WorkerId = Id<WorkerIdTag>;
using WorkerBootId = Id<WorkerBootIdTag>;
using DispatchId = Id<DispatchIdTag>;
using ResultId = Id<ResultIdTag>;

}  // namespace execution_fabric

namespace std {
template <> struct hash<execution_fabric::Uuid> {
    size_t operator()(const execution_fabric::Uuid& u) const noexcept {
        size_t h = 1469598103934665603ull;
        for (auto b : u.bytes()) { h = (h ^ b) * 1099511628211ull; }
        return h;
    }
};

template <typename Tag> struct hash<execution_fabric::Id<Tag>> {
    size_t operator()(const execution_fabric::Id<Tag>& id) const noexcept {
        return hash<execution_fabric::Uuid>{}(id.value());
    }
};
}  // namespace std