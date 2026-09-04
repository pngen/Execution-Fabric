#pragma once
#include <cstdint>
#include <limits>
#include <ostream>

namespace execution_fabric {

// ---------------------------------------------------------------------------
// Gen<Tag>
//
// A monotonically-non-decreasing generation counter bound to a named
// authority domain. Generations are the fencing primitive: a message that
// carries a generation older than the currently authoritative one can be
// rejected without consulting any higher authority.
//
// A value of 0 means "no generation has been established" (unset). The first
// real generation always has value >= 1.
// ---------------------------------------------------------------------------
template <typename Tag> class Gen {
public:
    constexpr Gen() noexcept = default;
    explicit constexpr Gen(std::uint64_t v) noexcept : v_(v) {}

    constexpr std::uint64_t value() const noexcept { return v_; }
    constexpr bool is_set() const noexcept { return v_ != 0; }

    // Pure successor: the generation that would be current after one advance.
    // Saturates at the maximum rather than wrapping, so corruption can never
    // produce a generation that reads as "newer" after overflow.
    constexpr Gen next() const noexcept {
        if (v_ == std::numeric_limits<std::uint64_t>::max()) { return *this; }
        return Gen(v_ + 1);
    }

    constexpr Gen advance(std::uint64_t n) const noexcept {
        Gen g = *this;
        while (n-- != 0) { g = g.next(); }
        return g;
    }

    constexpr bool operator==(const Gen& o) const noexcept { return v_ == o.v_; }
    constexpr bool operator!=(const Gen& o) const noexcept { return !(*this == o); }
    constexpr bool operator<(const Gen& o) const noexcept { return v_ < o.v_; }
    constexpr bool operator<=(const Gen& o) const noexcept { return v_ <= o.v_; }
    constexpr bool operator>(const Gen& o) const noexcept { return v_ > o.v_; }
    constexpr bool operator>=(const Gen& o) const noexcept { return v_ >= o.v_; }

private:
    std::uint64_t v_{0};
};

struct ExecutionGenTag {};
struct AttemptGenTag {};
struct DispatchGenTag {};
struct OwnershipGenTag {};
struct FenceGenTag {};
struct CancellationGenTag {};
struct PreemptionGenTag {};
struct ResumeGenTag {};
struct CompletionGenTag {};
struct CommitGenTag {};
struct CoordinatorEpochTag {};

using ExecutionGeneration = Gen<ExecutionGenTag>;
using AttemptGeneration = Gen<AttemptGenTag>;
using DispatchGeneration = Gen<DispatchGenTag>;
using OwnershipGeneration = Gen<OwnershipGenTag>;
using FenceGeneration = Gen<FenceGenTag>;
using CancellationGeneration = Gen<CancellationGenTag>;
using PreemptionGeneration = Gen<PreemptionGenTag>;
using ResumeGeneration = Gen<ResumeGenTag>;
using CompletionGeneration = Gen<CompletionGenTag>;
using CommitGeneration = Gen<CommitGenTag>;
using CoordinatorEpoch = Gen<CoordinatorEpochTag>;

template <typename Tag>
std::ostream& operator<<(std::ostream& os, const Gen<Tag>& g) {
    return os << g.value();
}

}  // namespace execution_fabric