#pragma once
#include "execution_fabric/record.hpp"
#include "execution_fabric/generation.hpp"
#include <string>
#include <vector>

namespace execution_fabric {

// ---------------------------------------------------------------------------
// IPersistenceStore
//
// Durable storage of execution-authority state. Implementations must guarantee
// that a recovered snapshot is byte-for-byte identical to what was saved and
// that any truncation, corruption, malformed length, invalid enum, invalid
// generation, duplicate identity, or impossible state combination is rejected
// rather than plausibly decoded.
// ---------------------------------------------------------------------------
class IPersistenceStore {
public:
    virtual ~IPersistenceStore() = default;

    // Persist all records and the coordinator epoch atomically. Returns false
    // on failure (use last_error() for detail).
    virtual bool save(const std::vector<ExecutionRecord>& records,
                      const CoordinatorEpoch& epoch) = 0;

    // Recover records and the coordinator epoch. Returns false on any integrity
    // violation; in that case out is left empty and last_error() explains why.
    virtual bool load(std::vector<ExecutionRecord>& out, CoordinatorEpoch& epoch) = 0;

    virtual std::string last_error() const noexcept = 0;
};

// ---------------------------------------------------------------------------
// FilePersistenceStore
//
// Versioned binary store with per-record CRC64, a whole-file CRC, length
// accounting, and rigorous decode validation.
// ---------------------------------------------------------------------------
class FilePersistenceStore : public IPersistenceStore {
public:
    explicit FilePersistenceStore(std::string path);
    bool save(const std::vector<ExecutionRecord>& records, const CoordinatorEpoch& epoch) override;
    bool load(std::vector<ExecutionRecord>& out, CoordinatorEpoch& epoch) override;
    std::string last_error() const noexcept override { return last_error_; }

    // Serialize/deserialize a single execution record (exposed for tests and
    // the corruption-test harness).
    static std::vector<std::uint8_t> encode_record(const ExecutionRecord& rec);
    static bool decode_record(const std::uint8_t* data, std::size_t len, ExecutionRecord& out,
                              std::string& error);

private:
    std::string path_;
    std::string last_error_;
};

}  // namespace execution_fabric