#include "execution_fabric/persistence.hpp"
#include "execution_fabric/binary.hpp"
#include "execution_fabric/checksum.hpp"
#include "execution_fabric/state.hpp"

#include <cstdio>
#include <fstream>
#include <unordered_set>
#include <utility>

#ifdef _WIN32
#include <windows.h>
#endif

namespace execution_fabric {

namespace {

constexpr std::uint8_t kMagic[] = {0x45, 0x46, 0x41, 0x42, 0x53, 0x54, 0x4F, 0x52};  // "EFABSTOR"
constexpr std::uint8_t kTrailerMagic[] = {0x45, 0x46, 0x54, 0x52, 0x41, 0x49, 0x4C, 0x52};  // "EFTRAILR"
constexpr std::uint32_t kFormatVersion = 1;
constexpr std::size_t kHeaderSize = 8 + 4 + 8 + 4;   // magic + version + epoch + count
constexpr std::size_t kTrailerSize = 8 + 8;          // magic + body crc
constexpr std::uint32_t kMaxRecordPayload = 64u * 1024u * 1024u;  // 64 MiB

bool valid_exec_state(std::uint8_t v) {
    return v <= static_cast<std::uint8_t>(ExecutionState::TERMINAL);
}
bool valid_attempt_state(std::uint8_t v) {
    return v <= static_cast<std::uint8_t>(AttemptState::TERMINAL);
}

bool write_all(const std::string& path, const std::vector<std::uint8_t>& data) {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    if (!f) { return false; }
    f.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
    return f.good();
}

bool read_all(const std::string& path, std::vector<std::uint8_t>& out) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    if (!f) { return false; }
    const auto sz = f.tellg();
    if (sz < 0) { return false; }
    f.seekg(0, std::ios::beg);
    out.resize(static_cast<std::size_t>(sz));
    if (out.empty()) { return true; }
    f.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(out.size()));
    return f.good();
}

bool replace_file(const std::string& tmp, const std::string& dest) {
#ifdef _WIN32
    if (!::MoveFileExA(tmp.c_str(), dest.c_str(), MOVEFILE_REPLACE_EXISTING)) {
        // Fallback: remove then rename is not atomic; only use when replace fails.
        ::DeleteFileA(dest.c_str());
        if (!::MoveFileExA(tmp.c_str(), dest.c_str(), 0)) { return false; }
        return true;
    }
    return true;
#else
    if (::remove(dest.c_str()) != 0 && errno != ENOENT) { return false; }
    return ::rename(tmp.c_str(), dest.c_str()) == 0;
#endif
}

}  // namespace

FilePersistenceStore::FilePersistenceStore(std::string path) : path_(std::move(path)) {}

// ---------------------------------------------------------------------------
// Record encode / decode
// ---------------------------------------------------------------------------
std::vector<std::uint8_t> FilePersistenceStore::encode_record(const ExecutionRecord& rec) {
    ByteWriter w;
    w.bytes(rec.id.value().data(), 16);
    w.u64(rec.generation.value());
    w.u8(static_cast<std::uint8_t>(rec.state));
    w.u64(rec.coordinator_epoch.value());
    w.u64(rec.ownership_generation.value());
    w.u64(rec.fence_generation.value());
    w.bool_byte(rec.current_attempt_id.has_value());
    if (rec.current_attempt_id) { w.bytes(rec.current_attempt_id->value().bytes().data(), 16); }
    w.bool_byte(rec.current_attempt_generation.has_value());
    if (rec.current_attempt_generation) { w.u64(rec.current_attempt_generation->value()); }
    w.bool_byte(rec.owner_worker.has_value());
    if (rec.owner_worker) { w.bytes(rec.owner_worker->value().bytes().data(), 16); }
    w.bool_byte(rec.owner_worker_boot.has_value());
    if (rec.owner_worker_boot) { w.bytes(rec.owner_worker_boot->value().bytes().data(), 16); }
    w.u64(rec.cancellation_generation.value());
    w.u64(rec.preemption_generation.value());
    w.u64(rec.resume_generation.value());
    w.u64(rec.completion_generation.value());
    w.u64(rec.commit_generation.value());
    w.bool_byte(rec.committed_digest.has_value());
    if (rec.committed_digest) { w.bytes(rec.committed_digest->bytes().data(), 32); }
    w.u32(static_cast<std::uint32_t>(rec.attempts.size()));
    for (const auto& at : rec.attempts) {
        w.bytes(at.id.value().data(), 16);
        w.u64(at.generation.value());
        w.bytes(at.worker_id.value().data(), 16);
        w.bytes(at.worker_boot_id.value().data(), 16);
        w.bytes(at.dispatch_id.value().data(), 16);
        w.u64(at.dispatch_generation.value());
        w.u64(at.ownership_generation.value());
        w.u64(at.fence_generation.value());
        w.u8(static_cast<std::uint8_t>(at.execution_state_at_dispatch));
        w.u8(static_cast<std::uint8_t>(at.state));
        w.bool_byte(at.result_digest.has_value());
        if (at.result_digest) { w.bytes(at.result_digest->bytes().data(), 32); }
        w.bool_byte(at.result_payload_hex.has_value());
        if (at.result_payload_hex) { w.string(*at.result_payload_hex); }
        w.bool_byte(at.failure_reason.has_value());
        if (at.failure_reason) { w.string(*at.failure_reason); }
    }
    w.string(rec.created_at);
    w.string(rec.committed_at);
    return std::move(w).take();
}

bool FilePersistenceStore::decode_record(const std::uint8_t* data, std::size_t len,
                                         ExecutionRecord& out, std::string& error) {
    ByteReader r(data, len);
    ExecutionRecord rec;
    if (!r.bytes(rec.id.value().data(), 16)) { error = "record: truncated execution id"; return false; }
    std::uint64_t gen = 0;
    if (!r.u64(gen)) { error = "record: truncated generation"; return false; }
    rec.generation = ExecutionGeneration(gen);
    std::uint8_t st = 0;
    if (!r.u8(st)) { error = "record: truncated state"; return false; }
    if (!valid_exec_state(st)) { error = "record: invalid execution state enum"; return false; }
    rec.state = static_cast<ExecutionState>(st);
    std::uint64_t v = 0;
    if (!r.u64(v)) { error = "record: truncated coordinator epoch"; return false; }
    rec.coordinator_epoch = CoordinatorEpoch(v);
    if (!r.u64(v)) { error = "record: truncated ownership generation"; return false; }
    rec.ownership_generation = OwnershipGeneration(v);
    if (!r.u64(v)) { error = "record: truncated fence generation"; return false; }
    rec.fence_generation = FenceGeneration(v);

    bool present = false;
    if (!r.bool_byte(present)) { error = "record: truncated attempt-id present"; return false; }
    if (present) {
        AttemptId id;
        if (!r.bytes(id.value().data(), 16)) { error = "record: truncated attempt id"; return false; }
        rec.current_attempt_id = id;
    }
    if (!r.bool_byte(present)) { error = "record: truncated attempt-gen present"; return false; }
    if (present) {
        std::uint64_t g = 0;
        if (!r.u64(g)) { error = "record: truncated attempt generation"; return false; }
        rec.current_attempt_generation = AttemptGeneration(g);
    }
    if (!r.bool_byte(present)) { error = "record: truncated owner-worker present"; return false; }
    if (present) {
        WorkerId id;
        if (!r.bytes(id.value().data(), 16)) { error = "record: truncated owner worker"; return false; }
        rec.owner_worker = id;
    }
    if (!r.bool_byte(present)) { error = "record: truncated owner-boot present"; return false; }
    if (present) {
        WorkerBootId id;
        if (!r.bytes(id.value().data(), 16)) { error = "record: truncated owner boot"; return false; }
        rec.owner_worker_boot = id;
    }
    if (!r.u64(v)) { error = "record: truncated cancellation generation"; return false; }
    rec.cancellation_generation = CancellationGeneration(v);
    if (!r.u64(v)) { error = "record: truncated preemption generation"; return false; }
    rec.preemption_generation = PreemptionGeneration(v);
    if (!r.u64(v)) { error = "record: truncated resume generation"; return false; }
    rec.resume_generation = ResumeGeneration(v);
    if (!r.u64(v)) { error = "record: truncated completion generation"; return false; }
    rec.completion_generation = CompletionGeneration(v);
    if (!r.u64(v)) { error = "record: truncated commit generation"; return false; }
    rec.commit_generation = CommitGeneration(v);

    if (!r.bool_byte(present)) { error = "record: truncated committed-digest present"; return false; }
    if (present) {
        Sha256::Digest d;
        if (!r.bytes(d.data(), 32)) { error = "record: truncated committed digest"; return false; }
        rec.committed_digest = ResultDigest::from_bytes(d);
    }

    std::uint32_t n_attempts = 0;
    if (!r.u32(n_attempts)) { error = "record: truncated attempt count"; return false; }
    if (n_attempts > 1u << 20) { error = "record: attempt count too large"; return false; }
    rec.attempts.reserve(n_attempts);
    for (std::uint32_t i = 0; i < n_attempts; ++i) {
        AttemptRecord at;
        if (!r.bytes(at.id.value().data(), 16)) { error = "record: truncated attempt id"; return false; }
        std::uint64_t g = 0;
        if (!r.u64(g)) { error = "record: truncated attempt generation"; return false; }
        at.generation = AttemptGeneration(g);
        if (!r.bytes(at.worker_id.value().data(), 16)) { error = "record: truncated attempt worker"; return false; }
        if (!r.bytes(at.worker_boot_id.value().data(), 16)) { error = "record: truncated attempt boot"; return false; }
        if (!r.bytes(at.dispatch_id.value().data(), 16)) { error = "record: truncated attempt dispatch"; return false; }
        if (!r.u64(v)) { error = "record: truncated attempt dispatch generation"; return false; }
        at.dispatch_generation = DispatchGeneration(v);
        if (!r.u64(v)) { error = "record: truncated attempt ownership generation"; return false; }
        at.ownership_generation = OwnershipGeneration(v);
        if (!r.u64(v)) { error = "record: truncated attempt fence generation"; return false; }
        at.fence_generation = FenceGeneration(v);
        std::uint8_t es = 0;
        if (!r.u8(es)) { error = "record: truncated attempt exec state"; return false; }
        if (!valid_exec_state(es)) { error = "record: invalid attempt execution state enum"; return false; }
        at.execution_state_at_dispatch = static_cast<ExecutionState>(es);
        std::uint8_t as = 0;
        if (!r.u8(as)) { error = "record: truncated attempt state"; return false; }
        if (!valid_attempt_state(as)) { error = "record: invalid attempt state enum"; return false; }
        at.state = static_cast<AttemptState>(as);
        if (!r.bool_byte(present)) { error = "record: truncated attempt digest present"; return false; }
        if (present) {
            Sha256::Digest d;
            if (!r.bytes(d.data(), 32)) { error = "record: truncated attempt digest"; return false; }
            at.result_digest = ResultDigest::from_bytes(d);
        }
        if (!r.bool_byte(present)) { error = "record: truncated attempt payload present"; return false; }
        if (present) {
            std::string s;
            if (!r.string(s)) { error = "record: truncated attempt payload"; return false; }
            at.result_payload_hex = s;
        }
        if (!r.bool_byte(present)) { error = "record: truncated attempt failure present"; return false; }
        if (present) {
            std::string s;
            if (!r.string(s)) { error = "record: truncated attempt failure"; return false; }
            at.failure_reason = s;
        }
        rec.attempts.push_back(std::move(at));
    }
    if (!r.string(rec.created_at)) { error = "record: truncated created_at"; return false; }
    if (!r.string(rec.committed_at)) { error = "record: truncated committed_at"; return false; }

    if (!r.ok()) { error = "record: decode error (bounds)"; return false; }
    if (r.remaining() != 0) { error = "record: trailing garbage after record"; return false; }

    // --- Cross-field validation (impossible state combinations) --------------
    if (!rec.generation.is_set()) { error = "record: execution generation must be set"; return false; }

    // Attempt id + dispatch id uniqueness within a record.
    {
        std::unordered_set<AttemptId> a_ids;
        std::unordered_set<DispatchId> d_ids;
        for (const auto& at : rec.attempts) {
            if (!a_ids.insert(at.id).second) { error = "record: duplicate attempt id"; return false; }
            if (!d_ids.insert(at.dispatch_id).second) { error = "record: duplicate dispatch id"; return false; }
            if (!at.generation.is_set()) { error = "record: attempt generation must be set"; return false; }
            if (!at.dispatch_generation.is_set()) { error = "record: attempt dispatch generation must be set"; return false; }
        }
    }
    // If a current attempt is named, it must exist and be unique.
    if (rec.current_attempt_id) {
        bool found = false;
        for (const auto& at : rec.attempts) {
            if (at.id == *rec.current_attempt_id) {
                if (found) { error = "record: current attempt duplicated"; return false; }
                found = true;
                if (rec.current_attempt_generation && at.generation != *rec.current_attempt_generation) {
                    error = "record: current attempt generation mismatch"; return false;
                }
            }
        }
        if (!found) { error = "record: current attempt does not exist in history"; return false; }
        if (!rec.current_attempt_generation) { error = "record: current attempt generation unset"; return false; }
    }
    if (rec.state == ExecutionState::COMMITTED) {
        if (!rec.committed_digest) { error = "record: committed state without committed digest"; return false; }
        if (!rec.commit_generation.is_set()) { error = "record: committed state without commit generation"; return false; }
    }
    // Worker owner consistency: if an execution is running/dispatched, it must
    // name an owner worker & boot.
    if (rec.state == ExecutionState::RUNNING || rec.state == ExecutionState::DISPATCHED ||
        rec.state == ExecutionState::RESUMING || rec.state == ExecutionState::CANCELLATION_REQUESTED ||
        rec.state == ExecutionState::PREEMPTION_REQUESTED) {
        if (!rec.current_attempt_id || !rec.owner_worker || !rec.owner_worker_boot) {
            error = "record: active execution must name current attempt and owner"; return false;
        }
    }

    out = std::move(rec);
    return true;
}

// ---------------------------------------------------------------------------
// Save / load
// ---------------------------------------------------------------------------
bool FilePersistenceStore::save(const std::vector<ExecutionRecord>& records,
                                const CoordinatorEpoch& epoch) {
    ByteWriter w;
    w.bytes(kMagic, 8);
    w.u32(kFormatVersion);
    w.u64(epoch.value());
    w.u32(static_cast<std::uint32_t>(records.size()));

    std::vector<std::uint8_t> body;
    ByteWriter bodyw;
    // header
    bodyw.bytes(w.data());
    // records
    std::vector<std::uint8_t> record_bytes;
    for (const auto& rec : records) {
        record_bytes = encode_record(rec);
        ByteWriter rw;
        rw.u32(static_cast<std::uint32_t>(record_bytes.size()));
        rw.u64(Crc64::compute(record_bytes.data(), record_bytes.size()));
        rw.bytes(record_bytes.data(), record_bytes.size());
        bodyw.bytes(rw.data());
    }
    // Recompute full body crc over header + records.
    const std::uint64_t body_crc = Crc64::compute(bodyw.data().data(), bodyw.data().size());

    std::vector<std::uint8_t> out;
    out.reserve(bodyw.size() + kTrailerSize);
    out.insert(out.end(), bodyw.data().begin(), bodyw.data().end());
    // trailer
    ByteWriter tr;
    tr.bytes(kTrailerMagic, 8);
    tr.u64(body_crc);
    out.insert(out.end(), tr.data().begin(), tr.data().end());

    const std::string tmp = path_ + ".tmp";
    if (!write_all(tmp, out)) {
        last_error_ = "save: failed to write temporary file";
        return false;
    }
    if (!replace_file(tmp, path_)) {
        last_error_ = "save: failed to atomically replace target";
        return false;
    }
    last_error_.clear();
    return true;
}

bool FilePersistenceStore::load(std::vector<ExecutionRecord>& out, CoordinatorEpoch& epoch) {
    out.clear();
    last_error_.clear();
    std::vector<std::uint8_t> bytes;
    if (!read_all(path_, bytes)) {
        last_error_ = "load: cannot read file (missing?)";
        return false;
    }
    if (bytes.size() < kHeaderSize + kTrailerSize) {
        last_error_ = "load: file too short (truncated)";
        return false;
    }
    // Verify trailer.
    {
        const std::size_t tr_start = bytes.size() - kTrailerSize;
        if (std::memcmp(bytes.data() + tr_start, kTrailerMagic, 8) != 0) {
            last_error_ = "load: trailer magic mismatch (corrupt/truncated)";
            return false;
        }
        std::uint64_t stored_crc = 0;
        ByteReader tr(bytes.data() + tr_start + 8, 8);
        if (!tr.u64(stored_crc)) { last_error_ = "load: trailer crc unreadable"; return false; }
        const std::uint64_t computed_crc = Crc64::compute(bytes.data(), tr_start);
        if (stored_crc != computed_crc) {
            last_error_ = "load: whole-file crc mismatch (corrupt)";
            return false;
        }
    }
    const std::size_t body_len = bytes.size() - kTrailerSize;
    ByteReader r(bytes.data(), body_len);
    std::uint8_t magic[8];
    if (!r.bytes(magic, 8)) { last_error_ = "load: truncated header"; return false; }
    if (std::memcmp(magic, kMagic, 8) != 0) { last_error_ = "load: header magic mismatch"; return false; }
    std::uint32_t version = 0;
    if (!r.u32(version)) { last_error_ = "load: truncated version"; return false; }
    if (version != kFormatVersion) { last_error_ = "load: unsupported format version"; return false; }
    std::uint64_t ep = 0;
    if (!r.u64(ep)) { last_error_ = "load: truncated epoch"; return false; }
    epoch = CoordinatorEpoch(ep);
    std::uint32_t count = 0;
    if (!r.u32(count)) { last_error_ = "load: truncated count"; return false; }
    if (count > (1u << 20)) { last_error_ = "load: record count too large"; return false; }

    std::unordered_set<ExecutionId> exec_ids;
    std::vector<ExecutionRecord> recs;
    recs.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        std::uint32_t len = 0;
        if (!r.u32(len)) { last_error_ = "load: truncated record length"; return false; }
        if (len == 0 || len > kMaxRecordPayload) {
            last_error_ = "load: invalid record length";
            return false;
        }
        if (r.remaining() < len + 8) { last_error_ = "load: record length exceeds file (truncated)"; return false; }
        std::uint64_t crc = 0;
        if (!r.u64(crc)) { last_error_ = "load: truncated record crc"; return false; }
        const std::uint8_t* payload = bytes.data() + body_len - r.remaining();
        const std::uint64_t computed = Crc64::compute(payload, len);
        if (computed != crc) { last_error_ = "load: record crc mismatch (corrupt)"; return false; }
        ExecutionRecord rec;
        std::string err;
        if (!decode_record(payload, len, rec, err)) {
            last_error_ = "load: malformed record (" + err + ")";
            return false;
        }
        if (!exec_ids.insert(rec.id).second) {
            last_error_ = "load: duplicate execution identity";
            return false;
        }
        recs.push_back(std::move(rec));
        if (!r.skip(len)) { last_error_ = "load: record read failed"; return false; }
    }
    if (r.remaining() != 0) {
        last_error_ = "load: trailing garbage after declared records";
        return false;
    }
    out = std::move(recs);
    return true;
}

}  // namespace execution_fabric