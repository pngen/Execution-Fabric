#include "execution_fabric/engine.hpp"
#include "execution_fabric/persistence.hpp"
#include "execution_fabric/binary.hpp"
#include "execution_fabric/checksum.hpp"
#include "test_util.hpp"
#include <cstdio>
#include <fstream>

using namespace execution_fabric;

namespace {

const std::uint8_t kMagic[] = {0x45,0x46,0x41,0x42,0x53,0x54,0x4F,0x52};
const std::uint8_t kTrailerMagic[] = {0x45,0x46,0x54,0x52,0x41,0x49,0x4C,0x52};

// Build a valid store file from records, mirroring the on-disk layout.
std::vector<std::uint8_t> build_file(const std::vector<ExecutionRecord>& records, const CoordinatorEpoch& epoch) {
    ByteWriter body;
    ByteWriter header;
    header.bytes(kMagic, 8);
    header.u32(1);  // version
    header.u64(epoch.value());
    header.u32(static_cast<std::uint32_t>(records.size()));
    body.bytes(header.data());
    for (const auto& rec : records) {
        auto payload_bytes = FilePersistenceStore::encode_record(rec);
        ByteWriter rw;
        rw.u32(static_cast<std::uint32_t>(payload_bytes.size()));
        rw.u64(Crc64::compute(payload_bytes.data(), payload_bytes.size()));
        rw.bytes(payload_bytes.data(), payload_bytes.size());
        body.bytes(rw.data());
    }
    std::vector<std::uint8_t> out(body.data().begin(), body.data().end());
    const auto c = Crc64::compute(out.data(), out.size());
    ByteWriter tr;
    tr.bytes(kTrailerMagic, 8);
    tr.u64(c);
    out.insert(out.end(), tr.data().begin(), tr.data().end());
    return out;
}

void write_file(const std::string& path, const std::vector<std::uint8_t>& data) {
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    f.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
}

std::vector<std::uint8_t> read_file(const std::string& path) {
    std::ifstream f(path, std::ios::binary | std::ios::ate);
    const auto sz = f.tellg();
    f.seekg(0, std::ios::beg);
    std::vector<std::uint8_t> out(static_cast<std::size_t>(sz));
    f.read(reinterpret_cast<char*>(out.data()), sz);
    return out;
}

}  // namespace

int main() {
    const CoordinatorEpoch EPOCH(1);
    ExecutionEngine eng(EPOCH);
    const ExecutionId ex = ExecutionId::random();
    const WorkerId w = WorkerId::random();
    const WorkerBootId b = WorkerBootId::random();

    CHECK_EQ(eng.create(ex, EPOCH).code, DecisionCode::ALLOW);
    CHECK_EQ(eng.activate(ex, EPOCH).code, DecisionCode::ALLOW);
    DispatchTicket d;
    CHECK_EQ(eng.dispatch(ex, w, b, EPOCH, d).code, DecisionCode::ALLOW);
    {
        StartTicket s; s.execution_id=d.execution_id; s.execution_generation=d.execution_generation;
        s.attempt_id=d.attempt_id; s.attempt_generation=d.attempt_generation;
        s.worker_id=d.worker_id; s.worker_boot_id=d.worker_boot_id;
        s.epoch=d.epoch; s.dispatch_id=d.dispatch_id; s.dispatch_generation=d.dispatch_generation;
        s.ownership_generation=d.ownership_generation; s.fence_generation=d.fence_generation;
        CHECK_EQ(eng.mark_running(s).code, DecisionCode::ALLOW);
    }
    const char payload[] = "persisted-result";
    CompletionTicket c; c.execution_id=d.execution_id; c.execution_generation=d.execution_generation;
    c.attempt_id=d.attempt_id; c.attempt_generation=d.attempt_generation; c.worker_id=d.worker_id;
    c.worker_boot_id=d.worker_boot_id; c.epoch=d.epoch; c.dispatch_id=d.dispatch_id;
    c.dispatch_generation=d.dispatch_generation; c.ownership_generation=d.ownership_generation;
    c.fence_generation=d.fence_generation; c.completion_generation=CompletionGeneration(1);
    c.has_result=true; c.result_digest=ResultDigest::of(payload,sizeof(payload));
    CHECK_EQ(eng.complete(c).code, DecisionCode::ALLOW);
    const auto recs = eng.records();

    // Round trip.
    const std::string path = "roundtrip_test.efdl";
    FilePersistenceStore store(path);
    CHECK(store.save(recs, EPOCH));
    std::vector<ExecutionRecord> loaded;
    CoordinatorEpoch lepoch(0);
    CHECK(store.load(loaded, lepoch));
    CHECK_EQ(lepoch.value(), EPOCH.value());
    CHECK_EQ(loaded.size(), recs.size());
    CHECK(loaded[0].id == recs[0].id);
    CHECK(loaded[0].state == ExecutionState::COMMITTED);
    CHECK(loaded[0].commit_generation.value() == 1u);
    CHECK(loaded[0].current_attempt_id.has_value());
    CHECK(loaded[0].attempts.size() == 1u);
    CHECK(loaded[0].committed_digest.has_value());
    CHECK(*loaded[0].committed_digest == recs[0].committed_digest.value());

    // Save / load an engine that also has history (superseded attempt).
    {
        ExecutionEngine e2(EPOCH);
        const ExecutionId ex2 = ExecutionId::random();
        const WorkerId w2 = WorkerId::random(); const WorkerBootId b2 = WorkerBootId::random();
        CHECK_EQ(e2.create(ex2, EPOCH).code, DecisionCode::ALLOW);
        CHECK_EQ(e2.activate(ex2, EPOCH).code, DecisionCode::ALLOW);
        DispatchTicket a1;
        CHECK_EQ(e2.dispatch(ex2, w2, b2, EPOCH, a1).code, DecisionCode::ALLOW);
        DispatchTicket a2;
        CHECK_EQ(e2.dispatch(ex2, w, b, EPOCH, a2).code, DecisionCode::ALLOW);  // supersede a1
        const auto r2 = e2.records();
        CHECK_EQ(r2.size(), 1u);
        CHECK_EQ(r2[0].attempts.size(), 2u);
        FilePersistenceStore s2("history_test.efdl");
        CHECK(s2.save(r2, EPOCH));
        std::vector<ExecutionRecord> l2; CoordinatorEpoch e3(0);
        CHECK(s2.load(l2, e3));
        CHECK_EQ(l2.size(), 1u);
        CHECK_EQ(l2[0].attempts.size(), 2u);
        CHECK(l2[0].current_attempt_id.has_value());
        CHECK(l2[0].attempts[0].state == AttemptState::SUPERSEDED);
        // The second (current) attempt is the last non-terminal one.
        CHECK(l2[0].attempts[1].id == l2[0].current_attempt_id.value());
    }

    // --- Corruption: flip a byte in the record payload -> crc mismatch.
    {
        const std::string p = "corrupt1.efdl";
        auto bytes = build_file(recs, EPOCH);
        // Flip a byte in the middle (inside the first record payload).
        bytes[28] ^= 0xFF;
        write_file(p, bytes);
        FilePersistenceStore s(p);
        std::vector<ExecutionRecord> o; CoordinatorEpoch e(0);
        CHECK(!s.load(o, e));
        CHECK(!s.last_error().empty());
        CHECK_EQ(o.size(), 0u);
        std::remove(p.c_str());
    }

    // --- Truncation.
    {
        const std::string p = "corrupt2.efdl";
        auto bytes = build_file(recs, EPOCH);
        bytes.resize(bytes.size() - 5);
        write_file(p, bytes);
        FilePersistenceStore s(p);
        std::vector<ExecutionRecord> o; CoordinatorEpoch e(0);
        CHECK(!s.load(o, e));
        std::remove(p.c_str());
    }

    // --- Trailing garbage (extra bytes after the trailer).
    {
        const std::string p = "corrupt3.efdl";
        auto bytes = build_file(recs, EPOCH);
        bytes.push_back(0xAA);
        bytes.push_back(0xBB);
        write_file(p, bytes);
        FilePersistenceStore s(p);
        std::vector<ExecutionRecord> o; CoordinatorEpoch e(0);
        CHECK(!s.load(o, e));  // whole-file crc mismatch
        std::remove(p.c_str());
    }

    // --- Invalid enum: corrupt a state byte inside a record, recompute CRCs.
    {
        const std::string p = "corrupt4.efdl";
        auto payload_bytes = FilePersistenceStore::encode_record(recs[0]);
        ByteReader pr(payload_bytes.data(), payload_bytes.size());
        // State is the 25th byte (24 id + 8 gen = 24 -> index 24). Overwrite with 200.
        if (payload_bytes.size() > 24) { payload_bytes[24] = 200; }
        ByteWriter rw; rw.u32((std::uint32_t)payload_bytes.size());
        rw.u64(Crc64::compute(payload_bytes.data(), payload_bytes.size()));
        rw.bytes(payload_bytes.data(), payload_bytes.size());
        ByteWriter body;
        ByteWriter header; header.bytes(kMagic,8); header.u32(1); header.u64(EPOCH.value()); header.u32(1);
        body.bytes(header.data()); body.bytes(rw.data());
        std::vector<std::uint8_t> full(body.data().begin(), body.data().end());
        ByteWriter tr; tr.bytes(kTrailerMagic,8); tr.u64(Crc64::compute(full.data(), full.size()));
        full.insert(full.end(), tr.data().begin(), tr.data().end());
        write_file(p, full);
        FilePersistenceStore s(p);
        std::vector<ExecutionRecord> o; CoordinatorEpoch e(0);
        CHECK(!s.load(o, e));
        CHECK(s.last_error().find("invalid execution state enum") != std::string::npos);
        std::remove(p.c_str());
    }

    std::remove("roundtrip_test.efdl");
    std::remove("history_test.efdl");
    std::printf("persistence_test: %d checks, %d failures\n", eftest::checks, eftest::failures);
    return TEST_MAIN_RETURN();
}