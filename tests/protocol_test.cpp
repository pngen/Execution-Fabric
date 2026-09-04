#include "protocol/protocol.hpp"
#include "test_util.hpp"
#include <cstring>

using namespace execution_fabric;

int main() {
    // --- Valid round-trip for a nontrivial payload.
    {
        WorkerDispatchPayload wd;
        wd.ticket.execution_id = ExecutionId::random();
        wd.ticket.attempt_id = AttemptId::random();
        wd.ticket.worker_id = WorkerId::random();
        wd.ticket.worker_boot_id = WorkerBootId::random();
        wd.ticket.epoch = CoordinatorEpoch(1);
        wd.ticket.attempt_generation = AttemptGeneration(1);
        wd.ticket.ownership_generation = OwnershipGeneration(3);
        wd.ticket.fence_generation = FenceGeneration(5);
        wd.ticket.dispatch_generation = DispatchGeneration(1);
        const auto p = pack_worker_dispatch(wd);
        const auto f = encode_frame(MsgType::WORKER_DISPATCH, p);
        DecodedFrame out; std::size_t consumed = 0; std::string err;
        CHECK(decode_frame(f.data(), f.size(), out, consumed, err));
        CHECK(out.type == MsgType::WORKER_DISPATCH);
        CHECK(consumed == f.size());
        WorkerDispatchPayload back;
        CHECK(unpack_worker_dispatch(out.payload, back, err));
        CHECK(back.ticket.attempt_id == wd.ticket.attempt_id);
        CHECK(back.ticket.ownership_generation == wd.ticket.ownership_generation);
        CHECK(back.ticket.fence_generation == wd.ticket.fence_generation);
    }

    // --- Complete ticket round-trip (with digest).
    {
        CompletionTicket ct;
        ct.execution_id = ExecutionId::random();
        ct.attempt_id = AttemptId::random();
        ct.worker_id = WorkerId::random();
        ct.worker_boot_id = WorkerBootId::random();
        ct.epoch = CoordinatorEpoch(2);
        ct.dispatch_id = DispatchId::random();
        ct.ownership_generation = OwnershipGeneration(9);
        ct.fence_generation = FenceGeneration(11);
        ct.completion_generation = CompletionGeneration(1);
        ct.attempt_generation = AttemptGeneration(2);
        ct.execution_generation = ExecutionGeneration(1);
        ct.dispatch_generation = DispatchGeneration(1);
        ct.has_result = true;
        const std::vector<std::uint8_t> res{1,2,3,4,5};
        ct.result_digest = ResultDigest::of_buffer(res);
        ByteWriter w; put_completion_ticket(w, ct);
        ByteReader r(w.data().data(), w.size());
        CompletionTicket back;
        std::string err;
        CHECK(get_completion_ticket(r, back, err));
        CHECK(back.attempt_id == ct.attempt_id);
        CHECK(back.worker_boot_id == ct.worker_boot_id);
        CHECK(back.result_digest == ct.result_digest);
        CHECK(back.completion_generation == ct.completion_generation);
    }

    // --- Malformed frames are rejected.
    {
        const std::vector<std::uint8_t> payload{'x','y','z'};
        const auto f = encode_frame(MsgType::COMPLETE, payload);
        DecodedFrame out; std::size_t consumed = 0; std::string err;

        // Bad magic.
        auto bad = f; bad[0] ^= 0x5A;
        CHECK(!decode_frame(bad.data(), bad.size(), out, consumed, err));
        // Bad version.
        bad = f; bad[4] = 99;
        CHECK(!decode_frame(bad.data(), bad.size(), out, consumed, err));
        // Invalid type.
        bad = f; bad[5] = 200;
        CHECK(!decode_frame(bad.data(), bad.size(), out, consumed, err));
        // Length exceeding limit.
        bad = f; std::uint32_t huge = kMaxPayload + 1;
        std::memcpy(bad.data() + 6, &huge, 4);
        CHECK(!decode_frame(bad.data(), bad.size(), out, consumed, err));
        // Payload checksum mismatch.
        bad = f; bad[f.size() - 1] ^= 0xFF;
        CHECK(!decode_frame(bad.data(), bad.size(), out, consumed, err));
        // Truncated frame.
        CHECK(!decode_frame(f.data(), f.size() - 1, out, consumed, err));
        // Trailing garbage after a valid frame is fine; decode consumes exactly one frame.
        auto with_tail = f;
        with_tail.push_back(0xEE);
        CHECK(decode_frame(with_tail.data(), with_tail.size(), out, consumed, err));
        CHECK(consumed == f.size());
    }

    // --- Zero-length payload (valid, e.g. PING).
    {
        const auto f = encode_frame(MsgType::PING, {});
        DecodedFrame out; std::size_t consumed = 0; std::string err;
        CHECK(decode_frame(f.data(), f.size(), out, consumed, err));
        CHECK(out.type == MsgType::PING);
        CHECK(consumed == f.size());
    }

    std::printf("protocol_test: %d checks, %d failures\n", eftest::checks, eftest::failures);
    return TEST_MAIN_RETURN();
}