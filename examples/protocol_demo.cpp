// protocol_demo.cpp
//
// Demonstrates the framed wire protocol: encode/decode a frame with integrity
// checking, and round-trip the authority ticket types that the coordinator and
// workers exchange.
#include "protocol/protocol.hpp"
#include <cstdio>
#include <string>

using namespace execution_fabric;

template <typename T>
static void roundtrip(const char* label, const T& in, T& out) {
    // payload codec is type-specific; use the generic frame for a WorkerDispatch.
    (void)in; (void)out; (void)label;
}

int main() {
    // Build and decode a framed message.
    WorkerDispatchPayload payload;
    payload.ticket = DispatchTicket{};
    payload.ticket.execution_id = ExecutionId::random();
    payload.ticket.attempt_id = AttemptId::random();
    payload.ticket.worker_id = WorkerId::random();
    payload.ticket.worker_boot_id = WorkerBootId::random();
    payload.ticket.epoch = CoordinatorEpoch(1);
    payload.ticket.attempt_generation = AttemptGeneration(3);
    payload.ticket.ownership_generation = OwnershipGeneration(7);
    payload.ticket.fence_generation = FenceGeneration(9);
    payload.work_bytes = 4096;
    payload.work_seed = 12345;

    const auto bytes = pack_worker_dispatch(payload);
    const auto frame = encode_frame(MsgType::WORKER_DISPATCH, bytes);

    std::printf("Frame bytes: %zu (header %zu + payload %zu)\n",
                frame.size(), kFrameHeaderLen, bytes.size());

    DecodedFrame decoded;
    std::size_t consumed = 0;
    std::string err;
    if (!decode_frame(frame.data(), frame.size(), decoded, consumed, err)) {
        std::printf("decode failed: %s\n", err.c_str());
        return 1;
    }
    std::printf("Decoded type: %s, consumed %zu\n", to_string(decoded.type), consumed);

    WorkerDispatchPayload back;
    if (!unpack_worker_dispatch(decoded.payload, back, err)) {
        std::printf("unpack failed: %s\n", err.c_str());
        return 1;
    }
    const bool ok = back.ticket.attempt_id == payload.ticket.attempt_id &&
                    back.ticket.attempt_generation == payload.ticket.attempt_generation &&
                    back.work_seed == payload.work_seed &&
                    back.ticket.fence_generation == payload.ticket.fence_generation;
    std::printf("Round-trip identical: %s\n", ok ? "yes" : "no");
    if (!ok) { return 1; }

    // Demonstrate that corrupting a single byte is detected.
    auto bad = frame;
    bad[18] ^= 0x01;
    if (!decode_frame(bad.data(), bad.size(), decoded, consumed, err)) {
        std::printf("Corruption detected: %s\n", err.c_str());
    } else {
        std::printf("UNEXPECTED: corruption not detected\n");
        return 1;
    }
    std::printf("OK\n");
    return 0;
}
