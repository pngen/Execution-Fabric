#pragma once
#include "execution_fabric/authority.hpp"
#include "execution_fabric/record.hpp"
#include "execution_fabric/decision.hpp"
#include "execution_fabric/binary.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace execution_fabric {

// ---------------------------------------------------------------------------
// Protocol constants
// ---------------------------------------------------------------------------
constexpr std::uint32_t kFrameMagic = 0x44465845;   // "EFXD" (LE: 45 46 58 44)
constexpr std::uint8_t kFrameVersion = 1;
constexpr std::size_t kFrameHeaderLen = 4 + 1 + 1 + 4 + 8;  // magic + ver + type + len + crc
constexpr std::uint32_t kMaxPayload = 64u * 1024u * 1024u;  // 64 MiB

enum class MsgType : std::uint8_t {
    HELLO = 0,
    WELCOME = 1,
    REGISTER_WORKER = 2,
    REGISTER_ACK = 3,
    SUBMIT = 4,
    SUBMIT_ACK = 5,
    DISPATCH = 6,           // controller -> coordinator: request a dispatch
    DISPATCH_ACK = 7,       // coordinator -> controller: ticket
    WORKER_DISPATCH = 8,    // coordinator -> worker: run this attempt
    WORKER_DISPATCH_ACK = 9,
    START = 10,             // worker -> coordinator: began running
    COMPLETE = 11,          // worker/client -> coordinator: completion
    COMPLETE_ACK = 12,      // coordinator -> worker: decision + commit
    CANCEL = 13,
    CANCEL_ACK = 14,
    PREEMPT = 15,
    PREEMPT_ACK = 16,
    ACK_PREEMPT = 17,       // worker -> coordinator: preemption acknowledged
    MARK_RESUMABLE = 18,
    RESUME = 19,
    RESUME_ACK = 20,
    WORKER_LOST = 21,       // controller/harness -> coordinator: notify loss
    QUERY = 22,
    QUERY_RESPONSE = 23,
    PING = 24,
    PONG = 25,
    SHUTDOWN = 26,
};

const char* to_string(MsgType t) noexcept;

// ---------------------------------------------------------------------------
// Message payload structs
// ---------------------------------------------------------------------------
struct HelloPayload {
    std::uint8_t role = 0;          // 0 = controller, 1 = worker
    std::uint8_t protocol_version = kFrameVersion;
    WorkerId worker_id;
    WorkerBootId worker_boot_id;
    std::string tag;
};

struct WelcomePayload {
    CoordinatorEpoch epoch;
    std::string coordinator_id;
    std::uint32_t protocol_version = kFrameVersion;
};

struct RegisterAckPayload {
    bool accepted = false;
    std::string reason;
};

struct SubmitPayload {
    ExecutionId execution_id;
    CoordinatorEpoch epoch;
};

struct SubmitAckPayload {
    bool accepted = false;
    ExecutionId execution_id;
    ExecutionGeneration generation;
    ExecutionState state;
    std::string reason;
};

struct DispatchRequestPayload {
    ExecutionId execution_id;
    CoordinatorEpoch epoch;
    WorkerId worker_id;
    WorkerBootId worker_boot_id;
};

struct DispatchAckPayload {
    bool accepted = false;
    DecisionCode decision = DecisionCode::UNKNOWN;
    std::string reason;
    DispatchTicket ticket;
};

struct WorkerDispatchPayload {
    DispatchTicket ticket;
    std::uint32_t work_bytes = 0;     // hint about how much work the attempt should do
    std::uint32_t work_seed = 0;
};

struct StartPayload {
    StartTicket ticket;
};

struct CompletePayload {
    CompletionTicket ticket;
    std::vector<std::uint8_t> result;   // physical result payload (digest derived by engine/client)
};

struct CompleteAckPayload {
    DecisionCode decision = DecisionCode::UNKNOWN;
    std::string reason;
    bool committed = false;
    ResultDigest committed_digest;
};

struct CancelPayload {
    ControlRequest request;
};
using CancelAckPayload = Decision;      // the Decision itself is the ack payload
using PreemptAckPayload = Decision;
using ResumeAckPayload = Decision;

struct AckPreemptPayload {
    ExecutionId execution_id;
    CoordinatorEpoch epoch;
};
struct MarkResumablePayload {
    ExecutionId execution_id;
    CoordinatorEpoch epoch;
};
struct ResumeRequestPayload {
    ExecutionId execution_id;
    CoordinatorEpoch epoch;
    ResumeGeneration resume_gen;
    WorkerId worker_id;
    WorkerBootId worker_boot_id;
};

struct WorkerLostPayload {
    ExecutionId execution_id;
    WorkerId worker_id;
    WorkerBootId worker_boot_id;
    CoordinatorEpoch epoch;
};

struct QueryPayload {
    ExecutionId execution_id;
};
struct QueryResponsePayload {
    bool found = false;
    ExecutionId execution_id;
    ExecutionState state;
    ExecutionGeneration generation;
    CoordinatorEpoch epoch;
    std::optional<AttemptId> current_attempt;
    std::optional<AttemptGeneration> current_attempt_generation;
    std::size_t attempt_count = 0;
    std::optional<ResultDigest> committed_digest;
    std::string detail;
};

struct PingPayload { std::uint64_t nonce = 0; };
using PongPayload = PingPayload;

// ---------------------------------------------------------------------------
// Frame encode/decode
// ---------------------------------------------------------------------------
struct DecodedFrame {
    MsgType type;
    std::vector<std::uint8_t> payload;
};

// Encode a full frame (header + payload). Returns the wire bytes.
std::vector<std::uint8_t> encode_frame(MsgType type, const std::vector<std::uint8_t>& payload);

// Decode one complete frame from a buffer that may contain trailing data.
// Consumes exactly kFrameHeaderLen + payload_len bytes. Validates magic,
// version, checksum, and length bounds. On failure returns false and sets
// error; on success sets out and returns the number of bytes consumed.
bool decode_frame(const std::uint8_t* data, std::size_t len, DecodedFrame& out,
                  std::size_t& consumed, std::string& error);

// ---------------------------------------------------------------------------
// Ticket / payload codec (uses ByteWriter / ByteReader)
// ---------------------------------------------------------------------------
void put_execution_id(ByteWriter& w, const ExecutionId& id);
bool get_execution_id(ByteReader& r, ExecutionId& id, std::string& err);

void put_dispatch_ticket(ByteWriter& w, const DispatchTicket& t);
bool get_dispatch_ticket(ByteReader& r, DispatchTicket& t, std::string& err);

void put_start_ticket(ByteWriter& w, const StartTicket& t);
bool get_start_ticket(ByteReader& r, StartTicket& t, std::string& err);

void put_completion_ticket(ByteWriter& w, const CompletionTicket& t);
bool get_completion_ticket(ByteReader& r, CompletionTicket& t, std::string& err);

void put_control_request(ByteWriter& w, const ControlRequest& req);
bool get_control_request(ByteReader& r, ControlRequest& req, std::string& err);

// Convenience payload pack/unpack functions.
std::vector<std::uint8_t> pack_hello(const HelloPayload& p);
bool unpack_hello(const std::vector<std::uint8_t>& b, HelloPayload& p, std::string& err);
std::vector<std::uint8_t> pack_welcome(const WelcomePayload& p);
bool unpack_welcome(const std::vector<std::uint8_t>& b, WelcomePayload& p, std::string& err);
std::vector<std::uint8_t> pack_register_ack(const RegisterAckPayload& p);
void pack_register_ack(const RegisterAckPayload& p, ByteWriter& w);
bool unpack_register_ack(const std::vector<std::uint8_t>& b, RegisterAckPayload& p, std::string& err);
std::vector<std::uint8_t> pack_submit(const SubmitPayload& p);
bool unpack_submit(const std::vector<std::uint8_t>& b, SubmitPayload& p, std::string& err);
std::vector<std::uint8_t> pack_submit_ack(const SubmitAckPayload& p);
bool unpack_submit_ack(const std::vector<std::uint8_t>& b, SubmitAckPayload& p, std::string& err);
std::vector<std::uint8_t> pack_dispatch(const DispatchRequestPayload& p);
bool unpack_dispatch(const std::vector<std::uint8_t>& b, DispatchRequestPayload& p, std::string& err);
std::vector<std::uint8_t> pack_dispatch_ack(const DispatchAckPayload& p);
bool unpack_dispatch_ack(const std::vector<std::uint8_t>& b, DispatchAckPayload& p, std::string& err);
std::vector<std::uint8_t> pack_worker_dispatch(const WorkerDispatchPayload& p);
bool unpack_worker_dispatch(const std::vector<std::uint8_t>& b, WorkerDispatchPayload& p, std::string& err);
std::vector<std::uint8_t> pack_start(const StartPayload& p);
bool unpack_start(const std::vector<std::uint8_t>& b, StartPayload& p, std::string& err);
std::vector<std::uint8_t> pack_complete(const CompletePayload& p);
bool unpack_complete(const std::vector<std::uint8_t>& b, CompletePayload& p, std::string& err);
std::vector<std::uint8_t> pack_complete_ack(const CompleteAckPayload& p);
bool unpack_complete_ack(const std::vector<std::uint8_t>& b, CompleteAckPayload& p, std::string& err);
std::vector<std::uint8_t> pack_decision(const Decision& d);
bool unpack_decision(const std::vector<std::uint8_t>& b, Decision& d, std::string& err);
std::vector<std::uint8_t> pack_cancel(const CancelPayload& p);
bool unpack_cancel(const std::vector<std::uint8_t>& b, CancelPayload& p, std::string& err);
std::vector<std::uint8_t> pack_preempt(const CancelPayload& p);
std::vector<std::uint8_t> pack_ack_preempt(const AckPreemptPayload& p);
bool unpack_ack_preempt(const std::vector<std::uint8_t>& b, AckPreemptPayload& p, std::string& err);
std::vector<std::uint8_t> pack_mark_resumable(const MarkResumablePayload& p);
bool unpack_mark_resumable(const std::vector<std::uint8_t>& b, MarkResumablePayload& p, std::string& err);
std::vector<std::uint8_t> pack_resume(const ResumeRequestPayload& p);
bool unpack_resume(const std::vector<std::uint8_t>& b, ResumeRequestPayload& p, std::string& err);
std::vector<std::uint8_t> pack_worker_lost(const WorkerLostPayload& p);
bool unpack_worker_lost(const std::vector<std::uint8_t>& b, WorkerLostPayload& p, std::string& err);
std::vector<std::uint8_t> pack_query(const QueryPayload& p);
bool unpack_query(const std::vector<std::uint8_t>& b, QueryPayload& p, std::string& err);
std::vector<std::uint8_t> pack_query_response(const QueryResponsePayload& p);
bool unpack_query_response(const std::vector<std::uint8_t>& b, QueryResponsePayload& p, std::string& err);

}  // namespace execution_fabric
