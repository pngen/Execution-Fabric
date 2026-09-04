#include "protocol/protocol.hpp"
#include "execution_fabric/checksum.hpp"
#include "execution_fabric/sha256.hpp"

namespace execution_fabric {

namespace {
void put_guid(ByteWriter& w, const Uuid& u) { w.bytes(u.data(), 16); }
bool get_guid(ByteReader& r, Uuid& u, std::string& err) {
    if (!r.bytes(u.data(), 16)) { err = "truncated uuid"; return false; }
    return true;
}
template <typename T> void put_id(ByteWriter& w, const Id<T>& v) { put_guid(w, v.value()); }
template <typename T> bool get_id(ByteReader& r, Id<T>& v, std::string& err) {
    Uuid u;
    if (!get_guid(r, u, err)) { return false; }
    v = Id<T>(u);
    return true;
}
template <typename T> void put_gen(ByteWriter& w, const Gen<T>& g) { w.u64(g.value()); }
template <typename T> bool get_gen(ByteReader& r, Gen<T>& g, std::string& err) {
    std::uint64_t v = 0;
    if (!r.u64(v)) { err = "truncated generation"; return false; }
    g = Gen<T>(v);
    return true;
}
}  // namespace

const char* to_string(MsgType t) noexcept {
    switch (t) {
    case MsgType::HELLO: return "HELLO";
    case MsgType::WELCOME: return "WELCOME";
    case MsgType::REGISTER_WORKER: return "REGISTER_WORKER";
    case MsgType::REGISTER_ACK: return "REGISTER_ACK";
    case MsgType::SUBMIT: return "SUBMIT";
    case MsgType::SUBMIT_ACK: return "SUBMIT_ACK";
    case MsgType::DISPATCH: return "DISPATCH";
    case MsgType::DISPATCH_ACK: return "DISPATCH_ACK";
    case MsgType::WORKER_DISPATCH: return "WORKER_DISPATCH";
    case MsgType::WORKER_DISPATCH_ACK: return "WORKER_DISPATCH_ACK";
    case MsgType::START: return "START";
    case MsgType::COMPLETE: return "COMPLETE";
    case MsgType::COMPLETE_ACK: return "COMPLETE_ACK";
    case MsgType::CANCEL: return "CANCEL";
    case MsgType::CANCEL_ACK: return "CANCEL_ACK";
    case MsgType::PREEMPT: return "PREEMPT";
    case MsgType::PREEMPT_ACK: return "PREEMPT_ACK";
    case MsgType::ACK_PREEMPT: return "ACK_PREEMPT";
    case MsgType::MARK_RESUMABLE: return "MARK_RESUMABLE";
    case MsgType::RESUME: return "RESUME";
    case MsgType::RESUME_ACK: return "RESUME_ACK";
    case MsgType::WORKER_LOST: return "WORKER_LOST";
    case MsgType::QUERY: return "QUERY";
    case MsgType::QUERY_RESPONSE: return "QUERY_RESPONSE";
    case MsgType::PING: return "PING";
    case MsgType::PONG: return "PONG";
    case MsgType::SHUTDOWN: return "SHUTDOWN";
    }
    return "UNKNOWN";
}

// ---------------------------------------------------------------------------
// Frame
// ---------------------------------------------------------------------------
std::vector<std::uint8_t> encode_frame(MsgType type, const std::vector<std::uint8_t>& payload) {
    ByteWriter w;
    w.u32(kFrameMagic);
    w.u8(kFrameVersion);
    w.u8(static_cast<std::uint8_t>(type));
    w.u32(static_cast<std::uint32_t>(payload.size()));
    w.u64(Crc64::compute(payload.data(), payload.size()));
    w.bytes(payload.data(), payload.size());
    return std::move(w).take();
}

bool decode_frame(const std::uint8_t* data, std::size_t len, DecodedFrame& out,
                  std::size_t& consumed, std::string& error) {
    if (len < kFrameHeaderLen) { error = "frame too short"; return false; }
    ByteReader r(data, len);
    std::uint32_t magic = 0;
    if (!r.u32(magic) || magic != kFrameMagic) { error = "bad frame magic"; return false; }
    std::uint8_t ver = 0;
    if (!r.u8(ver) || ver != kFrameVersion) { error = "bad frame version"; return false; }
    std::uint8_t ty = 0;
    if (!r.u8(ty)) { error = "bad frame type"; return false; }
    if (ty > static_cast<std::uint8_t>(MsgType::SHUTDOWN)) { error = "invalid message type"; return false; }
    std::uint32_t length = 0;
    if (!r.u32(length)) { error = "bad frame length"; return false; }
    if (length > kMaxPayload) { error = "frame length exceeds limit"; return false; }
    if (kFrameHeaderLen + length > len) { error = "frame payload truncated"; return false; }
    std::uint64_t crc = 0;
    if (!r.u64(crc)) { error = "bad frame crc"; return false; }
    const std::uint8_t* payload = data + kFrameHeaderLen;
    if (Crc64::compute(payload, length) != crc) { error = "frame checksum mismatch"; return false; }
    out.type = static_cast<MsgType>(ty);
    out.payload.assign(payload, payload + length);
    consumed = kFrameHeaderLen + length;
    return true;
}

// ---------------------------------------------------------------------------
// Ticket codec
// ---------------------------------------------------------------------------
void put_execution_id(ByteWriter& w, const ExecutionId& id) { put_id(w, id); }
bool get_execution_id(ByteReader& r, ExecutionId& id, std::string& err) { return get_id(r, id, err); }

void put_dispatch_ticket(ByteWriter& w, const DispatchTicket& t) {
    put_id(w, t.execution_id); put_gen(w, t.execution_generation);
    put_id(w, t.attempt_id); put_gen(w, t.attempt_generation);
    put_id(w, t.worker_id); put_id(w, t.worker_boot_id);
    put_gen(w, t.epoch); put_id(w, t.dispatch_id); put_gen(w, t.dispatch_generation);
    put_gen(w, t.ownership_generation); put_gen(w, t.fence_generation);
}
bool get_dispatch_ticket(ByteReader& r, DispatchTicket& t, std::string& err) {
    return get_id(r, t.execution_id, err) && get_gen(r, t.execution_generation, err) &&
           get_id(r, t.attempt_id, err) && get_gen(r, t.attempt_generation, err) &&
           get_id(r, t.worker_id, err) && get_id(r, t.worker_boot_id, err) &&
           get_gen(r, t.epoch, err) && get_id(r, t.dispatch_id, err) &&
           get_gen(r, t.dispatch_generation, err) && get_gen(r, t.ownership_generation, err) &&
           get_gen(r, t.fence_generation, err);
}

void put_start_ticket(ByteWriter& w, const StartTicket& t) {
    put_id(w, t.execution_id); put_gen(w, t.execution_generation);
    put_id(w, t.attempt_id); put_gen(w, t.attempt_generation);
    put_id(w, t.worker_id); put_id(w, t.worker_boot_id);
    put_gen(w, t.epoch); put_id(w, t.dispatch_id); put_gen(w, t.dispatch_generation);
    put_gen(w, t.ownership_generation); put_gen(w, t.fence_generation);
}
bool get_start_ticket(ByteReader& r, StartTicket& t, std::string& err) {
    return get_id(r, t.execution_id, err) && get_gen(r, t.execution_generation, err) &&
           get_id(r, t.attempt_id, err) && get_gen(r, t.attempt_generation, err) &&
           get_id(r, t.worker_id, err) && get_id(r, t.worker_boot_id, err) &&
           get_gen(r, t.epoch, err) && get_id(r, t.dispatch_id, err) &&
           get_gen(r, t.dispatch_generation, err) && get_gen(r, t.ownership_generation, err) &&
           get_gen(r, t.fence_generation, err);
}

void put_completion_ticket(ByteWriter& w, const CompletionTicket& t) {
    put_id(w, t.execution_id); put_gen(w, t.execution_generation);
    put_id(w, t.attempt_id); put_gen(w, t.attempt_generation);
    put_id(w, t.worker_id); put_id(w, t.worker_boot_id);
    put_gen(w, t.epoch); put_id(w, t.dispatch_id); put_gen(w, t.dispatch_generation);
    put_gen(w, t.ownership_generation); put_gen(w, t.fence_generation);
    put_gen(w, t.completion_generation);
    w.bool_byte(t.has_result);
    if (t.has_result) { w.bytes(t.result_digest.bytes().data(), 32); }
}
bool get_completion_ticket(ByteReader& r, CompletionTicket& t, std::string& err) {
    if (!get_id(r, t.execution_id, err) || !get_gen(r, t.execution_generation, err) ||
        !get_id(r, t.attempt_id, err) || !get_gen(r, t.attempt_generation, err) ||
        !get_id(r, t.worker_id, err) || !get_id(r, t.worker_boot_id, err) ||
        !get_gen(r, t.epoch, err) || !get_id(r, t.dispatch_id, err) ||
        !get_gen(r, t.dispatch_generation, err) || !get_gen(r, t.ownership_generation, err) ||
        !get_gen(r, t.fence_generation, err)) { return false; }
    if (!get_gen(r, t.completion_generation, err)) { return false; }
    bool has = false;
    if (!r.bool_byte(has)) { err = "truncated has_result"; return false; }
    t.has_result = has;
    if (has) {
        Sha256::Digest d;
        if (!r.bytes(d.data(), 32)) { err = "truncated digest"; return false; }
        t.result_digest = ResultDigest::from_bytes(d);
    }
    return true;
}

void put_control_request(ByteWriter& w, const ControlRequest& req) {
    put_id(w, req.execution_id); put_gen(w, req.epoch);
    put_gen(w, req.authority_generation);
    put_gen(w, req.cancellation_generation);
    put_gen(w, req.preemption_generation);
    put_gen(w, req.resume_generation);
}
bool get_control_request(ByteReader& r, ControlRequest& req, std::string& err) {
    return get_id(r, req.execution_id, err) && get_gen(r, req.epoch, err) &&
           get_gen(r, req.authority_generation, err) && get_gen(r, req.cancellation_generation, err) &&
           get_gen(r, req.preemption_generation, err) && get_gen(r, req.resume_generation, err);
}

// ---------------------------------------------------------------------------
// Payload pack / unpack
// ---------------------------------------------------------------------------
std::vector<std::uint8_t> pack_hello(const HelloPayload& p) {
    ByteWriter w; w.u8(p.role); w.u8(p.protocol_version);
    put_id(w, p.worker_id); put_id(w, p.worker_boot_id); w.string(p.tag);
    return std::move(w).take();
}
bool unpack_hello(const std::vector<std::uint8_t>& b, HelloPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!r.u8(p.role)) { err = "truncated role"; return false; }
    if (!r.u8(p.protocol_version)) { err = "truncated version"; return false; }
    if (!get_id(r, p.worker_id, err)) { return false; }
    if (!get_id(r, p.worker_boot_id, err)) { return false; }
    if (!r.string(p.tag)) { err = "truncated tag"; return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_welcome(const WelcomePayload& p) {
    ByteWriter w; put_gen(w, p.epoch); w.string(p.coordinator_id); w.u32(p.protocol_version);
    return std::move(w).take();
}
bool unpack_welcome(const std::vector<std::uint8_t>& b, WelcomePayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!get_gen(r, p.epoch, err)) { return false; }
    if (!r.string(p.coordinator_id)) { err = "truncated coordinator id"; return false; }
    if (!r.u32(p.protocol_version)) { err = "truncated protocol version"; return false; }
    return r.ok() && r.remaining() == 0;
}

void pack_register_ack(const RegisterAckPayload& p, ByteWriter& w) {
    w.u8(p.accepted ? 1 : 0); w.string(p.reason);
}
std::vector<std::uint8_t> pack_register_ack(const RegisterAckPayload& p) {
    ByteWriter w; pack_register_ack(p, w); return std::move(w).take();
}
bool unpack_register_ack(const std::vector<std::uint8_t>& b, RegisterAckPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    std::uint8_t a = 0;
    if (!r.u8(a)) { err = "truncated accepted"; return false; }
    p.accepted = (a != 0);
    if (!r.string(p.reason)) { err = "truncated reason"; return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_submit(const SubmitPayload& p) {
    ByteWriter w; put_id(w, p.execution_id); put_gen(w, p.epoch); return std::move(w).take();
}
bool unpack_submit(const std::vector<std::uint8_t>& b, SubmitPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!get_id(r, p.execution_id, err)) { return false; }
    if (!get_gen(r, p.epoch, err)) { return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_submit_ack(const SubmitAckPayload& p) {
    ByteWriter w; w.u8(p.accepted ? 1 : 0); put_id(w, p.execution_id);
    put_gen(w, p.generation); w.u8(static_cast<std::uint8_t>(p.state)); w.string(p.reason);
    return std::move(w).take();
}
bool unpack_submit_ack(const std::vector<std::uint8_t>& b, SubmitAckPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    std::uint8_t a = 0;
    if (!r.u8(a)) { err = "truncated accepted"; return false; }
    p.accepted = (a != 0);
    if (!get_id(r, p.execution_id, err)) { return false; }
    if (!get_gen(r, p.generation, err)) { return false; }
    std::uint8_t s = 0;
    if (!r.u8(s)) { err = "truncated state"; return false; }
    if (s > static_cast<std::uint8_t>(ExecutionState::TERMINAL)) { err = "invalid state enum"; return false; }
    p.state = static_cast<ExecutionState>(s);
    if (!r.string(p.reason)) { err = "truncated reason"; return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_dispatch(const DispatchRequestPayload& p) {
    ByteWriter w; put_id(w, p.execution_id); put_gen(w, p.epoch);
    put_id(w, p.worker_id); put_id(w, p.worker_boot_id);
    return std::move(w).take();
}
bool unpack_dispatch(const std::vector<std::uint8_t>& b, DispatchRequestPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!get_id(r, p.execution_id, err) || !get_gen(r, p.epoch, err) ||
        !get_id(r, p.worker_id, err) || !get_id(r, p.worker_boot_id, err)) { return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_dispatch_ack(const DispatchAckPayload& p) {
    ByteWriter w; w.u8(p.accepted ? 1 : 0); w.u8(static_cast<std::uint8_t>(p.decision));
    w.string(p.reason); put_dispatch_ticket(w, p.ticket);
    return std::move(w).take();
}
bool unpack_dispatch_ack(const std::vector<std::uint8_t>& b, DispatchAckPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    std::uint8_t a = 0;
    if (!r.u8(a)) { err = "truncated accepted"; return false; }
    p.accepted = (a != 0);
    std::uint8_t dc = 0;
    if (!r.u8(dc)) { err = "truncated decision"; return false; }
    if (dc > static_cast<std::uint8_t>(DecisionCode::RETRY_REJECTED)) { err = "invalid decision enum"; return false; }
    p.decision = static_cast<DecisionCode>(dc);
    if (!r.string(p.reason)) { err = "truncated reason"; return false; }
    if (!get_dispatch_ticket(r, p.ticket, err)) { return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_worker_dispatch(const WorkerDispatchPayload& p) {
    ByteWriter w; put_dispatch_ticket(w, p.ticket); w.u32(p.work_bytes); w.u32(p.work_seed);
    return std::move(w).take();
}
bool unpack_worker_dispatch(const std::vector<std::uint8_t>& b, WorkerDispatchPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!get_dispatch_ticket(r, p.ticket, err)) { return false; }
    if (!r.u32(p.work_bytes) || !r.u32(p.work_seed)) { err = "truncated work hint"; return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_start(const StartPayload& p) {
    ByteWriter w; put_start_ticket(w, p.ticket); return std::move(w).take();
}
bool unpack_start(const std::vector<std::uint8_t>& b, StartPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!get_start_ticket(r, p.ticket, err)) { return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_complete(const CompletePayload& p) {
    ByteWriter w; put_completion_ticket(w, p.ticket);
    w.u32(static_cast<std::uint32_t>(p.result.size()));
    w.bytes(p.result.data(), p.result.size());
    return std::move(w).take();
}
bool unpack_complete(const std::vector<std::uint8_t>& b, CompletePayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!get_completion_ticket(r, p.ticket, err)) { return false; }
    std::uint32_t n = 0;
    if (!r.u32(n)) { err = "truncated result length"; return false; }
    if (n > ByteReader::kMaxVector) { err = "result too large"; return false; }
    if (!r.bytes(p.result, n)) { err = "truncated result"; return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_complete_ack(const CompleteAckPayload& p) {
    ByteWriter w; w.u8(static_cast<std::uint8_t>(p.decision)); w.string(p.reason);
    w.u8(p.committed ? 1 : 0);
    if (p.committed) { w.bytes(p.committed_digest.bytes().data(), 32); }
    return std::move(w).take();
}
bool unpack_complete_ack(const std::vector<std::uint8_t>& b, CompleteAckPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    std::uint8_t dc = 0;
    if (!r.u8(dc)) { err = "truncated decision"; return false; }
    if (dc > static_cast<std::uint8_t>(DecisionCode::RETRY_REJECTED)) { err = "invalid decision enum"; return false; }
    p.decision = static_cast<DecisionCode>(dc);
    if (!r.string(p.reason)) { err = "truncated reason"; return false; }
    std::uint8_t c = 0;
    if (!r.u8(c)) { err = "truncated committed"; return false; }
    p.committed = (c != 0);
    if (p.committed) {
        Sha256::Digest d;
        if (!r.bytes(d.data(), 32)) { err = "truncated committed digest"; return false; }
        p.committed_digest = ResultDigest::from_bytes(d);
    }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_decision(const Decision& d) {
    ByteWriter w;
    w.u8(static_cast<std::uint8_t>(d.code)); w.string(d.reason);
    w.u8(d.execution_id ? 1 : 0); if (d.execution_id) { put_id(w, *d.execution_id); }
    w.u8(d.execution_generation ? 1 : 0); if (d.execution_generation) { put_gen(w, *d.execution_generation); }
    return std::move(w).take();
}
bool unpack_decision(const std::vector<std::uint8_t>& b, Decision& d, std::string& err) {
    ByteReader r(b.data(), b.size());
    std::uint8_t dc = 0;
    if (!r.u8(dc)) { err = "truncated decision"; return false; }
    if (dc > static_cast<std::uint8_t>(DecisionCode::RETRY_REJECTED)) { err = "invalid decision enum"; return false; }
    d.code = static_cast<DecisionCode>(dc);
    if (!r.string(d.reason)) { err = "truncated reason"; return false; }
    std::uint8_t has = 0;
    if (!r.u8(has)) { err = "truncated opt"; return false; }
    if (has) { ExecutionId id; if (!get_id(r, id, err)) { return false; } d.execution_id = id; }
    if (!r.u8(has)) { err = "truncated opt"; return false; }
    if (has) { ExecutionGeneration g; if (!get_gen(r, g, err)) { return false; } d.execution_generation = g; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_cancel(const CancelPayload& p) {
    ByteWriter w; put_control_request(w, p.request); return std::move(w).take();
}
bool unpack_cancel(const std::vector<std::uint8_t>& b, CancelPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!get_control_request(r, p.request, err)) { return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_preempt(const CancelPayload& p) { return pack_cancel(p); }

std::vector<std::uint8_t> pack_ack_preempt(const AckPreemptPayload& p) {
    ByteWriter w; put_id(w, p.execution_id); put_gen(w, p.epoch); return std::move(w).take();
}
bool unpack_ack_preempt(const std::vector<std::uint8_t>& b, AckPreemptPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!get_id(r, p.execution_id, err) || !get_gen(r, p.epoch, err)) { return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_mark_resumable(const MarkResumablePayload& p) {
    ByteWriter w; put_id(w, p.execution_id); put_gen(w, p.epoch); return std::move(w).take();
}
bool unpack_mark_resumable(const std::vector<std::uint8_t>& b, MarkResumablePayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!get_id(r, p.execution_id, err) || !get_gen(r, p.epoch, err)) { return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_resume(const ResumeRequestPayload& p) {
    ByteWriter w; put_id(w, p.execution_id); put_gen(w, p.epoch); put_gen(w, p.resume_gen);
    put_id(w, p.worker_id); put_id(w, p.worker_boot_id);
    return std::move(w).take();
}
bool unpack_resume(const std::vector<std::uint8_t>& b, ResumeRequestPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!get_id(r, p.execution_id, err) || !get_gen(r, p.epoch, err) ||
        !get_gen(r, p.resume_gen, err) || !get_id(r, p.worker_id, err) || !get_id(r, p.worker_boot_id, err)) { return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_worker_lost(const WorkerLostPayload& p) {
    ByteWriter w; put_id(w, p.execution_id); put_id(w, p.worker_id);
    put_id(w, p.worker_boot_id); put_gen(w, p.epoch);
    return std::move(w).take();
}
bool unpack_worker_lost(const std::vector<std::uint8_t>& b, WorkerLostPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!get_id(r, p.execution_id, err) || !get_id(r, p.worker_id, err) ||
        !get_id(r, p.worker_boot_id, err) || !get_gen(r, p.epoch, err)) { return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_query(const QueryPayload& p) {
    ByteWriter w; put_id(w, p.execution_id); return std::move(w).take();
}
bool unpack_query(const std::vector<std::uint8_t>& b, QueryPayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    if (!get_id(r, p.execution_id, err)) { return false; }
    return r.ok() && r.remaining() == 0;
}

std::vector<std::uint8_t> pack_query_response(const QueryResponsePayload& p) {
    ByteWriter w;
    w.u8(p.found ? 1 : 0); put_id(w, p.execution_id); w.u8(static_cast<std::uint8_t>(p.state));
    put_gen(w, p.generation); put_gen(w, p.epoch);
    w.u8(p.current_attempt ? 1 : 0); if (p.current_attempt) { put_id(w, *p.current_attempt); }
    w.u8(p.current_attempt_generation ? 1 : 0); if (p.current_attempt_generation) { put_gen(w, *p.current_attempt_generation); }
    w.u64(p.attempt_count);
    w.u8(p.committed_digest ? 1 : 0); if (p.committed_digest) { w.bytes(p.committed_digest->bytes().data(), 32); }
    w.string(p.detail);
    return std::move(w).take();
}
bool unpack_query_response(const std::vector<std::uint8_t>& b, QueryResponsePayload& p, std::string& err) {
    ByteReader r(b.data(), b.size());
    std::uint8_t f = 0;
    if (!r.u8(f)) { err = "truncated found"; return false; }
    p.found = (f != 0);
    if (!get_id(r, p.execution_id, err)) { return false; }
    std::uint8_t s = 0;
    if (!r.u8(s)) { err = "truncated state"; return false; }
    if (s > static_cast<std::uint8_t>(ExecutionState::TERMINAL)) { err = "invalid state enum"; return false; }
    p.state = static_cast<ExecutionState>(s);
    if (!get_gen(r, p.generation, err) || !get_gen(r, p.epoch, err)) { return false; }
    std::uint8_t has = 0;
    if (!r.u8(has)) { err = "truncated opt"; return false; }
    if (has) { AttemptId id; if (!get_id(r, id, err)) { return false; } p.current_attempt = id; }
    if (!r.u8(has)) { err = "truncated opt"; return false; }
    if (has) { AttemptGeneration g; if (!get_gen(r, g, err)) { return false; } p.current_attempt_generation = g; }
    if (!r.u64(p.attempt_count)) { err = "truncated count"; return false; }
    if (!r.u8(has)) { err = "truncated opt"; return false; }
    if (has) { Sha256::Digest d; if (!r.bytes(d.data(), 32)) { err = "truncated digest"; return false; } p.committed_digest = ResultDigest::from_bytes(d); }
    if (!r.string(p.detail)) { err = "truncated detail"; return false; }
    return r.ok() && r.remaining() == 0;
}

}  // namespace execution_fabric