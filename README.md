# Execution Fabric

**Execution Fabric is an open-source, vendor-neutral C++20 runtime for governing distributed execution authority across attempts, retries, ownership, fencing, cancellation, preemption, resume, completion, and logical commit across heterogeneous AI infrastructure.**

It answers one systems question:

> **Which execution attempt is authoritative right now, who owns it, what may still run, and which completion is allowed to become real?**

The defining thesis is:

> **Distributed execution is not just starting work on a worker. It is governing which attempt remains authoritative as retries, cancellation, preemption, process death, restarts, duplicate messages, ambiguous completion, and recovery occur.**

---

## The boundary

Execution Fabric sits *beneath* schedulers and resource-placement systems and owns **execution authority only**. It deliberately does not own the neighbouring concerns:

| Repository | Owns |
|---|---|
| **Compute Fabric** | where computation should run (placement) |
| **Resource Broker** | arbitration of the resource portfolio (compute, memory, bandwidth) |
| **Checkpoint Fabric** | durable execution *state* that must survive |
| **Failure Fabric** | failure classification, compensation, and recovery semantics |
| **Execution Fabric** | which physical execution attempt has **authority** to affect the logical outcome |

Execution Fabric is not a scheduler, not a resource broker, not a checkpoint runtime, and not a failure-classification service. It is the **authority** layer.

---

## What it makes explicit

A **logical execution** is distinct from every physical attempt made on its behalf. One logical execution may have no attempt yet, one current attempt, many historical attempts, a failed-then-retried attempt, a preempted-then-resumed attempt, an attempt whose physical outcome is unknown, an old attempt still physically running after authority moved, or speculative/redundant physical attempts where only explicitly authorised completion may commit.

The runtime never equates **"code ran"** with **"the result is authoritative."**

Execution authority is fenced through strongly-typed identities and monotonically advancing generations:

- `ExecutionId`, `ExecutionGeneration`
- `AttemptId`, `AttemptGeneration`
- `CoordinatorEpoch`
- `WorkerId`, `WorkerBootId`
- `DispatchId`, `DispatchGeneration`
- `OwnershipGeneration`
- `FenceGeneration` (alias `LeaseGeneration`)
- `CancellationGeneration`, `PreemptionGeneration`, `ResumeGeneration`
- `CompletionGeneration`, `CommitGeneration`

These are distinct C++ types, not collapsed into untyped integers or strings — a `WorkerId` cannot be silently supplied where an `ExecutionId` is required.

---

## Lifecycle state machines

Logical executions and physical attempts have **guarded** lifecycles; invalid transitions are rejected. Terminal states are **absorbing** — a committed outcome can never be silently reopened.

The execution grammar (the vocabulary used by the runtime):

`CREATED → READY → DISPATCHED → RUNNING → { CANCELLATION_REQUESTED, PREEMPTION_REQUESTED, PREEMPTED, RESUMABLE, RESUMING, COMPLETION_PENDING, COMPLETED, COMMITTED, FAILED, CANCELLED, AMBIGUOUS, SUPERSEDED, TERMINAL }`

---

## Exactly-once logical commit

Physical work may execute more than once. The invariant the runtime upholds is:

> **At most one authorised terminal result may commit for a given logical execution generation.**

This is *exactly-once logical commit* — or, phrased honestly, **exactly-once-ish execution semantics with at-most-one authoritative commit**. The runtime does not claim impossible general exactly-once physical execution.

Completion is two-stage where useful:

**physical completion → authority validation → logical commit**

A completion carries sufficient authority to prove execution identity, execution generation, attempt identity/generation, worker identity, `WorkerBootId`, coordinator epoch, dispatch authority, ownership/fence authority, and a result identity/digest. Only current authority may cross the commit boundary. Conflicting completions fail loudly; duplicate identical completions are recognised idempotently but never double-commit.

---

## Decisions are explicit, not implicit

Unknown never becomes implicit permission. Every mutation produces a machine-readable `Decision` with an explanatory message and the authoritative context required to answer:

- Why was this dispatch accepted/rejected?
- Why is this attempt stale?
- Who owns execution now?
- May this attempt continue / complete / commit?
- Which generation superseded this attempt?
- What authority must change before this could become valid?

The taxonomy: `ALLOW`, `REJECT_STALE_EPOCH`, `REJECT_STALE_BOOT`, `REJECT_STALE_ATTEMPT`, `REJECT_STALE_GENERATION`, `REJECT_NOT_OWNER`, `REJECT_ALREADY_TERMINAL`, `REJECT_CANCELLED`, `REJECT_PREEMPTED`, `REJECT_CONFLICTING_COMPLETION`, `REJECT_ALREADY_COMMITTED`, `REJECT_INSUFFICIENT_AUTHORITY`, `REJECT_EXISTS`, `REJECT_UNKNOWN_EXECUTION`, `REJECT_NO_CURRENT_ATTEMPT`, `RETRY_ALLOWED`, `RESUME_ALLOWED`, `RETRY_REJECTED`, `DEFER`, `UNKNOWN`.

---

## Architecture

```
include/execution_fabric/   public API (authority model, engine, persistence)
src/                        core implementation
protocol/                   framed wire protocol + TCP transport
coordinator/                the authoritative coordinator process
worker/                     the worker runtime + process
proof/                      multiprocess proof harness
tests/                      unit, property, concurrency, adversarial
benchmarks/                 runtime-primitive benchmarks
examples/                   API + protocol demos
tools/                      CLI inspection tool
cuda/                       CUDA-backed execution proof (RTX 5090 / sm_120)
consumer/                   downstream find_package consumer test
```

The public API exposes the execution-authority model without forcing users to adopt the built-in TCP coordinator. Transport is behind an interface.

---

## Building

### Non-CUDA (library, tests, coordinator, workers, proof, examples, benchmarks, tools)

```sh
cmake -G "Visual Studio 17 2022" -S . -B build
cmake --build build --config Release
ctest -C Release --test-dir build --output-on-failure
```

### CUDA (RTX 5090 / sm_120)

```sh
cmake -G "Visual Studio 17 2022" -S . -B build-cuda -DEXECUTION_FABRIC_USE_CUDA=ON
cmake --build build-cuda --config Release
ctest -C Release --test-dir build-cuda -R cuda_proof
```

### Installing

```sh
cmake --install build --prefix <prefix>
```

Then downstream:

```cmake
find_package(ExecutionFabric CONFIG REQUIRED)
target_link_libraries(app PRIVATE ExecutionFabric::ExecutionFabric)
```

---

## Running the multiprocess proof

The proof (`ef_proof`) spawns a real coordinator, two worker processes, and a controller over loopback TCP. It demonstrates: real worker-kill and restart; fresh `WorkerBootId` semantics; stale-authority rejection; the retry/commit sequence; duplicate and conflicting completion handling; coordinator epoch rollover; persistence/recovery; and that old dynamic worker authority is never resurrected.

```sh
ctest -C Release --test-dir build -R multiprocess_proof --output-on-failure
```

---

## CUDA proof

On the local NVIDIA RTX 5090 (compute capability 12.0 / sm_120), the CUDA proof runs **real device work** (`cudaMalloc`, H2D, kernel, synchronization, D2H, `cudaFree`) and exercises every authority scenario: normal completion (A), stale completion after retry (B), cancellation race (C), preemption/resume with deterministic progress partitioning (D), and ambiguous completion / lost ACK (E). Device memory returns to baseline after validation.

**What is proven vs. not:** process death is proven by the real OS-kill in the *TCP multiprocess* proof. The CUDA *in-process* proof demonstrates the authority semantics around real GPU execution; the stale-authority advance in scenario B is deterministically orchestrated (a real device partition is superseded by a newer attempt), and scenario D uses **application/runtime-level cooperative preemption between kernel launches** — it does not claim kernel-level GPU preemption. The machine exposes a **single physical GPU** (RTX 5090); no multi-GPU, multi-node, RDMA, NVLink, or hardware-preemption behaviour is manufactured.

---

## Tests

- `state_test` — guarded transition tables, terminal-state absorption, SHA-256 known-answer.
- `engine_test` — full authority flows: happy path, stale boot/epoch/attempt, duplicate & conflicting completion, retry after worker loss, cancellation, preemption/resume, terminal guards.
- `persistence_test` — round-trip, history preservation, and rejection of truncation, corruption, trailing garbage, and invalid enum state.
- `property_test` — 20,000 randomized legal/illegal event sequences with invariant checking.
- `concurrency_test` — genuine multi-threaded producers racing through a serialised decision point.
- `adversarial_test` — duplicate identity, old-coordinator rollover, stale-owner mutation, impossible persisted states.
- `multiprocess_proof` — real coordinator/worker TCP + process kill/restart + recovery.
- `cuda_proof` — CUDA-backed authority scenarios A–E.

---

## Benchmarks

`ef_benchmark` reports genuine throughput of the runtime primitives (`execution create`, `attempt dispatch`, `completion validate + commit`, `persistence save/recover`, `protocol encode/decode`, `concurrent event ingestion`) with full workload detail — not vanity numbers.

---

## Honest limitations

- **Physical execution is not exactly-once.** Only the *logical commit* is exactly-once.
- **No hardware/kernel preemption** is claimed; preemption is runtime/application-level and cooperative.
- The local machine exposes **one physical GPU**.
- `UNKNOWN`/`DEFER` are never treated as permission.
- Recovered process-local/live authority is never treated as automatically current; it requires revalidation.

---

## License

Apache-2.0 (see `LICENSE`).
