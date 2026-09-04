#pragma once
#include "execution_fabric/digest.hpp"
#include <cstdint>
#include <string>
#include <vector>

namespace execution_fabric::cuda {

// ---------------------------------------------------------------------------
// CUDA executor
//
// Runs a real, verifiable vector-add on the current CUDA device:
//   cudaMalloc -> H2D (a,b) -> kernel -> synchronize -> D2H (c) -> CPU verify
//   -> cudaFree.
// Returns the result bytes (host copy of c) and its ResultDigest. On any CUDA
// or verification error it returns false and sets err. The physical result is
// deterministic for a given (n, seed), so the authority layer can recognise
// identical and conflicting completions precisely.
// ---------------------------------------------------------------------------
// base is the global index offset applied to the host-side operand generation,
// so that disjoint device partitions are exactly the corresponding ranges of one
// full continuous computation.
bool run_vector_add(std::uint32_t n, std::uint32_t seed, std::uint32_t base,
                    std::vector<std::uint8_t>& out_bytes, ResultDigest& digest,
                    std::string& err);

// Reports the device free/total memory in bytes (for the resource-cleanup
// check). Returns false if CUDA is unavailable.
bool device_memory(std::size_t& free_bytes, std::size_t& total_bytes);

}  // namespace execution_fabric::cuda