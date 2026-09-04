#include "cuda/cuda_executor.hpp"

#include <cuda_runtime.h>
#include <cstdio>
#include <cstring>

namespace execution_fabric::cuda {

namespace {

__global__ void vector_add(const float* a, const float* b, float* c, int n) {
    const int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) { c[i] = a[i] + b[i]; }
}

bool cuda_ok(const char* what, std::string& err) {
    const cudaError_t e = cudaGetLastError();
    if (e != cudaSuccess) { err = std::string(what) + ": " + cudaGetErrorString(e); return false; }
    return true;
}

}  // namespace

bool device_memory(std::size_t& free_bytes, std::size_t& total_bytes) {
    std::size_t fb = 0, tb = 0;
    if (cudaMemGetInfo(&fb, &tb) != cudaSuccess) { return false; }
    free_bytes = fb; total_bytes = tb;
    return true;
}

bool run_vector_add(std::uint32_t n, std::uint32_t seed, std::uint32_t base,
                    std::vector<std::uint8_t>& out_bytes, ResultDigest& digest,
                    std::string& err) {
    if (n == 0) { err = "n must be nonzero"; return false; }
    std::vector<float> a(n), b(n), c(n);
    for (std::uint32_t i = 0; i < n; ++i) {
        const std::uint32_t gi = base + i;
        a[i] = static_cast<float>((seed + gi) % 10007u) / 100.0f;
        b[i] = static_cast<float>((seed * 3u + gi * 7u) % 10007u) / 100.0f;
    }

    float* da = nullptr; float* db = nullptr; float* dc = nullptr;
    if (cudaMalloc(&da, n * sizeof(float)) != cudaSuccess) { err = "cudaMalloc a failed"; return false; }
    if (cudaMalloc(&db, n * sizeof(float)) != cudaSuccess) { cudaFree(da); err = "cudaMalloc b failed"; return false; }
    if (cudaMalloc(&dc, n * sizeof(float)) != cudaSuccess) { cudaFree(da); cudaFree(db); err = "cudaMalloc c failed"; return false; }

    bool ok = true;
    if (!cuda_ok("cudaMalloc", err)) { ok = false; }
    if (ok && cudaMemcpy(da, a.data(), n * sizeof(float), cudaMemcpyHostToDevice) != cudaSuccess) {
        err = "cudaMemcpy H2D a failed"; ok = false;
    }
    if (ok && cudaMemcpy(db, b.data(), n * sizeof(float), cudaMemcpyHostToDevice) != cudaSuccess) {
        err = "cudaMemcpy H2D b failed"; ok = false;
    }
    if (ok) {
        const int threads = 256;
        const int blocks = static_cast<int>((n + threads - 1) / threads);
        vector_add<<<blocks, threads>>>(da, db, dc, static_cast<int>(n));
        if (cudaGetLastError() != cudaSuccess) { err = "kernel launch failed"; ok = false; }
    }
    if (ok && cudaDeviceSynchronize() != cudaSuccess) { err = "cudaDeviceSynchronize failed"; ok = false; }
    if (ok) {
        if (cudaMemcpy(c.data(), dc, n * sizeof(float), cudaMemcpyDeviceToHost) != cudaSuccess) {
            err = "cudaMemcpy D2H failed"; ok = false;
        }
    }
    if (ok) {
        // CPU-reference verification: c[i] == a[i] + b[i] (exact for these values).
        for (std::uint32_t i = 0; i < n; ++i) {
            const float want = a[i] + b[i];
            if (c[i] != want) { err = "CPU-reference mismatch at " + std::to_string(i); ok = false; break; }
        }
    }
    cudaFree(da); cudaFree(db); cudaFree(dc);
    if (!ok) { return false; }

    out_bytes.resize(n * sizeof(float));
    std::memcpy(out_bytes.data(), c.data(), n * sizeof(float));
    digest = ResultDigest::of_buffer(out_bytes);
    return true;
}

}  // namespace execution_fabric::cuda