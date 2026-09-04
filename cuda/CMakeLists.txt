# CUDA-backed execution-authority proof (built only when EXECUTION_FABRIC_USE_CUDA=ON).
enable_language(CUDA)
find_package(CUDAToolkit REQUIRED)

# RTX 5090 / Blackwell is sm_120. Fall back to a portable arch if the toolkit
# does not advertise it.
set(CMAKE_CUDA_ARCHITECTURES 120)

add_executable(ef_cuda_proof cuda_proof.cpp cuda_executor.cu)
target_link_libraries(ef_cuda_proof PRIVATE ExecutionFabric CUDA::cudart)
target_include_directories(ef_cuda_proof PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)

if(EXECUTION_FABRIC_BUILD_TESTS)
    add_test(NAME cuda_proof COMMAND ef_cuda_proof)
    set_tests_properties(cuda_proof PROPERTIES TIMEOUT 300)
endif()