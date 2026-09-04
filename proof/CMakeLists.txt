# Multiprocess proof harness (requires the coordinator & worker executables).
add_library(ExecutionFabricProof STATIC process.cpp)
target_link_libraries(ExecutionFabricProof PUBLIC ExecutionFabricProtocol)
target_include_directories(ExecutionFabricProof PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
add_executable(ef_proof proof_harness.cpp)
target_link_libraries(ef_proof PRIVATE ExecutionFabricProof)
# The proof spawns the coordinator & worker binaries next to itself.
set_target_properties(ef_proof PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/proof)

if(EXECUTION_FABRIC_BUILD_TESTS)
    add_test(NAME multiprocess_proof COMMAND ef_proof)
    set_tests_properties(multiprocess_proof PROPERTIES
        TIMEOUT 120
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif()
