# Test executables auto-discovered by name.
set(EF_TESTS
    state_test
    engine_test
    persistence_test
    property_test
    concurrency_test
    adversarial_test
    protocol_test)

foreach(ef_test ${EF_TESTS})
    add_executable(${ef_test} ${ef_test}.cpp)
    target_link_libraries(${ef_test} PRIVATE ExecutionFabric)
    target_include_directories(${ef_test} PRIVATE ${PROJECT_SOURCE_DIR})
    if(${ef_test} STREQUAL "protocol_test")
        target_link_libraries(${ef_test} PRIVATE ExecutionFabricProtocol)
    endif()
    add_test(NAME ${ef_test} COMMAND ${ef_test})
endforeach()
