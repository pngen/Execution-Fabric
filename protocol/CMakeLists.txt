# Protocol/transport library used by the coordinator, workers, proofs, and examples.
add_library(ExecutionFabricProtocol STATIC
    protocol.cpp
    transport.cpp)
target_link_libraries(ExecutionFabricProtocol PUBLIC ExecutionFabric)
target_include_directories(ExecutionFabricProtocol PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/..
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>)
if(WIN32)
    target_link_libraries(ExecutionFabricProtocol PRIVATE ws2_32)
endif()
