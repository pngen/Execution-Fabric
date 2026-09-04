# Examples of the public API and protocol.
add_executable(ef_example_basic basic_authority.cpp)
target_link_libraries(ef_example_basic PRIVATE ExecutionFabric)

add_executable(ef_example_protocol protocol_demo.cpp)
target_link_libraries(ef_example_protocol PRIVATE ExecutionFabricProtocol)
