cmake_minimum_required(VERSION 3.20)
project(ef_consumer CXX)

# Prove the installed package is consumable via find_package.
find_package(ExecutionFabric CONFIG REQUIRED)

add_executable(ef_consumer main.cpp)
target_link_libraries(ef_consumer PRIVATE ExecutionFabric::ExecutionFabric)