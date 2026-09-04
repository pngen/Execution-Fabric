cmake_minimum_required(VERSION 3.20)

project(ExecutionFabric
    VERSION 1.0.0
    DESCRIPTION "Open-source, vendor-neutral C++20 runtime for governing distributed execution authority"
    LANGUAGES CXX)

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
option(EXECUTION_FABRIC_BUILD_TESTS      "Build Execution Fabric tests"       ON)
option(EXECUTION_FABRIC_BUILD_EXAMPLES   "Build Execution Fabric examples"    ON)
option(EXECUTION_FABRIC_BUILD_BENCHMARKS "Build Execution Fabric benchmarks"  ON)
option(EXECUTION_FABRIC_BUILD_TOOLS      "Build Execution Fabric tools"       ON)
option(EXECUTION_FABRIC_BUILD_PROOFS     "Build multiprocess & CUDA proofs"   ON)
option(EXECUTION_FABRIC_USE_CUDA         "Enable the CUDA-backed execution proof" OFF)
option(EXECUTION_FABRIC_COVERAGE         "Enable coverage instrumentation"     OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Testing is enabled early so that every component (unit tests and the
# multiprocess proof) can register CTest tests.
enable_testing()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# ---------------------------------------------------------------------------
# Compiler strictness - never suppress project warnings for green output.
# ---------------------------------------------------------------------------
add_library(ef_warnings INTERFACE)
if(MSVC)
    # Strict host-C++ warnings only. CUDA sources must not receive raw MSVC
    # flags (nvcc rejects them); they are compiled with the toolchain defaults.
    target_compile_options(ef_warnings INTERFACE
        $<$<COMPILE_LANGUAGE:CXX>:/W4 /WX /permissive- /Zc:__cplusplus>
        $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=/W4>)
else()
    target_compile_options(ef_warnings INTERFACE
        $<$<COMPILE_LANGUAGE:CXX>:-Wall -Wextra -Werror -fno-common>)
endif()

# ---------------------------------------------------------------------------
# Core library
# ---------------------------------------------------------------------------
file(GLOB_RECURSE EXECUTION_FABRIC_SOURCES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

add_library(ExecutionFabric STATIC ${EXECUTION_FABRIC_SOURCES})
add_library(ExecutionFabric::ExecutionFabric ALIAS ExecutionFabric)

target_include_directories(ExecutionFabric PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(ExecutionFabric PUBLIC $<BUILD_INTERFACE:ef_warnings>)

# Consumers of the installed package automatically get C++20 (required by the headers).
target_compile_features(ExecutionFabric PUBLIC cxx_std_20)

if(MSVC)
    target_compile_options(ExecutionFabric PRIVATE $<$<CONFIG:Release>:/O2>)
    target_compile_options(ExecutionFabric PRIVATE $<$<CONFIG:Debug>:/Ob0 /RTC1>)
endif()

set_target_properties(ExecutionFabric PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR})

# ---------------------------------------------------------------------------
# Components / subdirectories
# ---------------------------------------------------------------------------
if(EXECUTION_FABRIC_BUILD_PROOFS)
    add_subdirectory(protocol)
    add_subdirectory(coordinator)
    add_subdirectory(worker)
    add_subdirectory(proof)
endif()

if(EXECUTION_FABRIC_BUILD_TESTS)
    add_subdirectory(tests)
endif()

if(EXECUTION_FABRIC_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(EXECUTION_FABRIC_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

if(EXECUTION_FABRIC_BUILD_TOOLS)
    add_subdirectory(tools)
endif()

if(EXECUTION_FABRIC_USE_CUDA)
    add_subdirectory(cuda)
endif()

# ---------------------------------------------------------------------------
# Install + package
# ---------------------------------------------------------------------------
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/execution_fabric
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(TARGETS ExecutionFabric
        EXPORT ExecutionFabricTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

install(EXPORT ExecutionFabricTargets
        FILE ExecutionFabricTargets.cmake
        NAMESPACE ExecutionFabric::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ExecutionFabric)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/ExecutionFabricConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/ExecutionFabricConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ExecutionFabric)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/ExecutionFabricConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/ExecutionFabricConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/ExecutionFabricConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ExecutionFabric)