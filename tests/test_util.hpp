#pragma once
#include <cstdio>
#include <cstdlib>

namespace eftest {
inline int checks = 0;
inline int failures = 0;
}  // namespace eftest

#define CHECK(cond) do { \
    ++eftest::checks; \
    if (!(cond)) { ++eftest::failures; std::printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); } \
} while (0)

#define CHECK_EQ(a, b) do { \
    ++eftest::checks; \
    auto _va = (a); auto _vb = (b); \
    if (!(_va == _vb)) { ++eftest::failures; std::printf("FAIL %s:%d: %s == %s\n", __FILE__, __LINE__, #a, #b); } \
} while (0)

#define CHECK_MSG(cond, msg) do { \
    ++eftest::checks; \
    if (!(cond)) { ++eftest::failures; std::printf("FAIL %s:%d: %s (%s)\n", __FILE__, __LINE__, #cond, msg); } \
} while (0)

#define TEST_MAIN_RETURN() \
    (std::printf("[%s] %d checks, %d failures\n", __FILE__, eftest::checks, eftest::failures), \
     (eftest::failures == 0 ? 0 : 1))