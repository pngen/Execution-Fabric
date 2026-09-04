#include "execution_fabric/checksum.hpp"
#include <array>

namespace execution_fabric {

const std::array<std::uint64_t, 256> Crc64::table_ = [] {
    std::array<std::uint64_t, 256> t{};
    for (std::uint64_t i = 0; i < 256; ++i) {
        std::uint64_t c = i;
        for (int k = 0; k < 8; ++k) {
            c = (c & 1) ? (0xC96C5795D7870F42ull ^ (c >> 1)) : (c >> 1);
        }
        t[i] = c;
    }
    return t;
}();

}  // namespace execution_fabric