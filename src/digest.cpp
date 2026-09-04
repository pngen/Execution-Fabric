#include "execution_fabric/digest.hpp"

namespace execution_fabric {

std::string ResultDigest::to_hex() const {
    static const char* hex = "0123456789abcdef";
    std::string out;
    out.reserve(64);
    for (auto b : bytes_) {
        out.push_back(hex[b >> 4]);
        out.push_back(hex[b & 0x0F]);
    }
    return out;
}

}  // namespace execution_fabric