#include "common/sha2_extended.h"

#include <array>
#include <cassert>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <tuple>

template <std::size_t N>
static std::string hexDigest(const std::array<uint8_t, N>& digest) {
    std::ostringstream out;
    for (const uint8_t byte : digest) {
        out << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<unsigned int>(byte);
    }
    return out.str();
}

static void verifyVectors(const std::string& input,
                          const std::string& sha224,
                          const std::string& sha384,
                          const std::string& sha512) {
    const auto digest224 = SHA224::digestBytes(input);
    const auto digest384 = SHA384::digestBytes(input);
    const auto digest512 = SHA512::digestBytes(input);
    static_assert(std::tuple_size<decltype(digest224)>::value == 28, "SHA-224 size");
    static_assert(std::tuple_size<decltype(digest384)>::value == 48, "SHA-384 size");
    static_assert(std::tuple_size<decltype(digest512)>::value == 64, "SHA-512 size");
    assert(hexDigest(digest224) == sha224);
    assert(hexDigest(digest384) == sha384);
    assert(hexDigest(digest512) == sha512);
    assert(SHA224::hash(input) == sha224);
    assert(SHA384::hash(input) == sha384);
    assert(SHA512::hash(input) == sha512);
}

int main() {
    verifyVectors(
        "",
        "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
        "38b060a751ac96384cd9327eb1b1e36a21fdb71114be07434c0cc7bf63f6e1da"
        "274edebfe76f65fbd51ad2f14898b95b",
        "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce"
        "47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e");

    verifyVectors(
        "abc",
        "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7",
        "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed"
        "8086072ba1e7cc2358baeca134c825a7",
        "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2"
        "192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f");

    const std::string binary("\0abc\0", 5);
    assert(binary.size() == 5);
    verifyVectors(
        binary,
        "a29e18bf3df333b8289f6769d0761f559c0d2f0f2abed0ad51457fd6",
        "278217c228368518a415a50853162abd4253541b6600e1ab4896150afce0f0c5"
        "26b6559d804e51bc08e6fdb9954217ac",
        "bfb2b7e1fad00b91e7cc9a0111a6a31cb0b7cd310298d56be47adeec765b8a6f"
        "db1956aea9d02c728336bd60852f553dc0c53e29e33fbef42d1847683ef07044");

    std::cout << "[SHA2 EXTENDED] SHA-224/SHA-384/SHA-512 vectors OK\n";
    return 0;
}
