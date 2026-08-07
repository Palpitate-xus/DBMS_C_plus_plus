#include "common/scram_sha256.h"

#include <cassert>
#include <iostream>

int main() {
    const dbms::scram::Bytes highBitSalt = {
        0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87,
        0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f,
    };
    const std::string verifier = dbms::scram::makeVerifier("bobpass", highBitSalt);
    assert(verifier ==
           "SCRAM-SHA-256$4096:gIGCg4SFhoeIiYqLjI2Ojw==$"
           "c/D6pKktoBmeNQ9T99lVfNpc11qi6IvErOcLTNnhKY8=:"
           "ZldsuPy9WlTNhx/TszGOkodJX5DQpUSMJ9GiweQf75w=");
    assert(dbms::scram::verifyPassword("bobpass", verifier));
    assert(!dbms::scram::verifyPassword("wrong", verifier));
    std::cout << "[SCRAM] high-bit salt vector OK\n";
    return 0;
}
