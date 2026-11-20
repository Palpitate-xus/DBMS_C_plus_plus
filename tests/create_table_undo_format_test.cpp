#include "commands/CreateTableUndo.h"

#include <cassert>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>

namespace undo = dbms::create_table_undo;

static std::string reseal(std::string payload) {
    return payload + SHA256::hash(payload);
}

static std::string hex(std::string_view bytes) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string result;
    for (const unsigned char byte : bytes) {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 15]);
    }
    return result;
}

int main() {
    const undo::Header header{42, 3, 0};
    const auto encodedHeader = undo::encodeHeader(header);
    assert(encodedHeader.size() == 28 + undo::digestBytes);
    // Independently generated little-endian/SHA-256 format vectors.
    assert(hex(std::string_view(encodedHeader).substr(0, 28)) ==
           "43545548010000002a00000000000000030000000000000000000000");
    assert(encodedHeader.substr(28) ==
           "297b70bea06cbb67489f5185e3e9745b467fb23deefdcac41d66ef4574004174");
    undo::Header decodedHeader{999, 999, 999};
    assert(undo::decodeHeader(encodedHeader, decodedHeader));
    assert(decodedHeader.xid == 42 && decodedHeader.timeline == 3 &&
           decodedHeader.anchorLsn == 0);
    const undo::Header maxHeader{std::numeric_limits<uint64_t>::max(),
                                 std::numeric_limits<uint32_t>::max(),
                                 std::numeric_limits<uint64_t>::max()};
    assert(undo::decodeHeader(undo::encodeHeader(maxHeader), decodedHeader));
    assert(decodedHeader.xid == maxHeader.xid &&
           decodedHeader.timeline == maxHeader.timeline &&
           decodedHeader.anchorLsn == maxHeader.anchorLsn);
    assert(undo::encodeHeader({0, 1, 0}).empty());
    assert(undo::encodeHeader({1, 0, 0}).empty());

    const undo::Birth birth{1234567, "public__items", "pg_default"};
    const auto encodedBirth = undo::encodeBirth(birth);
    assert(hex(std::string_view(encodedBirth).substr(0, 43)) ==
           "435455420100000087d61200000000000d007075626c69635f5f6974656d730a0070675f64656661756c74");
    assert(encodedBirth.substr(43) ==
           "7e22c12978db12231e7229df5f8bb090531d32a50ab3d45c6c11b049e6d6f860");
    undo::Birth decodedBirth{99, "stale", "stale"};
    assert(undo::decodeBirth(encodedBirth, decodedBirth));
    assert(decodedBirth.physicalId == birth.physicalId &&
           decodedBirth.storageName == birth.storageName &&
           decodedBirth.tablespace == birth.tablespace);
    const undo::Birth longest{1, std::string(63, 's') + "__" + std::string(63, 't'),
                              std::string(63, 'x')};
    assert(undo::decodeBirth(undo::encodeBirth(longest), decodedBirth));
    assert(decodedBirth.storageName == longest.storageName &&
           decodedBirth.tablespace == longest.tablespace);

    for (const auto& invalid : {
             undo::Birth{0, "t", "pg_default"},
             undo::Birth{std::numeric_limits<uint64_t>::max(), "t", "pg_default"},
             undo::Birth{1, "", "pg_default"}, undo::Birth{1, "../t", "pg_default"},
             undo::Birth{1, std::string("t\0x", 3), "pg_default"},
             undo::Birth{1, std::string(64, 'x'), "pg_default"},
             undo::Birth{1, "t", ""}, undo::Birth{1, "t", "../space"},
             undo::Birth{1, "t", ".hidden"}, undo::Birth{1, "t", "pg_global"},
             undo::Birth{1, "t", "x:y"}, undo::Birth{1, "t", std::string(64, 'x')}}) {
        assert(undo::encodeBirth(invalid).empty());
    }

    // Every byte is protected, including the complete checksum suffix.
    for (size_t i = 0; i < encodedHeader.size(); ++i) {
        auto changed = encodedHeader;
        changed[i] ^= static_cast<char>(0x80);
        decodedHeader = {999, 999, 999};
        assert(!undo::decodeHeader(changed, decodedHeader));
        assert(decodedHeader.xid == 0 && decodedHeader.timeline == 0 &&
               decodedHeader.anchorLsn == 0);
    }
    for (size_t i = 0; i < encodedBirth.size(); ++i) {
        auto changed = encodedBirth;
        changed[i] ^= static_cast<char>(0x80);
        decodedBirth = {99, "stale", "stale"};
        assert(!undo::decodeBirth(changed, decodedBirth));
        assert(decodedBirth.physicalId == 0 && decodedBirth.storageName.empty() &&
               decodedBirth.tablespace.empty());
    }
    for (size_t n = 0; n < encodedHeader.size(); ++n)
        assert(!undo::decodeHeader(std::string_view(encodedHeader).substr(0, n), decodedHeader));
    for (size_t n = 0; n < encodedBirth.size(); ++n)
        assert(!undo::decodeBirth(std::string_view(encodedBirth).substr(0, n), decodedBirth));

    // Recomputed checksums still cannot authorize malformed semantics.
    auto headerPayload = encodedHeader.substr(0, encodedHeader.size() - undo::digestBytes);
    auto changedHeader = headerPayload;
    changedHeader[4] = 2; // unknown format version
    assert(!undo::decodeHeader(reseal(changedHeader), decodedHeader));
    changedHeader = headerPayload;
    changedHeader.replace(8, 8, 8, '\0'); // zero XID
    assert(!undo::decodeHeader(reseal(changedHeader), decodedHeader));
    changedHeader = headerPayload;
    changedHeader.replace(16, 4, 4, '\0'); // zero timeline
    assert(!undo::decodeHeader(reseal(changedHeader), decodedHeader));
    assert(!undo::decodeHeader(reseal(headerPayload + "x"), decodedHeader));

    auto birthPayload = encodedBirth.substr(0, encodedBirth.size() - undo::digestBytes);
    auto changedBirth = birthPayload;
    changedBirth[4] = 2;
    assert(!undo::decodeBirth(reseal(changedBirth), decodedBirth));
    changedBirth = birthPayload;
    changedBirth.replace(8, 8, 8, '\0'); // zero physical ID
    assert(!undo::decodeBirth(reseal(changedBirth), decodedBirth));
    changedBirth = birthPayload;
    changedBirth[16] = static_cast<char>(0xff); // length exceeds remaining bytes
    changedBirth[17] = static_cast<char>(0xff);
    assert(!undo::decodeBirth(reseal(changedBirth), decodedBirth));
    changedBirth = birthPayload;
    changedBirth[18] = '/'; // unsafe storage name
    assert(!undo::decodeBirth(reseal(changedBirth), decodedBirth));
    changedBirth = birthPayload;
    changedBirth[18 + birth.storageName.size() + 2] = '/'; // unsafe tablespace
    assert(!undo::decodeBirth(reseal(changedBirth), decodedBirth));
    assert(!undo::decodeBirth(reseal(birthPayload + "x"), decodedBirth));
    assert(!undo::decodeBirth(encodedHeader, decodedBirth));
    assert(!undo::decodeHeader(encodedBirth, decodedHeader));
    assert(!undo::decodeBirth(std::string(undo::maxRecordBytes + 1, 'a'), decodedBirth));

    std::cout << "CREATE_TABLE_UNDO_FORMAT_OK" << std::endl;
}
