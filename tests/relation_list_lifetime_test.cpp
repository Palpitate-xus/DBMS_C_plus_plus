// A pre-main owner must still decode persisted lists at ordinary destruction.
#include "commands/RelationNameCodec.h"
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

namespace {
namespace codec = dbms::relation_name_codec;
size_t controls = 0;
void require(bool ok, const char* message) {
    ++controls;
    if (!ok) {
        std::fprintf(stderr, "RELATION_LIST_LIFETIME_FAIL %s controls=%zu\n", message, controls);
        std::_Exit(1);
    }
}
std::string fixedLegacy() {
    std::string out("base"); out.resize(64, '\0');
    out += "quote\"d"; out.resize(128, '\0');
    return out;
}
std::string fixedExtended() {
    // Independent persisted V2 fixture: two 63-byte components plus "__".
    // FNV-1a checksum is fixed externally; encode() is not the fixture oracle.
    std::string out("DBMS_RELATION_LIST_V2\\"); out.resize(64, '\0');
    out.append("\x01\0\0\0\x80\0", 6);
    out += std::string(63, 's') + "__" + std::string(63, 't');
    out.append("\x2c\x1a\x52\x3a\x8d\xcc\x55\x4a", 8);
    return out;
}
void verify() {
    const std::vector<std::string> shortNames = {"base", "quote\"d"};
    const std::vector<std::string> longNames = {std::string(63, 's') + "__" + std::string(63, 't')};
    const auto legacy = fixedLegacy(), extended = fixedExtended();
    require(codec::decode(legacy) == shortNames, "legacy bytes decode");
    require(codec::decode(extended) == longNames, "V2 bytes decode");
    require(codec::encode(shortNames) == legacy, "legacy bytes unchanged");
    require(codec::encode(longNames) == extended, "V2 bytes unchanged");
    std::string expectedMagic("DBMS_RELATION_LIST_V2\\"); expectedMagic.resize(64, '\0');
    require(codec::listMagic() == expectedMagic, "all 64 marker bytes unchanged");
    require(codec::decode("") == std::vector<std::string>{}, "empty legacy list");
    require(!codec::decode(legacy.substr(0, 127)), "truncated legacy rejected");
    require(!codec::decode(extended.substr(0, extended.size() - 1)), "truncated V2 rejected");
    auto corrupt = extended; corrupt[70] ^= 1;
    require(!codec::decode(corrupt), "V2 checksum corruption rejected");
}
struct EarlierOwner {
    ~EarlierOwner() {
        // listMagic is first used after this owner's construction. Its backing
        // bytes must remain alive when an earlier owner flushes at shutdown.
        verify();
        std::printf("RELATION_LIST_LIFETIME_COMPLETE controls=%zu late_controls=9 failures=0\n", controls);
    }
} owner;
}
int main() {
    verify();
    std::printf("RELATION_LIST_LIFETIME_MAIN controls=%zu failures=0\n", controls);
    return 0;
}
