#include "storage/HeapWalIdentity.h"

#include <cassert>
#include <iostream>

using namespace dbms;
namespace codec = dbms::heap_wal_identity;

static XLogRecord pageImage() {
    XLogRecord record{};
    record.header.xl_info = (uint32_t{RM_HEAP_ID} << 8) | XLOG_HEAP_PAGE_AFTER;
    codec::append(record.data, uint32_t{5});
    record.data.insert(record.data.end(), {'i', 't', 'e', 'm', 's'});
    codec::append(record.data, uint32_t{1});
    codec::append(record.data, uint32_t{0});
    codec::append(record.data, uint16_t{0});
    codec::append(record.data, uint32_t{8192});
    record.data.resize(record.data.size() + 8192, '\0');
    return record;
}

int main() {
    std::optional<uint64_t> identity;
    auto legacy = pageImage();
    assert(codec::image(legacy, identity) && !identity);
    for (int padding = 0; padding <= 7; ++padding) {
        auto padded = legacy;
        padded.data.resize(padded.data.size() + padding, '\0');
        assert(codec::image(padded, identity) && !identity);
    }
    auto current = pageImage();
    codec::appendImage(current.data, 99);
    assert(codec::image(current, identity) && identity == 99);
    for (int missing = 1; missing <= 16; ++missing) {
        auto truncated = current;
        truncated.data.resize(truncated.data.size() - missing);
        // Removing the entire explicit extension yields the legacy prefix;
        // other partial extension bytes must never appear legacy.
        assert(codec::image(truncated, identity) == (missing == 16));
    }
    auto malformed = current;
    malformed.data.back() = 1;
    assert(codec::image(malformed, identity) && identity != 99);
    malformed = current;
    std::fill(malformed.data.end() - 8, malformed.data.end(), '\0');
    assert(!codec::image(malformed, identity));
    malformed = current;
    malformed.data[malformed.data.size() - 16] ^= 1;
    assert(!codec::image(malformed, identity));
    malformed = current;
    malformed.data.push_back('x');
    assert(!codec::image(malformed, identity));
    malformed = legacy;
    malformed.data.resize(malformed.data.size() + 8, '\0');
    assert(!codec::image(malformed, identity));

    XLogRecord event{};
    event.header.xl_info = (uint32_t{RM_SMGR_ID} << 8) | XLOG_SMGR_RELATION_CREATE;
    event.data = codec::event(99, "items");
    codec::Event decoded;
    assert(codec::event(event, decoded) && decoded.relationId == 99 && decoded.name == "items");
    event.header.xl_info = (uint32_t{RM_SMGR_ID} << 8) | XLOG_SMGR_RELATION_RETIRE;
    assert(codec::event(event, decoded));
    event.header.xl_info = (uint32_t{RM_SMGR_ID} << 8) | XLOG_SMGR_RELATION_RETIRE_COMPLETE;
    assert(codec::event(event, decoded));
    for (size_t length = 0; length < event.data.size(); ++length) {
        auto truncated = event;
        truncated.data.resize(length);
        assert(!codec::event(truncated, decoded));
    }
    event.data.push_back('x');
    assert(!codec::event(event, decoded));
    assert(codec::event(0, "items").empty());
    assert(codec::event(1, "").empty());
    assert(codec::event(1, std::string(64, 'x')).empty());
    assert(codec::event(1, std::string("a\0b", 3)).empty());
    std::vector<char> truncate;
    codec::append(truncate, uint32_t{5});
    truncate.insert(truncate.end(), {'i', 't', 'e', 'm', 's'});
    std::string name;
    assert(codec::nameIdentity(truncate, name, identity) && name == "items" && !identity);
    codec::appendImage(truncate, 99);
    assert(codec::nameIdentity(truncate, name, identity) && name == "items" && identity == 99);
    for (int missing = 1; missing < 16; ++missing) {
        auto truncated = truncate;
        truncated.resize(truncated.size() - missing);
        assert(!codec::nameIdentity(truncated, name, identity));
    }
    truncate.push_back('x');
    assert(!codec::nameIdentity(truncate, name, identity));
    std::cout << "[HEAP WAL IDENTITY CODEC] strict legacy/current image and lifecycle bounds passed\n";
}
