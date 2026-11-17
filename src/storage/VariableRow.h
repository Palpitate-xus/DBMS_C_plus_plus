#pragma once

#include "common/DbError.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>

namespace dbms::detail {

inline constexpr uint16_t resolvedVariableSentinel =
    std::numeric_limits<uint16_t>::max();
inline constexpr uint32_t resolvedVariableMagic = 0x31525654u; // TVR1

struct VariableFieldSpan {
    size_t offset;
    size_t length;
    bool resolved;
};

// This marker is reserved by the existing executor-row format. A physical
// heap tuple (at most one 8 KiB page) cannot have this offset/length pair.
inline bool hasResolvedVariableMarker(std::string_view row, size_t fixedSize,
                                      size_t variableCount) {
    if (!variableCount || fixedSize > row.size() ||
        variableCount > (row.size() - fixedSize) / 4) return false;
    uint16_t offset = 0, length = 0;
    std::memcpy(&offset, row.data() + fixedSize, sizeof(offset));
    std::memcpy(&length, row.data() + fixedSize + 2, sizeof(length));
    return offset == resolvedVariableSentinel &&
           length == resolvedVariableSentinel;
}

// Both existing encodings store packed, consecutive values after a complete
// directory, including zero-length NULL/empty slots. Validate that structure
// without reading any value payload or loading external TOAST data. A bad
// directory must never silently become an empty value or another encoding.
inline VariableFieldSpan variableFieldSpan(std::string_view row,
                                           size_t fixedSize,
                                           size_t variableCount,
                                           size_t wanted) {
    const auto corrupt = []() -> void {
        throw DbError("XX001", "invalid variable-length row directory");
    };
    if (!variableCount || wanted >= variableCount || fixedSize > row.size() ||
        variableCount > (row.size() - fixedSize) / 4) corrupt();

    const size_t legacyEnd = fixedSize + variableCount * 4;
    const bool resolved = hasResolvedVariableMarker(row, fixedSize, variableCount);
    size_t entriesStart = fixedSize, entrySize = 4, dataStart = legacyEnd;
    if (resolved) {
        if (row.size() - legacyEnd < 8) corrupt();
        uint32_t magic = 0, count = 0;
        std::memcpy(&magic, row.data() + legacyEnd, sizeof(magic));
        std::memcpy(&count, row.data() + legacyEnd + 4, sizeof(count));
        if (magic != resolvedVariableMagic || count != variableCount) corrupt();
        entriesStart = legacyEnd + 8;
        entrySize = 8;
        if (variableCount > (row.size() - entriesStart) / entrySize) corrupt();
        dataStart = entriesStart + variableCount * entrySize;
    }

    size_t expectedOffset = dataStart;
    VariableFieldSpan selected{0, 0, resolved};
    for (size_t i = 0; i < variableCount; ++i) {
        size_t offset, length;
        const char* entry = row.data() + entriesStart + i * entrySize;
        if (resolved) {
            uint32_t storedOffset = 0, storedLength = 0;
            std::memcpy(&storedOffset, entry, sizeof(storedOffset));
            std::memcpy(&storedLength, entry + 4, sizeof(storedLength));
            offset = storedOffset; length = storedLength;
        } else {
            uint16_t storedOffset = 0, storedLength = 0;
            std::memcpy(&storedOffset, entry, sizeof(storedOffset));
            std::memcpy(&storedLength, entry + 2, sizeof(storedLength));
            offset = storedOffset; length = storedLength;
        }
        // Check before subtraction/addition, including attacker-sized inputs.
        if (offset != expectedOffset || offset > row.size() ||
            length > row.size() - offset) corrupt();
        expectedOffset = offset + length;
        if (i == wanted) selected = {offset, length, resolved};
    }
    if (expectedOffset != row.size()) corrupt();
    return selected;
}

} // namespace dbms::detail
