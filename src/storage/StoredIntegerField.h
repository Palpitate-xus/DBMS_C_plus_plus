#pragma once

#include "interfaces/table_schema.h"

#include <cstring>
#include <limits>
#include <optional>
#include <string_view>

namespace dbms {

struct StoredIntegerField {
    size_t offset, width;

    bool operator==(const StoredIntegerField& other) const {
        return offset == other.offset && width == other.width;
    }

    std::optional<int64_t> read(std::string_view row) const {
        if (offset > row.size() || width > row.size() - offset) return {};
        int64_t value = 0;
        std::memcpy(&value, row.data() + offset, width);
        // Match the physical encoding used by extractColumnValueStatic.
        if (width == 4 && (value & 0x80000000LL)) value |= 0xFFFFFFFF00000000LL;
        if (width == 2 && (value & 0x8000LL)) value |= 0xFFFFFFFFFFFF0000LL;
        return value;
    }
};

inline std::optional<StoredIntegerField> storedSignedIntegerField(
        const TableSchema& schema, size_t ordinal, std::string_view type) {
    if (ordinal >= schema.len()) return {};
    const auto& column = schema.cols[ordinal];
    const size_t width = type == "smallint" ? sizeof(int16_t) : type == "integer" ? sizeof(int32_t) :
        type == "bigint" ? sizeof(int64_t) : 0;
    if (!width || column.isVariableLength || column.dsize != width ||
        column.storageCodec != Column::StorageCodec::Legacy) return {};
    size_t offset = 0;
    for (size_t i = 0; i < ordinal; ++i) {
        if (schema.cols[i].isVariableLength) continue;
        if (schema.cols[i].dsize > std::numeric_limits<size_t>::max() - offset) return {};
        offset += schema.cols[i].dsize;
    }
    return StoredIntegerField{offset, width};
}

} // namespace dbms
