#pragma once

#include "interfaces/table_schema.h"
#include <algorithm>
#include <optional>

namespace dbms::character_column {

// A legacy column has typeMod=-1 and retains its historical dsize bound.
// New VARCHAR declarations carry their actual character bound separately
// from the nominal physical width. This private negative marker represents
// an explicitly unbounded declaration; it must never escape as a PG typmod.
inline constexpr int32_t unboundedVarchar = -2;
inline constexpr int32_t maxDeclaredCharacters = 10485760;
inline constexpr size_t nominalVarcharBytes = 65535;
inline constexpr size_t maxVarlenaBytes = (size_t{1} << 30) - 1 - sizeof(uint32_t);

inline bool varying(const Column& column) noexcept {
    return column.dataType == "varchar" ||
           column.dataType == "character varying";
}

inline bool validModifier(const Column& column) noexcept {
    return varying(column) &&
        (column.typeMod == -1 || column.typeMod == unboundedVarchar ||
         (column.typeMod > 0 && column.typeMod <= maxDeclaredCharacters));
}

// Callers own a validated Column. Missing provenance in old schemas is not
// permission to infer an unbounded declaration from dsize=65535.
inline std::optional<size_t> characterLimit(const Column& column) noexcept {
    if (column.typeMod == unboundedVarchar) return std::nullopt;
    return column.typeMod > 0 ? static_cast<size_t>(column.typeMod) : column.dsize;
}

inline int32_t protocolModifier(const Column& column) noexcept {
    if (column.typeMod == unboundedVarchar) return -1;
    if (column.typeMod > 0) return column.typeMod + 4;
    // Preserve the historical descriptor of legacy nominal-capacity columns.
    return column.dsize == nominalVarcharBytes ? -1 :
        static_cast<int32_t>(column.dsize + 4);
}

inline std::string inputDeclaration(const Column& column) {
    std::string result = column.dataType;
    if (const auto bound = characterLimit(column))
        result += '(' + std::to_string(*bound) + ')';
    return result;
}

} // namespace dbms::character_column
