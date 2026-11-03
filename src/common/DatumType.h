#pragma once
#include <cstdint>
#include <memory>
#include <vector>
namespace dbms {
// Copied catalog/analyzed declaration metadata only. No evaluator, AST,
// Session, prepared execution, provider or parameter-frame is borrowed.
enum class DatumKind : uint8_t { Scalar, Record, Array };
struct DatumType;
struct CompoundDatum;
using DatumTypePtr = std::shared_ptr<const DatumType>;
struct DatumType {
    uint32_t actualOid = 0; // actual declaration, retained even for a domain
    uint32_t codecOid = 0;  // resolved base type identity; kind selects structure
    int16_t codecSize = -1; // same-snapshot base typlen for RowDescription
    int32_t effectiveTypeMod = -1; // inherited domain or actual declaration typmod
    DatumKind kind = DatumKind::Scalar;
    bool enumType = false;
    // Resolved internal output role, from genuine catalog kind or immutable
    // builtin definitions. This does NOT claim pg_type.typsend is populated.
    bool hasBinaryOutput = false;
    DatumTypePtr arrayElement;
    // Continuous field declarations, including actual OIDs of NULL fields.
    // Every non-NULL anonymous record value retains its own actual layout.
    std::vector<DatumType> recordFields;
};
struct DatumArrayDimension {
    int32_t length = 0;
    int32_t lower = 1;
};
} // namespace dbms
