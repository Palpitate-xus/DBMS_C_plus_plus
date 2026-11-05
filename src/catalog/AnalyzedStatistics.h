#pragma once

#include "expression/ExprEvaluator.h"
#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <tuple>
#include <vector>

namespace dbms::analyzed_statistics {

// Full pg_stats declaration contract. Catalog registration supplies the actual
// local view/attribute identities; these are type identities, never view OIDs.
struct Field { const char* name; uint32_t typeOid; int16_t size; };
inline constexpr std::array<Field, 17> fields{{
    {"schemaname",19,64}, {"tablename",19,64}, {"attname",19,64},
    {"inherited",16,1}, {"null_frac",700,4}, {"avg_width",23,4},
    {"n_distinct",700,4}, {"most_common_vals",2277,-1},
    {"most_common_freqs",1021,-1}, {"histogram_bounds",2277,-1},
    {"correlation",700,4}, {"most_common_elems",2277,-1},
    {"most_common_elem_freqs",1021,-1}, {"elem_count_histogram",1021,-1},
    {"range_length_histogram",2277,-1}, {"range_empty_frac",700,4},
    {"range_bounds_histogram",2277,-1}
}};

enum class Kind : uint16_t {
    MostCommon = 1, Histogram = 2, Correlation = 3, CommonElements = 4,
    ElementCounts = 5, RangeLength = 6, RangeBounds = 7
};
struct Key {
    uint32_t relation = 0;
    int16_t attribute = 0;
    bool inherited = false;
    bool operator<(const Key& b) const {
        return std::tie(relation,attribute,inherited) <
               std::tie(b.relation,b.attribute,b.inherited);
    }
};
struct Values {
    // This is the real slot element declaration. For MCV of integer[], it is
    // integer[] itself, not integer or an invented array-of-array catalog OID.
    DatumTypePtr elementType;
    std::vector<ExprValue> cells;
};
struct Slot {
    Kind kind = Kind::MostCommon;
    uint32_t comparisonOperator = 0;
    uint32_t collation = 0;
    // Absent, present-empty and present-with-NULL-child remain distinct.
    std::optional<std::vector<float>> numbers;
    std::optional<Values> values;
};
struct Column {
    // Labels are joined from a current catalog snapshot by Key. A stored path
    // or a display name cannot manufacture a namespace, relation or attribute.
    uint64_t physicalRelation = 0;
    uint64_t schemaGeneration = 0;
    DatumTypePtr attributeType;
    uint32_t collation = 0;
    int32_t target = 100;
    uint64_t sampleRows = 0;
    uint64_t nullRows = 0;
    double totalRows = 0;
    float nullFraction = 0;
    int32_t averageWidth = 0;
    float distinctEstimate = 0;
    std::vector<Slot> slots; // At most five used slots, kinds 1..7.
};
struct Snapshot {
    uint64_t generation = 0;
    std::map<Key,Column> columns;
};

// No expression, source/provider, session or parameter frame is retained.
// This owned persistence contract does not register a SQL view or compute
// unavailable legacy statistics. ANALYZE collectors supply actual records.
} // namespace dbms::analyzed_statistics
