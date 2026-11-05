#include "catalog/AnalyzedArrayStatistics.h"
#include <iostream>
#include <stdexcept>

namespace {
namespace stats = dbms::analyzed_statistics;
using namespace dbms;
size_t checks = 0;
void require(bool value, const char* message) {
    ++checks; if (!value) throw std::runtime_error(message);
}
DatumTypePtr declaration() {
    auto element = std::make_shared<DatumType>();
    element->actualOid = element->codecOid = 23; element->codecSize = 4;
    element->hasBinaryOutput = true;
    auto array = std::make_shared<DatumType>();
    array->actualOid = array->codecOid = 1007; array->kind = DatumKind::Array;
    array->arrayElement = element; array->hasBinaryOutput = true;
    return array;
}
stats::ArraySample row(const std::vector<std::optional<std::string>>& elements,
                      uint64_t ordinal, int lower = 1) {
    stats::ArraySample out; out.scalar.ordinal = ordinal;
    out.scalar.value = ExprValue("integer[]", "{", false);
    auto data = std::make_shared<CompoundDatum>(); data->type = declaration();
    data->arrayElements.emplace(); data->arrayNullBitmap = false;
    for (const auto& value : elements) {
        if (!data->arrayElements->empty()) out.scalar.value.value += ',';
        out.scalar.value.value += value ? *value : "NULL";
        data->arrayElements->emplace_back("integer", value.value_or(""), !value);
        if (!value) data->arrayNullBitmap = true;
    }
    out.scalar.value.value += '}';
    if (!elements.empty()) data->dimensions.push_back({int32_t(elements.size()), lower});
    out.scalar.value.compound = data;
    out.logicalBytes = out.scalar.value.value.size() + 4;
    out.scalar.storedWidth = out.scalar.value.value.size();
    out.scalar.tooWide = out.logicalBytes > 1024;
    return out;
}
stats::ArraySample nullRow(uint64_t ordinal) {
    stats::ArraySample out; out.scalar.ordinal = ordinal;
    out.scalar.value = ExprValue("integer[]", "", true); return out;
}
int compare(const ExprValue& a, const ExprValue& b) {
    const auto x = std::stold(a.value), y = std::stold(b.value);
    return (x > y) - (x < y);
}
stats::Column column(size_t rows, int target = 3) {
    stats::Column out; out.attributeType = declaration(); out.totalRows = rows;
    out.target = target; return out;
}
using Samples = std::vector<stats::ArraySample>;
std::optional<stats::Column> collect(const Samples& rows, int target = 3) {
    return stats::collectArrayStatistics(column(rows.size(), target), rows, 1070, 1072, 96, compare);
}
const stats::Slot* slot(const stats::Column& c, stats::Kind kind) {
    for (const auto& s : c.slots) if (s.kind == kind) return &s;
    return nullptr;
}
std::vector<std::string> cells(const stats::Column& c, stats::Kind kind) {
    std::vector<std::string> out;
    if (const auto* s = slot(c, kind)) if (s->values)
        for (const auto& value : s->values->cells) out.push_back(value.value);
    return out;
}
template<class F> void rejected(F action) {
    bool good = false;
    try { action(); } catch (const DbError& error) { good = error.sqlState() == "XX001"; }
    require(good, "malformed owning analysis input rejected");
}
} // namespace

int main() {
    try {
        Samples rows{row({"1", "1", "2", {}}, 0), row({"2", "3"}, 1),
                     row({}, 2), row({{}, {}}, 3), nullRow(4)};
        auto c = *collect(rows);
        require(c.sampleRows == 5 && c.nullRows == 1 && c.nullFraction == .2f,
                "NULL arrays have the ordinary sample denominator");
        require(c.distinctEstimate == -.8f && !slot(c, stats::Kind::MostCommon),
                "unique entire arrays have no MCV");
        require(cells(c, stats::Kind::CommonElements) == std::vector<std::string>({"1", "2", "3"}),
                "common elements sorted by actual element value");
        require(*slot(c, stats::Kind::CommonElements)->numbers ==
                std::vector<float>({.25f, .5f, .25f, .25f, .5f, .5f}),
                "membership once per non-NULL array and min/max/NULL-presence tail");
        require(*slot(c, stats::Kind::ElementCounts)->numbers == std::vector<float>({0, 0, 2, 1}),
                "distinct-element histogram includes empty and NULL-only arrays");
        require(slot(c, stats::Kind::ElementCounts)->comparisonOperator == 96 &&
                slot(c, stats::Kind::CommonElements)->values->elementType->actualOid == 23,
                "element slots retain actual equality and scalar element identity");
        require(slot(c, stats::Kind::Histogram)->values->elementType->actualOid == 1007 &&
                slot(c, stats::Kind::Histogram)->values->cells.front().compound->dimensions.empty(),
                "whole-array histogram retains real array element identity and empty owning datum");
        std::reverse(rows.begin(), rows.end());
        const auto shuffled = *collect(rows);
        require(cells(c, stats::Kind::Histogram) == cells(shuffled, stats::Kind::Histogram) &&
                slot(c, stats::Kind::CommonElements)->numbers == slot(shuffled, stats::Kind::CommonElements)->numbers &&
                slot(c, stats::Kind::Correlation)->numbers == slot(shuffled, stats::Kind::Correlation)->numbers,
                "reservoir slot order preserves physical ranks and stream order");

        rows = {row({"1.0", "1.00", "2"}, 0), row({"1", "2.0"}, 1)};
        c = *collect(rows);
        require(cells(c, stats::Kind::CommonElements).size() == 2 &&
                *slot(c, stats::Kind::CommonElements)->numbers == std::vector<float>({1, 1, 1, 1, 0}),
                "semantically equal element spellings are deduplicated within each array");

        auto a = row({"1", "2"}, 0, 0), b = row({"1", "2"}, 1, 1);
        require(stats::compareAnalyzedArrays(a.scalar.value, b.scalar.value, declaration(), compare) < 0,
                "array lower bounds distinguish otherwise equal datums");
        auto two = row({"1", "2"}, 2);
        auto dimensions = std::make_shared<CompoundDatum>(*two.scalar.value.compound);
        dimensions->dimensions = {{1, 1}, {2, 1}}; two.scalar.value.compound = dimensions;
        require(stats::compareAnalyzedArrays(b.scalar.value, two.scalar.value, declaration(), compare) < 0,
                "array dimensionality breaks equal flattened-element ties");
        require(stats::compareAnalyzedArrays(row({"2"}, 0).scalar.value,
                row({"10"}, 1).scalar.value, declaration(), compare) < 0,
                "numeric array ordering is not lexical text ordering");
        require(stats::compareAnalyzedArrays(row({"1"}, 0).scalar.value,
                row({{}}, 1).scalar.value, declaration(), compare) < 0,
                "array NULL elements sort after non-NULL values");

        rows.clear(); for (size_t i = 0; i < 2000; ++i) rows.push_back(row({std::to_string(i + 10)}, i));
        c = *collect(rows, 1);
        require(!slot(c, stats::Kind::CommonElements) &&
                *slot(c, stats::Kind::ElementCounts)->numbers == std::vector<float>({1, 1, 1}),
                "lossy pruning and significance cutoff suppress rare stream elements");
        rows.clear();
        for (size_t i = 0; i < 2000; ++i) rows.push_back(row({"1", std::to_string(i + 10)}, i));
        c = *collect(rows, 1);
        require(cells(c, stats::Kind::CommonElements) == std::vector<std::string>{"1"} &&
                *slot(c, stats::Kind::CommonElements)->numbers == std::vector<float>({1, 1, 1, 0}),
                "frequent stream member survives multiple lossy buckets");
        rows.clear();
        for (size_t i = 0; i < 40; ++i) {
            std::vector<std::optional<std::string>> values;
            for (size_t j = 1; j <= 20; ++j) if (i < 41 - j) values.push_back(std::to_string(j));
            rows.push_back(row(values, i));
        }
        c = *collect(rows, 1);
        require(cells(c, stats::Kind::CommonElements) ==
                std::vector<std::string>({"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}),
                "target-times-ten truncation selects the highest membership counts");
        require(slot(c, stats::Kind::CommonElements)->numbers->size() == 13 &&
                slot(c, stats::Kind::ElementCounts)->numbers->size() == 3,
                "target one keeps two histogram endpoints and the mean");

        std::vector<std::optional<std::string>> many(700, "1");
        auto wide = row(many, 0);
        require(wide.logicalBytes > 1024 && wide.logicalBytes < 65536, "two actual eligibility thresholds");
        auto huge = wide; huge.scalar.ordinal = 1; huge.logicalBytes = 65537;
        huge.scalar.value = ExprValue("integer[]", "", false); huge.scalar.tooWide = true;
        rows = {wide, huge, nullRow(2)}; c = *collect(rows);
        require(!slot(c, stats::Kind::MostCommon) && !slot(c, stats::Kind::Histogram) &&
                !slot(c, stats::Kind::Correlation) && cells(c, stats::Kind::CommonElements) == std::vector<std::string>{"1"},
                "wide whole arrays can still produce element statistics; oversize arrays are skipped there");
        require(*slot(c, stats::Kind::CommonElements)->numbers == std::vector<float>({1, 1, 1, 0}) &&
                *slot(c, stats::Kind::ElementCounts)->numbers == std::vector<float>({1, 1, 1, 1}),
                "specialized denominator excludes NULL and oversize arrays");
        rows = {nullRow(0), nullRow(1)}; c = *collect(rows);
        require(c.nullFraction == 1 && c.slots.empty(), "all-NULL column has no invented array slots");
        rows = {row({}, 0), row({}, 1)}; c = *collect(rows);
        require(!slot(c, stats::Kind::CommonElements) &&
                *slot(c, stats::Kind::ElementCounts)->numbers == std::vector<float>({0, 0, 0, 0}),
                "empty arrays have a real zero-count histogram without common elements");
        require(!collect({}) && !collect(rows, 0), "empty table and disabled target have no fabricated row");

        rows = {row({"1"}, 0)};
        auto bad = rows; bad[0].scalar.value.compound.reset(); rejected([&] { collect(bad); });
        bad = rows; bad[0].scalar.tooWide = true; rejected([&] { collect(bad); });
        bad = rows; bad.push_back(bad.front()); rejected([&] { collect(bad); });
        bad = rows; auto broken = std::make_shared<CompoundDatum>(*bad[0].scalar.value.compound);
        broken->dimensions[0].length = 2; bad[0].scalar.value.compound = broken;
        rejected([&] { collect(bad); });
        bad = {row({{}}, 0)}; broken = std::make_shared<CompoundDatum>(*bad[0].scalar.value.compound);
        broken->arrayNullBitmap = false; bad[0].scalar.value.compound = broken;
        rejected([&] { collect(bad); });
        bad = rows; broken = std::make_shared<CompoundDatum>(*bad[0].scalar.value.compound);
        auto wrong = std::make_shared<DatumType>(*broken->type); wrong->kind = DatumKind::Scalar;
        broken->type = wrong; bad[0].scalar.value.compound = broken;
        rejected([&] { collect(bad); });
        bad = rows; broken = std::make_shared<CompoundDatum>(*bad[0].scalar.value.compound);
        wrong = std::make_shared<DatumType>(*broken->type);
        auto wrongElement = std::make_shared<DatumType>(*wrong->arrayElement); wrongElement->actualOid = 25;
        wrong->arrayElement = wrongElement; broken->type = wrong; bad[0].scalar.value.compound = broken;
        rejected([&] { collect(bad); });
        rejected([&] { stats::collectArrayStatistics(column(1), rows, 1070, 1072, 0, compare); });

        many.assign(2000, "1"); rows = {row(many, 0)}; size_t interrupts = 0;
        bool cancelled = false;
        try {
            stats::collectArrayStatistics(column(1), rows, 1070, 1072, 96, compare,
                [&] { if (++interrupts == 4) throw DbError("57014", "test cancellation"); });
        } catch (const DbError& error) { cancelled = error.sqlState() == "57014"; }
        require(cancelled && interrupts == 4, "cancellation propagates during specialized element traversal");
        std::cout << "ANALYZED_ARRAY_COLLECTOR checks=" << checks << " failures=0\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "ANALYZED_ARRAY_COLLECTOR_FAIL checks=" << checks << ' ' << error.what() << '\n';
        return 1;
    }
}
