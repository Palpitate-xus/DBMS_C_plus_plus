#pragma once

#include "catalog/AnalyzedStatistics.h"
#include "common/DbError.h"
#include "types/numeric.h"
#include <algorithm>
#include <cmath>
#include <functional>
#include <iomanip>
#include <limits>
#include <numeric>
#include <sstream>

namespace dbms::analyzed_statistics {

struct RangeBound {
    std::string text;
    bool lower = false;
    bool unbounded = false;
    bool inclusive = false;
    // Optional keys belong to this sample only. They never enter the stored
    // snapshot, a session cache, an expression frame or a provider.
    std::optional<int64_t> integer;
    std::shared_ptr<const Numeric> numeric;
};
struct RangeSample {
    uint64_t ordinal = 0;
    uint32_t storedWidth = 0;
    bool isNull = false;
    bool empty = false;
    RangeBound lower, upper;
};
using RangeValueComparison = std::function<int(const RangeBound&, const RangeBound&)>;
using RangeDistance = std::function<double(const RangeBound&, const RangeBound&)>;
using RangeConstruction = std::function<ExprValue(const RangeBound&, const RangeBound&)>;

// Includes position and inclusivity, not just subtype values. In particular,
// equal exclusive upper/lower bounds do not represent the same boundary.
inline int compareRangeBounds(const RangeBound& a, const RangeBound& b,
                              const RangeValueComparison& compare) {
    if (a.unbounded || b.unbounded) {
        if (a.unbounded && b.unbounded) return a.lower == b.lower ? 0 : a.lower ? -1 : 1;
        return a.unbounded ? (a.lower ? -1 : 1) : (b.lower ? 1 : -1);
    }
    const int value = compare(a, b);
    if (value) return value;
    if (!a.inclusive && !b.inclusive) return a.lower == b.lower ? 0 : a.lower ? 1 : -1;
    if (!a.inclusive) return a.lower ? 1 : -1;
    if (!b.inclusive) return b.lower ? -1 : 1;
    return 0;
}

inline std::string statisticsFloat8(double value) {
    if (std::isnan(value)) return "NaN";
    if (std::isinf(value)) return std::signbit(value) ? "-Infinity" : "Infinity";
    std::ostringstream text; text.imbue(std::locale::classic());
    text << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
    return text.str();
}

// The input owner supplies actual subtype comparison/subdiff and constructs
// genuine range values. A multirange owner must supply its enclosing range;
// this function cannot invent a missing multirange catalog or storage owner.
inline std::optional<Column> collectRangeStatistics(Column column,
    const std::vector<RangeSample>& sample, DatumTypePtr rangeType,
    DatumTypePtr lengthType, const RangeValueComparison& compare,
    const RangeDistance& distance, const RangeConstruction& construct,
    const std::function<void()>& interrupt = {}) {
    if (!column.attributeType || !column.attributeType->actualOid || !column.attributeType->codecOid ||
        !rangeType || !rangeType->actualOid || !rangeType->codecOid || rangeType->kind != DatumKind::Scalar ||
        !lengthType || lengthType->actualOid != 701 || lengthType->kind != DatumKind::Scalar ||
        lengthType->codecOid != 701 || !compare || !construct ||
        column.target < 0 || column.target > 10000 ||
        !std::isfinite(column.totalRows) || column.totalRows < sample.size())
        throw DbError("XX001", "invalid range statistics analysis declaration or sample");
    if (!column.target || sample.empty()) return std::nullopt;
    column.slots.clear(); column.sampleRows = sample.size(); column.nullRows = 0;
    uint64_t width = 0, nonnull = 0, empty = 0;
    std::vector<size_t> lowers, uppers;
    std::vector<double> lengths;
    std::vector<uint64_t> ordinals;
    lowers.reserve(sample.size()); uppers.reserve(sample.size());
    lengths.reserve(sample.size()); ordinals.reserve(sample.size());
    for (size_t i = 0; i < sample.size(); ++i) {
        if (interrupt && (i & 1023U) == 0) interrupt();
        const auto& row = sample[i]; ordinals.push_back(row.ordinal);
        if (row.isNull) { ++column.nullRows; continue; }
        if (row.storedWidth > INT32_MAX || width > UINT64_MAX - row.storedWidth)
            throw DbError("XX001", "range statistics width exceeds its limit");
        width += row.storedWidth; ++nonnull;
        if (row.empty) { ++empty; continue; }
        if (!row.lower.lower || row.upper.lower ||
            (row.lower.unbounded && row.lower.inclusive) ||
            (row.upper.unbounded && row.upper.inclusive) ||
            compareRangeBounds(row.lower, row.upper, compare) > 0)
            throw DbError("XX001", "invalid nonempty analyzed range bounds");
        lowers.push_back(i); uppers.push_back(i);
        const double length = row.lower.unbounded || row.upper.unbounded
            ? std::numeric_limits<double>::infinity()
            : distance ? distance(row.upper, row.lower) : 1.0;
        if (length < 0) throw DbError("XX001", "negative analyzed range length");
        lengths.push_back(length);
    }
    std::sort(ordinals.begin(), ordinals.end());
    if (std::adjacent_find(ordinals.begin(), ordinals.end()) != ordinals.end())
        throw DbError("XX001", "duplicate range statistics sample ordinal");
    column.nullFraction = double(column.nullRows) / sample.size();
    column.averageWidth = nonnull ? width / nonnull : 0;
    column.distinctEstimate = nonnull ? -(1.0 - column.nullFraction) : 0;
    if (!nonnull) return column;

    size_t comparisons = 0;
    const auto checkSort = [&] { if (interrupt && (++comparisons & 1023U) == 0) interrupt(); };
    const auto boundSort = [&](const std::vector<size_t>& rows, bool lower) {
        auto sorted = rows;
        std::sort(sorted.begin(), sorted.end(), [&](size_t a, size_t b) {
            checkSort();
            const auto& x = lower ? sample[a].lower : sample[a].upper;
            const auto& y = lower ? sample[b].lower : sample[b].upper;
            const int result = compareRangeBounds(x, y, compare);
            return result ? result < 0 : sample[a].ordinal < sample[b].ordinal;
        });
        return sorted;
    };
    Slot length; length.kind = Kind::RangeLength;
    length.comparisonOperator = 672; length.collation = 0;
    length.numbers = std::vector<float>{float(double(empty) / nonnull)};
    if (lowers.size() >= 2) {
        lowers = boundSort(lowers, true); uppers = boundSort(uppers, false);
        // A strict total ordering keeps NaN lengths safe for C++ sorting.
        // Actual PG reference results, including special values, are checked
        // separately; subtype comparisons are never replaced by this order.
        std::sort(lengths.begin(), lengths.end(), [&](double a, double b) {
            checkSort();
            return std::isnan(b) ? !std::isnan(a) : !std::isnan(a) && a < b;
        });
        const size_t count = lowers.size() > size_t(column.target)
            ? size_t(column.target) + 1 : lowers.size();
        Slot bounds; bounds.kind = Kind::RangeBounds;
        bounds.values = Values{std::move(rangeType), {}};
        length.values = Values{std::move(lengthType), {}};
        const size_t whole = (lowers.size() - 1) / (count - 1);
        const size_t fraction = (lowers.size() - 1) % (count - 1);
        size_t index = 0, remainder = 0;
        for (size_t i = 0; i < count; ++i) {
            if (interrupt && (i & 1023U) == 0) interrupt();
            auto range = construct(sample[lowers[index]].lower, sample[uppers[index]].upper);
            if (range.isNull) throw DbError("XX001", "analyzed range construction returned NULL");
            bounds.values->cells.push_back(std::move(range));
            length.values->cells.emplace_back("double precision", statisticsFloat8(lengths[index]), false);
            index += whole; remainder += fraction;
            if (remainder >= count - 1) { ++index; remainder -= count - 1; }
        }
        column.slots.push_back(std::move(bounds));
    }
    // PG's actual pg_statistic publisher leaves stavalues SQL NULL when the
    // length histogram has zero entries. Do not turn that into present-empty.
    column.slots.push_back(std::move(length));
    return column;
}
} // namespace dbms::analyzed_statistics
