#pragma once

#include "catalog/AnalyzedStatistics.h"
#include "common/DbError.h"
#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <numeric>

namespace dbms::analyzed_statistics {

// Inputs come from one real physical scan. Width is the stored datum's byte
// span, not its display length; wide values still contribute to width/NULL
// statistics but need not retain their payload. The scan ordinal survives
// reservoir replacement and is unrelated to the sample vector's position.
struct ScalarSample {
    uint64_t ordinal = 0;
    ExprValue value;
    uint32_t storedWidth = 0;
    bool tooWide = false;
};

// The caller supplies resolved equality/ordering roles and a comparator for
// the actual declaration/collation. No lexical fallback for unknown types.
// The formulas and slot applicability follow PostgreSQL REL_18_6 analyze.c;
// storage widths and the sampling owner belong to this engine.
inline std::optional<Column> collectScalarStatistics(
    Column column, const std::vector<ScalarSample>& sample,
    uint32_t equalityOperator, uint32_t orderingOperator,
    const std::function<int(const ExprValue&, const ExprValue&)>& compare,
    const std::function<void()>& interrupt = {},
    const std::function<int(size_t, size_t)>& indexedCompare = {}) {
    if (!column.attributeType || !column.attributeType->actualOid ||
        column.target < 0 || column.target > 10000 || column.averageWidth < 0 ||
        !std::isfinite(column.totalRows) || column.totalRows < sample.size())
        throw DbError("XX001", "invalid scalar statistics analysis input");
    if (!column.target || sample.empty()) return std::nullopt;
    if (!equalityOperator || !orderingOperator || (!compare && !indexedCompare))
        throw DbError("XX001", "scalar statistics comparison role is absent");
    const auto compareSamples = [&](size_t a, size_t b) {
        return indexedCompare ? indexedCompare(a, b) : compare(sample[a].value, sample[b].value);
    };
    column.sampleRows = sample.size();
    column.nullRows = 0;
    column.slots.clear();
    uint64_t totalWidth = 0;
    size_t nonnull = 0, wide = 0;
    std::vector<size_t> values;
    values.reserve(sample.size());
    for (size_t i = 0; i < sample.size(); ++i) {
        if (interrupt && (i & 1023U) == 0) interrupt();
        const auto& cell = sample[i];
        if (cell.value.isNull) { ++column.nullRows; continue; }
        if (cell.storedWidth > INT32_MAX)
            throw DbError("XX001", "scalar statistics datum width exceeds int32");
        ++nonnull;
        totalWidth += cell.storedWidth;
        if (cell.tooWide) ++wide;
        else values.push_back(i);
    }
    column.nullFraction = double(column.nullRows) / sample.size();
    if (nonnull) column.averageWidth = static_cast<int32_t>(totalWidth / nonnull);
    column.distinctEstimate = 0;
    if (values.empty()) {
        if (nonnull) column.distinctEstimate = -(1.0 - column.nullFraction);
        return column;
    }

    // Physical rank includes only sortable non-NULL, non-wide sample values,
    // just as PG's values_cnt. A tie uses the original scan rank.
    std::sort(values.begin(), values.end(), [&](size_t a, size_t b) {
        return sample[a].ordinal < sample[b].ordinal;
    });
    std::vector<size_t> rank(sample.size());
    for (size_t i = 0; i < values.size(); ++i) {
        if (i && sample[values[i - 1]].ordinal == sample[values[i]].ordinal)
            throw DbError("XX001", "duplicate scalar statistics sample ordinal");
        rank[values[i]] = i;
    }
    size_t comparisons = 0;
    std::sort(values.begin(), values.end(), [&](size_t a, size_t b) {
        if (interrupt && (++comparisons & 1023U) == 0) interrupt();
        const int result = compareSamples(a, b);
        return result ? result < 0 : rank[a] < rank[b];
    });
    struct Group { size_t first, count; };
    std::vector<Group> groups;
    double xy = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        if (interrupt && (i & 1023U) == 0) interrupt();
        xy += double(i) * rank[values[i]];
        if (i && compareSamples(values[i - 1], values[i]) == 0)
            ++groups.back().count;
        else groups.push_back({i, 1});
    }
    const size_t repeated = std::count_if(groups.begin(), groups.end(),
        [](const Group& g) { return g.count > 1; });
    if (!repeated) column.distinctEstimate = -(1.0 - column.nullFraction);
    else if (!wide && repeated == groups.size()) column.distinctEstimate = groups.size();
    else {
        const double singleton = groups.size() - repeated + wide;
        const double distinct = singleton + repeated;
        const double population = column.totalRows * (1.0 - column.nullFraction);
        double estimate = nonnull * distinct /
            ((nonnull - singleton) + singleton * nonnull / population);
        estimate = std::min(population, std::max(distinct, estimate));
        column.distinctEstimate = std::floor(estimate + 0.5);
    }
    if (column.distinctEstimate > 0.1 * column.totalRows)
        column.distinctEstimate = -(column.distinctEstimate / column.totalRows);

    std::vector<size_t> common;
    for (size_t i = 0; i < groups.size(); ++i)
        if (groups[i].count > 1) common.push_back(i);
    std::stable_sort(common.begin(), common.end(), [&](size_t a, size_t b) {
        return groups[a].count > groups[b].count;
    });
    if (common.size() > static_cast<size_t>(column.target)) common.resize(column.target);
    // The full population needs no significance filter. For a sample, retain
    // a complete finite-domain list, or remove statistically weak tail items.
    const bool complete = common.size() == groups.size() && !wide &&
        column.distinctEstimate > 0;
    if (!complete && column.sampleRows != column.totalRows && column.totalRows > 1) {
        double populationDistinct = column.distinctEstimate;
        if (populationDistinct < 0) populationDistinct *= -column.totalRows;
        double preceding = 0;
        for (size_t i = 0; i + 1 < common.size(); ++i) preceding += groups[common[i]].count;
        while (!common.empty()) {
            double selectivity = std::clamp(1.0 - preceding / column.sampleRows -
                column.nullFraction, 0.0, 1.0);
            const double remaining = populationDistinct - (common.size() - 1);
            if (remaining > 1) selectivity /= remaining;
            const double count = groups[common.back()].count;
            const double population = column.totalRows, n = column.sampleRows;
            const double success = population * count / n;
            const double variance = n * success * (population - success) *
                (population - n) / (population * population * (population - 1));
            if (count > selectivity * n + 2 * std::sqrt(variance) + 0.5) break;
            common.pop_back();
            if (!common.empty()) preceding -= groups[common.back()].count;
        }
    }
    std::vector<bool> isCommon(groups.size(), false);
    if (!common.empty()) {
        Slot mcv; mcv.kind = Kind::MostCommon;
        mcv.comparisonOperator = equalityOperator; mcv.collation = column.collation;
        mcv.numbers.emplace(); mcv.values = Values{column.attributeType, {}};
        for (size_t g : common) {
            isCommon[g] = true;
            mcv.numbers->push_back(double(groups[g].count) / column.sampleRows);
            mcv.values->cells.push_back(sample[values[groups[g].first]].value);
        }
        column.slots.push_back(std::move(mcv));
    }
    size_t bounds = groups.size() - common.size();
    if (bounds > static_cast<size_t>(column.target)) bounds = column.target + 1;
    if (bounds >= 2) {
        std::vector<size_t> residual;
        for (size_t g = 0; g < groups.size(); ++g) if (!isCommon[g])
            for (size_t i = groups[g].first; i < groups[g].first + groups[g].count; ++i)
                residual.push_back(values[i]);
        Slot histogram; histogram.kind = Kind::Histogram;
        histogram.comparisonOperator = orderingOperator; histogram.collation = column.collation;
        histogram.values = Values{column.attributeType, {}};
        const size_t whole = (residual.size() - 1) / (bounds - 1);
        const size_t fraction = (residual.size() - 1) % (bounds - 1);
        size_t position = 0, remainder = 0;
        for (size_t i = 0; i < bounds; ++i) {
            histogram.values->cells.push_back(sample[residual[position]].value);
            position += whole; remainder += fraction;
            if (remainder >= bounds - 1) { ++position; remainder -= bounds - 1; }
        }
        column.slots.push_back(std::move(histogram));
    }
    if (values.size() > 1) {
        const double n = values.size(), sum = (n - 1) * n / 2;
        const double squares = (n - 1) * n * (2 * n - 1) / 6;
        Slot correlation; correlation.kind = Kind::Correlation;
        correlation.comparisonOperator = orderingOperator; correlation.collation = column.collation;
        correlation.numbers = std::vector<float>{float((n * xy - sum * sum) /
            (n * squares - sum * sum))};
        column.slots.push_back(std::move(correlation));
    }
    return column;
}
} // namespace dbms::analyzed_statistics
