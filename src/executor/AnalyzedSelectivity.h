#pragma once

#include "catalog/AnalyzedStatistics.h"
#include <algorithm>
#include <cmath>
#include <limits>

namespace dbms::planner_statistics {
namespace typed_detail {
inline const analyzed_statistics::Slot* slot(const analyzed_statistics::Column& column,
                                            analyzed_statistics::Kind kind) {
    for (const auto& value : column.slots) if (value.kind == kind) return &value;
    return nullptr;
}
inline bool declaration(const analyzed_statistics::Column& column,
                        const analyzed_statistics::Slot& slot) {
    return slot.values && slot.values->elementType && column.attributeType &&
        slot.values->elementType->actualOid == column.attributeType->actualOid &&
        slot.values->elementType->codecOid == column.attributeType->codecOid &&
        slot.values->elementType->kind == DatumKind::Scalar && slot.collation == column.collation;
}
struct Distribution {
    const analyzed_statistics::Slot* common = nullptr;
    double nonnull = 0, commonMass = 0, smallest = 1;
};
inline std::optional<Distribution> distribution(const analyzed_statistics::Column& column) {
    if (!column.attributeType || column.attributeType->kind != DatumKind::Scalar ||
        !std::isfinite(column.totalRows) || column.totalRows < column.sampleRows ||
        column.nullRows > column.sampleRows || !std::isfinite(column.nullFraction) ||
        column.nullFraction < 0 || column.nullFraction > 1 ||
        !std::isfinite(column.distinctEstimate) || column.distinctEstimate < -1 ||
        (column.totalRows > 0 && !column.sampleRows)) return {};
    if (column.sampleRows && std::abs(double(column.nullRows) / column.sampleRows - column.nullFraction) > 1e-6)
        return {};
    Distribution result; result.nonnull = 1.0 - column.nullFraction;
    result.common = slot(column, analyzed_statistics::Kind::MostCommon);
    if (result.common) {
        const auto& common = *result.common;
        if (!declaration(column, common) || !common.comparisonOperator || !common.numbers ||
            common.numbers->size() != common.values->cells.size()) return {};
        for (size_t i = 0; i < common.numbers->size(); ++i) {
            const double frequency = (*common.numbers)[i];
            if (!std::isfinite(frequency) || frequency < 0 || frequency > 1 ||
                common.values->cells[i].isNull || common.values->cells[i].compound) return {};
            result.commonMass += frequency;
            result.smallest = std::min(result.smallest, frequency);
        }
        // MCV frequencies are binary32. Permit their rounding, never a
        // material probability excess or an invented negative residual mass.
        if (result.commonMass > result.nonnull + 1e-6) return {};
    }
    return result;
}
} // namespace typed_detail

// Operator/declaration applicability is checked by the engine's owned read
// API. Equal invokes the real column semantics, not a pg_stats display parser.
template<class Equal>
std::optional<double> analyzedEqualityFraction(const analyzed_statistics::Column& column,
                                               double populationRows, Equal equal) {
    const auto distribution = typed_detail::distribution(column);
    if (!distribution || !std::isfinite(populationRows) || populationRows < 0) return {};
    if (!column.totalRows || !distribution->nonnull) return 0;
    double matched = 0; bool hit = false;
    if (distribution->common) {
        const auto& common = *distribution->common;
        for (size_t i = 0; i < common.values->cells.size(); ++i) {
            const auto matches = equal(common.values->cells[i]);
            if (!matches) return {};
            if (*matches) { hit = true; matched += (*common.numbers)[i]; }
        }
    }
    if (hit) return std::clamp(matched, 0.0, distribution->nonnull);
    double distinct = column.distinctEstimate;
    if (distinct < 0) distinct *= -populationRows;
    if (!(distinct > 0)) return {};
    distinct = std::floor(distinct + 0.5);
    const size_t commonCount = distribution->common ? distribution->common->values->cells.size() : 0;
    double residual = std::max(0.0, distribution->nonnull - distribution->commonMass);
    if (distinct > commonCount + 1.0) residual /= distinct - commonCount;
    if (commonCount) residual = std::min(residual, distribution->smallest);
    return std::clamp(residual, 0.0, distribution->nonnull);
}

// A conservative density guard for binary text prefixes. A histogram describes
// only the non-MCV population: add its fully covered bins to matching MCV mass.
// Straddling bins remain unknown. This is not full PostgreSQL patternsel.
inline std::optional<double> analyzedPrefixDensity(const analyzed_statistics::Column& column,
                                                   const std::string& prefix) {
    const auto distribution = typed_detail::distribution(column);
    if (!distribution || prefix.empty()) return {};
    const auto starts = [&](const ExprValue& value) {
        return !value.isNull && !value.compound && value.value.compare(0, prefix.size(), prefix) == 0;
    };
    double result = 0;
    if (distribution->common) {
        const auto& common = *distribution->common;
        for (size_t i = 0; i < common.values->cells.size(); ++i)
            if (starts(common.values->cells[i])) result += (*common.numbers)[i];
    }
    const auto* histogram = typed_detail::slot(column, analyzed_statistics::Kind::Histogram);
    if (histogram) {
        if (!typed_detail::declaration(column, *histogram) || !histogram->comparisonOperator ||
            histogram->values->cells.size() < 2) return {};
        const auto& bounds = histogram->values->cells;
        size_t covered = 0;
        for (size_t i = 0; i < bounds.size(); ++i) {
            if (bounds[i].isNull || bounds[i].compound) return {};
            if (i && starts(bounds[i - 1]) && starts(bounds[i])) ++covered;
        }
        result += std::max(0.0, distribution->nonnull - distribution->commonMass) *
            covered / (bounds.size() - 1);
    }
    return std::clamp(result, 0.0, distribution->nonnull);
}
} // namespace dbms::planner_statistics
