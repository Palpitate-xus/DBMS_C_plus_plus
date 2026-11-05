#pragma once

#include "catalog/AnalyzedScalarStatistics.h"
#include <map>

namespace dbms::analyzed_statistics {

struct ArraySample {
    ScalarSample scalar;
    // Logical size of the actual local stored input, including its header.
    // A TOAST reference's physical width is separate (scalar.storedWidth).
    uint64_t logicalBytes = 0;
};

// ANALYZE owns these arrays and their real element declarations. A rendered
// array is never parsed here; only the physical-input owner may decode one.
inline const CompoundDatum& analyzedArray(const ExprValue& value,
                                         const DatumTypePtr& declaration) {
    const auto& data = value.compound;
    if (!declaration || declaration->kind != DatumKind::Array ||
        !declaration->arrayElement || !data || !data->type ||
        data->type->kind != DatumKind::Array || data->type->actualOid != declaration->actualOid ||
        data->type->codecOid != declaration->codecOid ||
        !data->arrayElements || data->dimensions.size() > 6)
        throw DbError("XX001", "analyzed array lacks its owning declaration or elements");
    size_t count = data->dimensions.empty() ? 0 : 1;
    for (const auto& d : data->dimensions) {
        if (d.length <= 0 || int64_t(d.lower) + d.length - 1 > INT32_MAX ||
            count > size_t(INT32_MAX) / size_t(d.length))
            throw DbError("XX001", "invalid analyzed array dimensions");
        count *= d.length;
    }
    if (count != data->arrayElements->size() || !data->type->arrayElement ||
        data->type->arrayElement->actualOid != declaration->arrayElement->actualOid ||
        data->type->arrayElement->codecOid != declaration->arrayElement->codecOid)
        throw DbError("XX001", "analyzed array element identity or cardinality differs");
    if (data->arrayNullBitmap && !*data->arrayNullBitmap &&
        std::any_of(data->arrayElements->begin(), data->arrayElements->end(),
                    [](const ExprValue& v) { return v.isNull; }))
        throw DbError("XX001", "analyzed array NULL element has no bitmap");
    return *data;
}

inline int compareAnalyzedArrays(const ExprValue& a, const ExprValue& b,
    const DatumTypePtr& declaration,
    const std::function<int(const ExprValue&, const ExprValue&)>& elementCompare) {
    const auto& left = analyzedArray(a, declaration);
    const auto& right = analyzedArray(b, declaration);
    const auto& x = *left.arrayElements;
    const auto& y = *right.arrayElements;
    for (size_t i = 0; i < std::min(x.size(), y.size()); ++i) {
        if (x[i].isNull || y[i].isNull) {
            if (x[i].isNull != y[i].isNull) return x[i].isNull ? 1 : -1;
        } else if (const int c = elementCompare(x[i], y[i])) return c;
    }
    if (x.size() != y.size()) return x.size() < y.size() ? -1 : 1;
    if (left.dimensions.size() != right.dimensions.size())
        return left.dimensions.size() < right.dimensions.size() ? -1 : 1;
    for (size_t i = 0; i < left.dimensions.size(); ++i)
        if (left.dimensions[i].length != right.dimensions[i].length)
            return left.dimensions[i].length < right.dimensions[i].length ? -1 : 1;
    for (size_t i = 0; i < left.dimensions.size(); ++i)
        if (left.dimensions[i].lower != right.dimensions[i].lower)
            return left.dimensions[i].lower < right.dimensions[i].lower ? -1 : 1;
    return 0;
}

// PG REL_18_6 array_typanalyze semantics: ordinary whole-array slots plus
// Lossy Counting of row membership and distinct-element-count quantiles.
// The caller resolves actual comparison roles/collation; no raw-text hash or
// lexical ordering substitutes for the element's equality semantics.
inline std::optional<Column> collectArrayStatistics(Column column,
    const std::vector<ArraySample>& sample, uint32_t arrayEquality,
    uint32_t arrayOrdering, uint32_t elementEquality,
    const std::function<int(const ExprValue&, const ExprValue&)>& elementCompare,
    const std::function<void()>& interrupt = {}) {
    if (!column.attributeType || column.attributeType->kind != DatumKind::Array ||
        !column.attributeType->arrayElement || !column.attributeType->arrayElement->actualOid ||
        !column.attributeType->arrayElement->codecOid || !elementCompare || !elementEquality)
        throw DbError("XX001", "array statistics comparison or declaration is absent");
    std::vector<ScalarSample> scalar;
    std::vector<size_t> physical;
    scalar.reserve(sample.size()); physical.reserve(sample.size());
    for (size_t i = 0; i < sample.size(); ++i) {
        if (interrupt && (i & 1023U) == 0) interrupt();
        const auto& s = sample[i].scalar;
        if (!s.value.isNull && s.tooWide != (sample[i].logicalBytes > 1024))
            throw DbError("XX001", "array scalar width eligibility differs");
        ScalarSample base;
        base.ordinal = s.ordinal; base.storedWidth = s.storedWidth; base.tooWide = s.tooWide;
        base.value = s.tooWide ? ExprValue(s.value.typeName, {}, s.value.isNull) : s.value;
        scalar.push_back(std::move(base)); physical.push_back(i);
        if (!s.value.isNull && sample[i].logicalBytes <= 65536)
            (void)analyzedArray(s.value, column.attributeType);
    }
    const auto arrayCompare = [&](const ExprValue& a, const ExprValue& b) {
        return compareAnalyzedArrays(a, b, column.attributeType, elementCompare);
    };
    auto result = collectScalarStatistics(column, scalar, arrayEquality, arrayOrdering,
                                          arrayCompare, interrupt);
    if (!result) return result;
    std::sort(physical.begin(), physical.end(), [&](size_t a, size_t b) {
        return sample[a].scalar.ordinal < sample[b].scalar.ordinal;
    });
    for (size_t i = 1; i < physical.size(); ++i)
        if (sample[physical[i - 1]].scalar.ordinal == sample[physical[i]].scalar.ordinal)
            throw DbError("XX001", "duplicate array statistics sample ordinal");

    struct Count { uint64_t frequency, delta; size_t lastRow; };
    const auto less = [&](const ExprValue& a, const ExprValue& b) {
        return elementCompare(a, b) < 0;
    };
    std::map<ExprValue, Count, decltype(less)> tracked(less);
    std::map<uint32_t, uint64_t> counts;
    const uint64_t wanted = uint64_t(column.target) * 10;
    const uint64_t bucketWidth = wanted * 1000 / 7;
    uint64_t bucket = 1, elements = 0, analyzed = 0, nullElements = 0, work = 0;
    for (size_t row : physical) {
        if (interrupt) interrupt();
        const auto& input = sample[row];
        if (input.scalar.value.isNull || input.logicalBytes > 65536) continue;
        ++analyzed;
        const uint64_t start = elements;
        bool hasNull = false;
        for (const auto& value : *analyzedArray(input.scalar.value, column.attributeType).arrayElements) {
            if (interrupt && (++work & 1023U) == 0) interrupt();
            if (value.isNull) { hasNull = true; continue; }
            auto inserted = tracked.try_emplace(value, Count{1, bucket - 1, row});
            if (!inserted.second) {
                if (inserted.first->second.lastRow == row) continue;
                ++inserted.first->second.frequency;
                inserted.first->second.lastRow = row;
            }
            if (elements == UINT64_MAX / 9)
                throw DbError("54000", "array statistics element stream is too large");
            ++elements;
            if (elements % bucketWidth == 0) {
                for (auto it = tracked.begin(); it != tracked.end();) {
                    if (interrupt && (++work & 1023U) == 0) interrupt();
                    if (it->second.frequency + it->second.delta <= bucket) it = tracked.erase(it);
                    else ++it;
                }
                ++bucket;
            }
        }
        if (hasNull) ++nullElements;
        ++counts[static_cast<uint32_t>(elements - start)];
    }
    if (!analyzed) return result;
    using Entry = typename decltype(tracked)::value_type;
    std::vector<const Entry*> common;
    const uint64_t cutoff = elements * 9 / bucketWidth;
    for (const auto& entry : tracked)
        if (entry.second.frequency > cutoff) common.push_back(&entry);
    if (common.size() > wanted) {
        std::stable_sort(common.begin(), common.end(), [](const Entry* a, const Entry* b) {
            return a->second.frequency > b->second.frequency;
        });
        common.resize(wanted);
        std::sort(common.begin(), common.end(), [&](const Entry* a, const Entry* b) {
            return less(a->first, b->first);
        });
    }
    if (!common.empty()) {
        Slot slot; slot.kind = Kind::CommonElements;
        slot.comparisonOperator = elementEquality; slot.collation = column.collation;
        slot.values = Values{column.attributeType->arrayElement, {}}; slot.numbers.emplace();
        uint64_t minimum = elements, maximum = 0;
        for (const auto* item : common) {
            minimum = std::min(minimum, item->second.frequency);
            maximum = std::max(maximum, item->second.frequency);
            slot.values->cells.push_back(item->first);
            slot.numbers->push_back(double(item->second.frequency) / analyzed);
        }
        slot.numbers->push_back(double(minimum) / analyzed);
        slot.numbers->push_back(double(maximum) / analyzed);
        slot.numbers->push_back(double(nullElements) / analyzed);
        result->slots.push_back(std::move(slot));
    }
    Slot histogram; histogram.kind = Kind::ElementCounts;
    histogram.comparisonOperator = elementEquality; histogram.collation = column.collation;
    histogram.numbers.emplace();
    const size_t boundaries = std::max(column.target, 2);
    auto count = counts.begin(); uint64_t cumulative = count->second;
    for (size_t i = 0; i < boundaries; ++i) {
        const uint64_t position = (analyzed - 1) / (boundaries - 1) * i +
            ((analyzed - 1) % (boundaries - 1)) * i / (boundaries - 1);
        while (cumulative <= position) { ++count; cumulative += count->second; }
        histogram.numbers->push_back(count->first);
    }
    histogram.numbers->push_back(double(elements) / analyzed);
    result->slots.push_back(std::move(histogram));
    if (result->slots.size() > 5) throw DbError("XX001", "too many analyzed array statistics slots");
    return result;
}
} // namespace dbms::analyzed_statistics
