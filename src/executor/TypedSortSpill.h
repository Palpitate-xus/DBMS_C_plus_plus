#pragma once

#include "commands/TableManage.h"
#include "executor/SortSpill.h"
#include "expression/ExprEvaluator.h"

#include <limits>
#include <cstring>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace dbms {

// This observer belongs only to the private temporary-sort codec. Restoring a
// snapshot constructs a new value container; it never rebinds an execution,
// provider, source, parameter owner or cursor.
struct RowContextSpillAccess {
    using SqlValues = std::map<FunctionCallExpr::SqlValue, ExprValue>;
    using Values = std::map<std::string, ExprValue>;
    using Parameters = std::vector<ExprValue>;
    using BoundColumns = std::map<std::pair<size_t, size_t>, ExprValue>;
    static const SqlValues& sqlValues(const RowContext& row) { return row.sqlValues_; }
    static const Values& values(const RowContext& row) { return row.values_; }
    static const Parameters& parameters(const RowContext& row) { return row.parameters_; }
    static const BoundColumns& boundColumns(const RowContext& row) { return row.boundColumns_; }
    static RowContext restore(SqlValues sql, Values values, Parameters parameters, BoundColumns bound) {
        RowContext result;
        result.sqlValues_ = std::move(sql); result.values_ = std::move(values);
        result.parameters_ = std::move(parameters); result.boundColumns_ = std::move(bound);
        return result;
    }
};

namespace typed_sort_detail {

// Process-private framing: these headers contain lengths/flags, never live
// pointers or an on-disk/Wire datum representation. Zero initialization also
// makes their unused bytes independent of a caller's stack contents.
struct ValueHeader {
    uint64_t type = 0, value = 0, collation = 0, elements = 0, fields = 0;
    uint32_t oid = 0;
    uint8_t isNull = 0, hasOid = 0, hasFields = 0;
};
struct ElementHeader { uint32_t oid = 0; uint8_t present = 0; };
struct ContextHeader { uint64_t sql = 0, values = 0, parameters = 0, bound = 0; };

inline size_t addBytes(size_t a, size_t b) {
    return b > std::numeric_limits<size_t>::max() - a ? std::numeric_limits<size_t>::max() : a + b;
}
inline size_t multiplyBytes(size_t a, size_t b) {
    return a && b > std::numeric_limits<size_t>::max() / a ? std::numeric_limits<size_t>::max() : a * b;
}
[[noreturn]] inline void invalid(const char* detail) {
    throw DbError("58030", std::string("invalid typed sort temporary ") + detail);
}

template <typename Visitor>
inline void visitValues(const ExprValue& root, Visitor&& visit) {
    struct Frame { const std::vector<ExprValue>* fields; size_t next; };
    std::vector<Frame> stack;
    const ExprValue* current = &root;
    for (;;) {
        visit(*current);
        if (current->recordFields && !current->recordFields->empty()) {
            const auto* fields = &*current->recordFields;
            stack.push_back({fields, 1}); current = &fields->front();
            continue;
        }
        while (!stack.empty() && stack.back().next == stack.back().fields->size()) stack.pop_back();
        if (stack.empty()) return;
        auto& frame = stack.back(); current = &frame.fields->at(frame.next++);
    }
}

inline size_t valueBytes(const ExprValue& value) {
    size_t bytes = sizeof(ExprValue);
    visitValues(value, [&](const ExprValue& cell) {
        // Count string capacities conservatively, including a terminator.
        for (const auto* text : {&cell.typeName, &cell.value, &cell.collation})
            bytes = addBytes(bytes, addBytes(text->capacity(), 1));
        bytes = addBytes(bytes, multiplyBytes(cell.elementOids.capacity(), sizeof(std::optional<uint32_t>)));
        if (cell.recordFields)
            bytes = addBytes(bytes, multiplyBytes(cell.recordFields->capacity(), sizeof(ExprValue)));
    });
    return bytes;
}
inline size_t dynamicValueBytes(const ExprValue& value) {
    const auto bytes = valueBytes(value);
    return bytes == std::numeric_limits<size_t>::max() ? bytes : bytes - sizeof(ExprValue);
}
template <typename Map>
inline size_t mapBytes(const Map& values) {
    // The value pair owns its datum; include parent/left/right/color/padding
    // space for each ordinary tree node without depending on libstdc++ ABI.
    return multiplyBytes(values.size(), addBytes(sizeof(typename Map::value_type), 4 * sizeof(void*)));
}
inline size_t contextBytes(const RowContext& row) {
    const auto& sql = RowContextSpillAccess::sqlValues(row);
    const auto& values = RowContextSpillAccess::values(row);
    const auto& parameters = RowContextSpillAccess::parameters(row);
    const auto& bound = RowContextSpillAccess::boundColumns(row);
    size_t bytes = addBytes(sizeof(RowContext), addBytes(mapBytes(sql), addBytes(mapBytes(values), mapBytes(bound))));
    bytes = addBytes(bytes, multiplyBytes(parameters.capacity(), sizeof(ExprValue)));
    for (const auto& item : sql) bytes = addBytes(bytes, dynamicValueBytes(item.second));
    for (const auto& item : values) {
        bytes = addBytes(bytes, addBytes(item.first.capacity(), 1));
        bytes = addBytes(bytes, dynamicValueBytes(item.second));
    }
    for (const auto& cell : parameters) bytes = addBytes(bytes, dynamicValueBytes(cell));
    for (const auto& item : bound) bytes = addBytes(bytes, dynamicValueBytes(item.second));
    return bytes;
}

inline void writeValue(sort_detail::Writer& writer, const ExprValue& value) {
    visitValues(value, [&](const ExprValue& cell) {
        ValueHeader header{};
        std::memset(static_cast<void*>(&header), 0, sizeof(header));
        header.type = cell.typeName.size(); header.value = cell.value.size(); header.collation = cell.collation.size();
        header.elements = cell.elementOids.size(); header.fields = cell.recordFields ? cell.recordFields->size() : 0;
        header.isNull = cell.isNull; header.hasOid = cell.objectOid.has_value();
        header.oid = cell.objectOid.value_or(0); header.hasFields = cell.recordFields.has_value();
        writer.scalar(header);
        writer.append(cell.typeName.data(), cell.typeName.size());
        writer.append(cell.value.data(), cell.value.size());
        writer.append(cell.collation.data(), cell.collation.size());
        for (const auto& oid : cell.elementOids) {
            ElementHeader element{}; std::memset(static_cast<void*>(&element), 0, sizeof(element));
            element.oid = oid.value_or(0); element.present = oid.has_value();
            writer.scalar(element);
        }
    });
}

inline void requireCount(uint64_t count, uint64_t minimum, uint64_t maximum, uint64_t& remaining) {
    if (count > maximum || (minimum && count > remaining / minimum)) invalid("count");
    remaining -= count * minimum;
}
inline std::string readString(sort_detail::Reader& reader, uint64_t length) {
    if (length > reader.remaining() || length > std::string{}.max_size()) invalid("string extent");
    return reader.string(static_cast<size_t>(length));
}
struct ValueNode { ExprValue cell; uint64_t fields; };
inline ValueNode readNode(sort_detail::Reader& reader) {
    const auto header = reader.scalar<ValueHeader>();
    if (header.isNull > 1 || header.hasOid > 1 || header.hasFields > 1 ||
        (!header.hasOid && header.oid) || (!header.hasFields && header.fields)) invalid("value flags");
    uint64_t remaining = reader.remaining();
    for (const auto length : {header.type, header.value, header.collation})
        requireCount(length, 1, std::string{}.max_size(), remaining);
    requireCount(header.elements, sizeof(ElementHeader), std::vector<std::optional<uint32_t>>{}.max_size(), remaining);
    requireCount(header.fields, sizeof(ValueHeader), std::vector<ExprValue>{}.max_size(), remaining);
    ExprValue value;
    value.typeName = readString(reader, header.type); value.value = readString(reader, header.value);
    value.collation = readString(reader, header.collation); value.isNull = header.isNull != 0;
    if (header.hasOid) value.objectOid = header.oid;
    // Counts, max_size and the complete encoded minimum extent were checked
    // above. Reserve that validated count so geometric push_back growth cannot
    // inflate decoded merge-head memory beyond the original owned row.
    value.elementOids.reserve(static_cast<size_t>(header.elements));
    for (uint64_t i = 0; i < header.elements; ++i) {
        const auto element = reader.scalar<ElementHeader>();
        if (element.present > 1 || (!element.present && element.oid)) invalid("element identity flag");
        value.elementOids.push_back(element.present ? std::optional<uint32_t>(element.oid) : std::nullopt);
    }
    if (header.hasFields) {
        value.recordFields.emplace(); value.recordFields->reserve(static_cast<size_t>(header.fields));
    }
    return {std::move(value), header.fields};
}
inline ExprValue readValue(sort_detail::Reader& reader) {
    auto node = readNode(reader);
    ExprValue value = std::move(node.cell);
    struct Frame { std::vector<ExprValue>* fields; uint64_t left; };
    std::vector<Frame> stack;
    if (node.fields) stack.push_back({&*value.recordFields, node.fields});
    while (!stack.empty()) {
        auto& frame = stack.back();
        auto next = readNode(reader);
        frame.fields->push_back(std::move(next.cell)); --frame.left;
        auto* child = &frame.fields->back();
        if (next.fields) stack.push_back({&*child->recordFields, next.fields});
        else while (!stack.empty() && !stack.back().left) stack.pop_back();
    }
    return value;
}

inline void writeContext(sort_detail::Writer& writer, const RowContext& row) {
    const auto& sql = RowContextSpillAccess::sqlValues(row);
    const auto& values = RowContextSpillAccess::values(row);
    const auto& parameters = RowContextSpillAccess::parameters(row);
    const auto& bound = RowContextSpillAccess::boundColumns(row);
    writer.scalar(ContextHeader{sql.size(), values.size(), parameters.size(), bound.size()});
    for (const auto& item : sql) { writer.scalar(static_cast<uint32_t>(item.first)); writeValue(writer, item.second); }
    for (const auto& item : values) {
        writer.scalar(uint64_t(item.first.size())); writer.append(item.first.data(), item.first.size());
        writeValue(writer, item.second);
    }
    for (const auto& cell : parameters) writeValue(writer, cell);
    for (const auto& item : bound) {
        writer.scalar(uint64_t(item.first.first)); writer.scalar(uint64_t(item.first.second));
        writeValue(writer, item.second);
    }
}
inline RowContext readContext(sort_detail::Reader& reader) {
    const auto header = reader.scalar<ContextHeader>();
    RowContextSpillAccess::SqlValues sql;
    RowContextSpillAccess::Values values;
    RowContextSpillAccess::Parameters parameters;
    RowContextSpillAccess::BoundColumns bound;
    uint64_t remaining = reader.remaining();
    requireCount(header.sql, sizeof(uint32_t) + sizeof(ValueHeader), sql.max_size(), remaining);
    requireCount(header.values, sizeof(uint64_t) + sizeof(ValueHeader), values.max_size(), remaining);
    requireCount(header.parameters, sizeof(ValueHeader), parameters.max_size(), remaining);
    requireCount(header.bound, 2 * sizeof(uint64_t) + sizeof(ValueHeader), bound.max_size(), remaining);
    for (uint64_t i = 0; i < header.sql; ++i) {
        const auto key = reader.scalar<uint32_t>();
        if (key > static_cast<uint32_t>(FunctionCallExpr::SqlValue::LocalTimestamp)) invalid("SQL value kind");
        auto cell = readValue(reader);
        if (!sql.emplace(static_cast<FunctionCallExpr::SqlValue>(key), std::move(cell)).second) invalid("duplicate SQL value");
    }
    for (uint64_t i = 0; i < header.values; ++i) {
        const auto size = reader.scalar<uint64_t>();
        if (reader.remaining() < sizeof(ValueHeader) || size > reader.remaining() - sizeof(ValueHeader)) invalid("value name extent");
        auto key = readString(reader, size); auto cell = readValue(reader);
        if (!values.emplace(std::move(key), std::move(cell)).second) invalid("duplicate value name");
    }
    parameters.reserve(static_cast<size_t>(header.parameters));
    for (uint64_t i = 0; i < header.parameters; ++i) parameters.push_back(readValue(reader));
    for (uint64_t i = 0; i < header.bound; ++i) {
        const auto source = reader.scalar<uint64_t>(), column = reader.scalar<uint64_t>();
        if (source > std::numeric_limits<size_t>::max() || column > std::numeric_limits<size_t>::max()) invalid("bound column ordinal");
        auto cell = readValue(reader);
        if (!bound.emplace(std::make_pair(static_cast<size_t>(source), static_cast<size_t>(column)), std::move(cell)).second)
            invalid("duplicate bound column");
    }
    return RowContextSpillAccess::restore(std::move(sql), std::move(values), std::move(parameters), std::move(bound));
}

} // namespace typed_sort_detail
} // namespace dbms
