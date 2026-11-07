#pragma once
#include "catalog/systables.h"
#include "common/DbError.h"
#include "common/SqlArrayText.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <optional>
#include <sstream>
#include <vector>

namespace dbms::pg_object_constant {
struct Value {
    Oid type = INVALID_OID;
    std::optional<std::string> value;
};
inline bool array(Oid type) { return type == 2210 || type == 2211; }
inline Oid element(Oid type) { return type == 2205 || type == 2210 ? 2205 : 2206; }
inline std::string name(Oid type) { return element(type) == 2205 ? "regclass" : "regtype"; }
inline Oid oid(const std::string& text) {
    Oid value = 0;
    if (text.empty()) throw DbError("XX001", "empty bound object constant");
    for (char c : text) {
        if (c < '0' || c > '9' || value > (std::numeric_limits<Oid>::max() - (c - '0')) / 10)
            throw DbError("XX001", "invalid bound object constant");
        value = value * 10 + static_cast<Oid>(c - '0');
    }
    return value;
}
template<class T> inline void store(std::string& bytes, size_t offset, T value) {
    if (offset > bytes.size() || sizeof(T) > bytes.size() - offset)
        throw DbError("XX001", "invalid default constant encoding offset");
    std::memcpy(bytes.data() + offset, &value, sizeof(T));
}
inline std::string arrayDatum(const Value& constant) {
    const auto parsed = sql_array_text::parse(*constant.value);
    std::vector<std::optional<Oid>> cells;
    std::function<void(const sql_array_text::Literal&)> flatten = [&](const auto& part) {
        for (const auto& token : part.elements) {
            if (!token.empty() && token.front() == '{') flatten(sql_array_text::parse(token));
            else if (token == "NULL") cells.push_back(std::nullopt);
            else {
                std::string raw = token;
                if (raw.size() >= 2 && raw.front() == '"' && raw.back() == '"') raw = raw.substr(1, raw.size() - 2);
                cells.push_back(oid(raw));
            }
        }
    };
    flatten(parsed);
    const size_t dimensions = cells.empty() ? 0 : parsed.dimensions.size();
    if (dimensions > 6) throw DbError("54000", "number of array dimensions exceeds the maximum allowed");
    const bool nulls = std::any_of(cells.begin(), cells.end(), [](const auto& value) { return !value; });
    const size_t header = 16 + 8 * dimensions;
    const size_t alignment = alignof(double);
    const size_t offset = nulls ? (header + (cells.size() + 7) / 8 + alignment - 1) / alignment * alignment : header;
    const size_t count = std::count_if(cells.begin(), cells.end(), [](const auto& value) { return bool(value); });
    if (offset > 0x3fffffffU || count > (0x3fffffffU - offset) / sizeof(Oid))
        throw DbError("54000", "array default is too large");
    std::string bytes(offset + count * sizeof(Oid), '\0');
    const uint16_t endian = 1;
    const bool little = *reinterpret_cast<const unsigned char*>(&endian) == 1;
    store<uint32_t>(bytes, 0, static_cast<uint32_t>(bytes.size()) << (little ? 2 : 0));
    store<int32_t>(bytes, 4, static_cast<int32_t>(dimensions));
    store<int32_t>(bytes, 8, nulls ? static_cast<int32_t>(offset) : 0);
    store<Oid>(bytes, 12, element(constant.type));
    for (size_t i = 0; i < dimensions; ++i) {
        const auto& dimension = parsed.dimensions[i];
        if (dimension.length <= 0)
            throw DbError("2202E", "array default bounds are out of range");
        store<int32_t>(bytes, 16 + i * 4, static_cast<int32_t>(dimension.length));
        store<int32_t>(bytes, 16 + dimensions * 4 + i * 4, static_cast<int32_t>(dimension.lower));
    }
    size_t data = offset;
    for (size_t i = 0; i < cells.size(); ++i) if (cells[i]) {
        if (nulls) bytes[header + i / 8] = static_cast<char>(static_cast<unsigned char>(bytes[header + i / 8]) | (1U << (i % 8)));
        store<Oid>(bytes, data, *cells[i]); data += sizeof(Oid);
    }
    return bytes;
}
// PostgreSQL's nodeToString stores native Datum bytes, unlike its wire array
// representation. Location fields are stripped in persisted defaults.
inline std::string node(const Value& constant) {
    if (constant.type != 2205 && constant.type != 2206 && constant.type != 2210 && constant.type != 2211)
        throw DbError("XX001", "unsupported object constant node identity");
    const bool isArray = array(constant.type);
    std::ostringstream output;
    output << "{CONST :consttype " << constant.type
           << " :consttypmod -1 :constcollid 0 :constlen " << (isArray ? -1 : 4)
           << " :constbyval " << (isArray ? "false" : "true")
           << " :constisnull " << (constant.value ? "false" : "true")
           << " :location -1 :constvalue ";
    if (!constant.value) return output.str() + "<>}";
    std::string bytes;
    if (isArray) bytes = arrayDatum(constant);
    else {
        const uintptr_t datum = oid(*constant.value);
        bytes.assign(reinterpret_cast<const char*>(&datum), sizeof(datum));
    }
    output << (isArray ? bytes.size() : sizeof(Oid)) << " [ ";
    for (char byte : bytes) output << static_cast<int>(byte) << ' ';
    output << "]}";
    return output.str();
}
} // namespace dbms::pg_object_constant
