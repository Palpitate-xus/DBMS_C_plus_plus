#pragma once

#include "catalog/FormatType.h"
#include "catalog/PgObjectConstant.h"
#include "expression/StoredRegtypeValue.h"

namespace dbms::default_expression {

inline std::string quote(std::string_view value) {
    std::string result = "'";
    for (char byte : value) {
        if (byte == '\0') throw DbError("XX001", "catalog expression contains an invalid string datum");
        if (byte == '\'') result += '\'';
        result += byte;
    }
    return result + '\'';
}

inline std::optional<std::string> deparse(std::string_view expression, Oid relation,
    StorageEngine& engine, const std::string& database, const Session* session) {
    if (expression.empty()) throw DbError("0A000", "column default expression has no supported node encoding");
    const auto constant = pg_const_node::Reader(expression).parse();
    const bool isArray = pg_object_constant::array(constant.type);
    const bool object = isArray || constant.type == 2205 || constant.type == 2206;
    if (relation && (database.empty() || !engine.catalogService().relationIsVisible(database, relation, {})))
        return std::nullopt;

    int16_t width = 0;
    switch (constant.type) {
        case 16: width = 1; break;
        case 23: case 2205: case 2206: width = 4; break;
        case 20: width = 8; break;
        case 25: case 2210: case 2211: width = -1; break;
        default: throw DbError("0A000", "type of catalog constant expression is not supported");
    }
    if (constant.length != width || constant.byValue != (width > 0) || constant.typmod != -1 ||
        constant.collation != (constant.type == 25 ? 100U : 0U))
        throw DbError("XX001", "catalog constant declaration conflicts with its type");

    const auto label = [&] {
        return "::" + type_display::format(engine, database, constant.type, constant.typmod, session);
    };
    if (!constant.bytes) return "NULL" + label();
    const std::string_view bytes = *constant.bytes;
    if (constant.type == 25) return quote(pg_const_node::varlenaValue(bytes)) + label();
    const uintptr_t datum = constant.byValue ? pg_const_node::read<uintptr_t>(bytes) : 0;
    if (object) {
        const auto catalog = regtype_detail::metadata(engine, database);
        const auto input = isArray ? pg_object_constant::readArrayDatum(constant.type, bytes)
                                   : std::to_string(static_cast<Oid>(datum));
        return quote(stored_regtype_detail::output(input, isArray, catalog, session,
                                                   pg_object_constant::name(constant.type))) + label();
    }
    if (constant.type == 16) {
        if (datum > 1) throw DbError("XX001", "invalid boolean catalog constant");
        return datum ? "true" : "false";
    }
    if (constant.type == 23) {
        const auto number = static_cast<int32_t>(datum);
        return number >= 0 ? std::to_string(number) : quote(std::to_string(number)) + label();
    }
    return quote(std::to_string(static_cast<int64_t>(datum))) + label();
}

} // namespace dbms::default_expression
