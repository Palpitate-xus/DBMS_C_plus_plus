#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <cstring>
#include <iostream>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    dbms::TableSchema integer;
    integer.tablename = "integer_values";
    integer.append(dbms::makeIntColumn("f", true, 2));
    const std::string zero(4, '\0');
    const dbms::StorageEngine::Condition coalesce{"scalarexpr", "coalesce(f,0)", "= 0"};
    assert(dbms::StorageEngine::evalConditionOnRow(coalesce, zero, integer, {true}));
    assert(dbms::StorageEngine::evalConditionOnRow(coalesce, zero, integer, {false}));
    assert(!dbms::StorageEngine::evalConditionOnRow(
        {"scalarexpr", "abs(f)", "= 0"}, zero, integer, {true}));
    dbms::TableSchema text;
    text.tablename = "text_values";
    text.append(dbms::makeTextColumn("t", true));
    const auto datum = [](const std::string& value) {
        std::string row(4 + value.size(), '\0');
        const uint16_t offset = 4;
        const uint16_t length = static_cast<uint16_t>(value.size());
        std::memcpy(row.data(), &offset, sizeof(offset));
        std::memcpy(row.data() + 2, &length, sizeof(length));
        row.replace(4, value.size(), value);
        return row;
    };
    assert(dbms::StorageEngine::evalConditionOnRow(
        {"scalarexpr", "coalesce(t,'fallback')", "= ''"}, datum(""), text, {false}));
    assert(!dbms::StorageEngine::evalConditionOnRow(
        {"scalarexpr", "coalesce(t,'fallback')", "= ''"}, datum(""), text, {true}));
    assert(dbms::StorageEngine::evalConditionOnRow(
        {"scalarexpr", "coalesce(t,'fallback')", "= 'NULL'"}, datum("NULL"), text, {false}));
    assert(dbms::StorageEngine::evalConditionOnRow(
        {"scalarexpr", "coalesce(t,'fallback')", "= 'fallback'"}, datum(""), text, {true}));
    assert(!dbms::StorageEngine::evalConditionOnRow(
        {"scalarexpr", "coalesce(t,'fallback')", "!= NULL"}, datum(""), text, {false}));
    assert(dbms::StorageEngine::evalConditionOnRow(
        {"scalarexpr", "coalesce(t,'fallback')", "= 'a''b'"}, datum("a'b"), text, {false}));
    assert(!dbms::StorageEngine::evalConditionOnRow(
        {"scalarexpr", "coalesce(t,'fallback')", "= '0.500'"}, datum("0.50"), text, {false}));
    integer.cols[0] = dbms::makeIntColumn("f", true, 3);
    const int64_t exact = 9007199254740992LL;
    std::string bigint(8, '\0');
    std::memcpy(bigint.data(), &exact, sizeof(exact));
    assert(!dbms::StorageEngine::evalConditionOnRow(
        {"scalarexpr", "coalesce(f,0)", "= 9007199254740993"}, bigint, integer, {false}));
    std::cout << "[COALESCE PREDICATE NULL] passed\n";
}
