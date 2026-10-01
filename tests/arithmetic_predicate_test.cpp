#include "commands/TableManage.h"
#include "catalog/type_registry.h"

#include <cassert>
#include <cstring>
#include <iostream>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    dbms::TableSchema schema;
    schema.tablename = "arithmetic_values";
    schema.append(dbms::makeIntColumn("f", true, 2));
    int32_t number = 2;
    std::string row(4, '\0');
    std::memcpy(row.data(), &number, sizeof(number));
    const auto conditions = dbms::StorageEngine::parseConditions({">f/0.5 1"});
    assert(conditions.size() == 1);
    assert(dbms::StorageEngine::evalConditionOnRow(conditions.front(), row, schema, {false}));
    assert(!dbms::StorageEngine::evalConditionOnRow(conditions.front(), row, schema, {true}));
    const auto signedLiteral = dbms::StorageEngine::parseConditions({"=f -2"});
    assert(signedLiteral.size() == 1);
    assert(signedLiteral.front().op == "=" && signedLiteral.front().colName == "f");
    const auto grouped = dbms::StorageEngine::parseConditions(
        {"typedexpr (f/(0.25+0.25))>3 AND f=2"});
    assert(grouped.size() == 1);
    assert(dbms::StorageEngine::evalConditionOnRow(grouped.front(), row, schema, {false}));
    const auto lazy = dbms::StorageEngine::parseConditions(
        {"typedexpr coalesce(1,1/0)=1"});
    assert(lazy.size() == 1);
    assert(dbms::StorageEngine::evalConditionOnRow(lazy.front(), row, schema, {true}));
    bool rejected = false;
    try {
        const auto zero = dbms::StorageEngine::parseConditions({"typedexpr f/0>1"});
        assert(zero.size() == 1);
        (void)dbms::StorageEngine::evalConditionOnRow(zero.front(), row, schema, {false});
    } catch (const std::runtime_error& error) {
        rejected = std::string(error.what()).find("22012") != std::string::npos;
    }
    assert(rejected);
    std::cout << "[ARITHMETIC PREDICATE] passed" << std::endl;
}
