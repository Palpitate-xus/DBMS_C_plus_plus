#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include <cassert>
#include <cstring>
#include <iostream>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    dbms::TableSchema table;
    table.tablename = "nonstrict_values";
    table.append(dbms::makeIntColumn("f", true, 2));
    table.append(dbms::makeIntColumn("g", true, 2));
    const std::string zeros(8, '\0');
    const auto predicate = [&](const std::string& function, const std::vector<bool>& nulls) {
        return dbms::StorageEngine::evalConditionOnRow(
            {"scalarexpr", function + "(f,g)", "= 0"}, zeros, table, nulls);
    };
    assert(predicate("nullif", {false, true}));
    assert(!predicate("nullif", {true, false}));
    assert(!predicate("nullif", {false, false}));
    assert(predicate("greatest", {false, true}));
    assert(predicate("greatest", {true, false}));
    assert(!predicate("greatest", {true, true}));
    assert(predicate("least", {false, true}));
    assert(predicate("least", {true, false}));
    assert(!predicate("least", {true, true}));
    assert(!dbms::StorageEngine::evalConditionOnRow(
        {"scalarexpr", "greatest(f,g)", "!= NULL"}, zeros, table, {false, true}));
    assert(!dbms::StorageEngine::evalConditionOnRow(
        {"scalarexpr", "abs(f)", "= 0"}, zeros, table, {true, false}));
    std::cout << "[NONSTRICT PREDICATE NULL] passed\n";
}
