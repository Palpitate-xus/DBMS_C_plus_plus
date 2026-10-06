#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "api_scalar_literal", db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    TableSchema table;
    table.tablename = "values_table";
    table.formatVersion = DATA_FILE_FORMAT_VERSION;
    table.append(makeIntColumn("id", false, 4, true));
    table.append(makeVarCharColumn("v", true, 80));
    table.append(makeVarCharColumn("other", true, 80));
    assert(g_engine.createTable(db, table) == DBStatus::OK);
    const std::vector<std::string> values = {
        "same@example.test", "a'b", "1+2", "NULL", "", "OTHER"};
    for (size_t i = 0; i < values.size(); ++i) {
        assert(g_engine.insertRow(db, table.tablename,
            {{"id", std::to_string(i + 1)}, {"v", values[i]}, {"other", "wrong"}}) == DBStatus::OK);
    }
    assert(g_engine.insertRow(db, table.tablename,
        {{"id", "7"}, {"v", std::nullopt}, {"other", "wrong"}}) == DBStatus::OK);
    const auto check = [&](const std::string& condition) {
        std::cout << "API condition: " << condition << std::endl;
        assert(g_engine.query(db, table.tablename, {condition}, {"id"}).size() == 1);
    };
    check("=UPPER(v) SAME@EXAMPLE.TEST");
    check("=UPPER(v) A'B");
    check("=UPPER(v) 'A''B'");
    check("=UPPER(v) 1+2");
    check("=UPPER(v) NULL");
    check("=UPPER(v) ");
    check("=UPPER(v) OTHER");
    check("=ABS(id) 2");
    check("=ABS(id) 1+1");
    check("=UPPER(v) '1+' || '2'");
    check("=LENGTH(v) CAST(0 AS integer)");
    check("isnull UPPER(v)");
    assert(g_engine.query(db, table.tablename, {"=v v"}, {"id"}).empty());
    assert(g_engine.query(db, table.tablename,
        {"typedexpr UPPER(v) = 'SAME@EXAMPLE.TEST'"}, {"id"}).size() == 1);
    bool preciseMissing = false;
    try {
        (void)g_engine.query(db, table.tablename,
            {"typedexpr UPPER(v) = missing"}, {"id"});
    } catch (const std::exception& error) {
        preciseMissing = std::string(error.what()).find("SQLSTATE 42703") != std::string::npos;
    }
    assert(preciseMissing);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[API SCALAR LITERAL] preserved decoded RHS and strict SQL binding\n";
}
