#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <sstream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "floating_subnormal_storage", database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    TableSchema schema;
    schema.tablename = "float_values";
    schema.formatVersion = 2;
    schema.append(makeIntColumn("id", false, 4, true));
    schema.append(makeFloatColumn("f", false));
    schema.append(makeDoubleColumn("d", false));
    assert(g_engine.createTable(database, schema) == DBStatus::OK);
    assert(g_engine.insert(database, schema.tablename,
        {{"id", "1"}, {"f", "1.401298464324817e-45"},
         {"d", "4.9406564584124654e-324"}}) == DBStatus::OK);
    const auto rows = g_engine.query(database, schema.tablename, {"=id 1"}, {"f", "d"});
    assert(rows.size() == 1);
    std::istringstream values(rows.front());
    std::string f, d;
    values >> f >> d;
    assert(std::strtof(f.c_str(), nullptr) == std::numeric_limits<float>::denorm_min());
    assert(std::strtod(d.c_str(), nullptr) == std::numeric_limits<double>::denorm_min());
    for (const std::string column : {"f", "d"}) {
        for (const std::string invalid : {"1e1000", "-1e1000", "1e-1000", "1.5tail"}) {
            assert(g_engine.insert(database, schema.tablename,
                {{"id", "2"}, {column, invalid}}) != DBStatus::OK);
            assert(g_engine.query(database, schema.tablename, {"=id 2"}, {"id"}).empty());
            assert(g_engine.update(database, schema.tablename,
                {{column, invalid}}, {"=id 1"}) != DBStatus::OK);
        }
    }
    assert(g_engine.query(database, schema.tablename, {"=id 1"}, {"f", "d"}) == rows);
    assert(g_engine.update(database, schema.tablename,
        {{"f", "-0"}, {"d", "-0"}}, {"=id 1"}) == DBStatus::OK);
    const auto zeros = g_engine.query(database, schema.tablename, {"=id 1"}, {"f", "d"});
    assert(zeros == std::vector<std::string>{"-0 -0 "});
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[FLOATING SUBNORMAL STORAGE] representable values and strict errors passed\n";
}
