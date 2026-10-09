#include "catalog/type_registry.h"
#include "catalog/CatalogService.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "utils/interval_type.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <fstream>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    // ALTER retains the same semantic declaration as CREATE/Cast, including
    // all legal ranges, fractional precision and an array suffix.
    SQLParser parser;
    for (const auto* suffix : {"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
         "YEAR TO MONTH", "DAY TO HOUR", "DAY TO MINUTE", "DAY TO SECOND",
         "HOUR TO MINUTE", "HOUR TO SECOND", "MINUTE TO SECOND",
         "SECOND(1)", "DAY TO SECOND(3)", "SECOND(2)[]"}) {
        const std::string spelling = std::string("INTERVAL ") + suffix;
        const auto parsed = parser.parse("ALTER TABLE t ALTER COLUMN v TYPE " + spelling);
        assert(parsed.success);
        const auto* alter = dynamic_cast<const AlterTableStmt*>(parsed.stmt.get());
        assert(alter && alter->subCommands.size() == 1);
        assert(alter->subCommands.front().dataType == spelling);
        const auto expected = SQLParser::parseTypeSpecification(spelling);
        const auto actual = SQLParser::parseTypeSpecification(alter->subCommands.front().dataType);
        assert(actual.typeMods == expected.typeMods && actual.isArray == expected.isArray);
    }
    for (const auto* suffix : {"YEAR TO DAY", "DAY(3)", "SECOND(-1)", "SECOND(1,2)", "SECOND["}) {
        const auto parsed = parser.parse(std::string("ALTER TABLE t ALTER COLUMN v TYPE INTERVAL ") + suffix);
        assert(!parsed.success && parsed.sqlState == "42601");
    }
    const auto db = testDbPath("interval_column_modifier");
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t(id INT PRIMARY KEY,v INTERVAL DAY TO SECOND(3),y INTERVAL YEAR,a INTERVAL SECOND(2)[])", session));
    const int32_t precise = (7176 << 16) | 3;
    const int32_t years = (4 << 16) | 65535;
    const int32_t arrayPrecise = (4096 << 16) | 2;
    const auto schema = g_engine.getTableSchema(db, "t");
    assert(schema.cols[1].typeMod == precise && schema.cols[2].typeMod == years && schema.cols[3].typeMod == arrayPrecise);
    auto& catalog = g_engine.catalogService().get(db);
    const auto* space = catalog.findNamespaceByName("public");
    assert(space);
    const auto* relation = catalog.findClassByName("t", space->oid);
    assert(relation);
    const auto attributes = catalog.findAttributesByNum(relation->oid);
    assert(attributes.size() == 4);
    for (const auto& attribute : attributes) {
        if (attribute.attname == "v") assert(attribute.atttypmod == precise);
        if (attribute.attname == "y") assert(attribute.atttypmod == years);
        if (attribute.attname == "a") assert(attribute.atttypmod == arrayPrecise);
    }
    std::ifstream file(g_engine.dbPath(db) / "t.stc", std::ios::binary);
    int32_t format = 0;
    file.read(reinterpret_cast<char*>(&format), sizeof(format));
    assert(file && format == 0x4442000c);
    // An independent engine reads declaration metadata from disk, not the
    // first engine's schema cache or a borrowed catalog descriptor.
    StorageEngine cold;
    const auto loaded = cold.getTableSchema(db, "t");
    assert(loaded.len == 4 && loaded.cols[1].typeMod == precise &&
           loaded.cols[2].typeMod == years && loaded.cols[3].typeMod == arrayPrecise);

    assert(g_engine.insert(db, "t", {{"id","1"},{"v","2.3456 seconds"},{"y","2"},
        {"a","{2.345 seconds,NULL,-2.345 seconds}"}}) == DBStatus::OK);
    assert(g_engine.query(db, "t", {}, {"v"}, {}) == std::vector<std::string>{"00:00:02.346 "});
    assert(g_engine.query(db, "t", {}, {"y"}, {}) == std::vector<std::string>{"2 years "});
    assert(g_engine.update(db, "t", {{"v","3.4567 seconds"},{"y","3"}}, {"=id 1"}) == DBStatus::OK);
    assert(g_engine.query(db, "t", {}, {"v"}, {}) == std::vector<std::string>{"00:00:03.457 "});
    assert(g_engine.query(db, "t", {}, {"y"}, {}) == std::vector<std::string>{"3 years "});
    assert(g_engine.insert(db, "t", {{"id","2"},{"v","9223372036854775807 microseconds"}}) == DBStatus::INVALID_VALUE);
    assert(!ddl.executeSql("ALTER TABLE t ALTER COLUMN v TYPE INTERVAL SECOND(1)", session));
    assert(g_engine.getTableSchema(db, "t").cols[1].typeMod == ((4096 << 16) | 1));
    assert(g_engine.query(db, "t", {}, {"v"}, {}) == std::vector<std::string>{"00:00:03.5 "});
    StorageEngine afterAlter;
    assert(afterAlter.getTableSchema(db, "t").cols[1].typeMod == ((4096 << 16) | 1));

    // Existing schemas without modifiers retain their previous format and
    // unbounded semantics. No declaration is inferred from a physical width.
    assert(!ddl.executeSql("CREATE TABLE legacy_interval(v INTERVAL)", session));
    StorageEngine oldFormatReader;
    const auto legacy = oldFormatReader.getTableSchema(db, "legacy_interval");
    assert(legacy.len == 1 && legacy.cols[0].typeMod == -1);
    std::cout << "[INTERVAL COLUMN MODIFIER] metadata/cold schema/native insert-update/rewrite/legacy controls passed\n";
}
