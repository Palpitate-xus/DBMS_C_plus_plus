#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "table_schema.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <set>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static const dbms::Column* findCol(const dbms::TableSchema& t, const std::string& name) {
    for (size_t i = 0; i < t.len; ++i)
        if (t.cols[i].dataName == name) return &t.cols[i];
    return nullptr;
}

static size_t rowCount(const std::string& db, const std::string& tbl) {
    return g_engine.query(db, tbl, {}, {}).size();
}

static std::set<std::string> rowSet(const std::string& db, const std::string& tbl) {
    auto rows = g_engine.query(db, tbl, {}, {});
    return std::set<std::string>(rows.begin(), rows.end());
}

static void seed(const std::string& db, dbms::DdlExecutor& ddl, Session& s) {
    // Columns ordered id,name,age so the alphabetical set order {age,id,name}
    // differs from schema order {id,name,age} — exercises the mapping fix.
    assert(!ddl.executeSql("CREATE TABLE src (id INT, name VARCHAR(30), age INT)", s));
    auto ins = [&](const std::string& id, const std::string& nm, const std::string& age) {
        std::map<std::string, std::string> v{{"id", id}, {"name", nm}, {"age", age}};
        assert(g_engine.insert(db, "src", v) == dbms::DBStatus::OK);
    };
    ins("1", "alice", "30");
    ins("2", "bob", "25");
    ins("3", "carol", "40");
}

static void test_ctas_star() {
    std::string db = testDbPath("ctas_star");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    seed(db, ddl, s);

    assert(!ddl.executeSql("CREATE TABLE cp AS SELECT * FROM src", s));
    auto sc = g_engine.getTableSchema(db, "cp");
    auto src = g_engine.getTableSchema(db, "src");
    assert(sc.len == 3);

    // Precise types copied from source (not all coerced to varchar).
    const dbms::Column* id = findCol(sc, "id");
    const dbms::Column* name = findCol(sc, "name");
    const dbms::Column* age = findCol(sc, "age");
    assert(id && name && age);
    assert(id->dataType == findCol(src, "id")->dataType);
    assert(name->dataType == findCol(src, "name")->dataType);
    assert(name->dsize == findCol(src, "name")->dsize);
    // CTAS yields plain columns: PK/identity dropped.
    assert(id->isPrimaryKey == false);

    // All 3 rows copied with correct column mapping (regression: the old code
    // mapped values to the alphabetically-sorted set order and dropped rows).
    assert(rowCount(db, "cp") == 3);
    auto rows = rowSet(db, "cp");
    assert(rows.count("1 alice 30 "));
    assert(rows.count("2 bob 25 "));
    assert(rows.count("3 carol 40 "));

    auto& catalog = g_engine.catalogService().get(db);
    const auto* relation = catalog.resolveRelation("cp", {"public"});
    assert(relation != nullptr && relation->relname == "cp" &&
           relation->relnatts == 3);
    const dbms::Oid relationOid = relation->oid;
    assert(catalog.findAttribute(relationOid, "id") != nullptr);
    assert(catalog.findAttribute(relationOid, "name") != nullptr);
    assert(catalog.findAttribute(relationOid, "age") != nullptr);

    g_engine.catalogService().evict(db);
    auto& reloaded = g_engine.catalogService().get(db);
    relation = reloaded.resolveRelation("cp", {"public"});
    assert(relation != nullptr && relation->oid == relationOid);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[CTAS] SELECT * precise types + mapping OK" << std::endl;
}

static void test_schema_qualified_ctas_catalog_identity() {
    const std::string db = testDbPath("ctas_schema_catalog");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    seed(db, ddl, s);
    assert(!ddl.executeSql("CREATE SCHEMA analytics", s));

    assert(!ddl.executeSql(
        "CREATE TABLE analytics.cp AS SELECT id, age FROM src", s));
    assert(g_engine.tableExists(db, "analytics__cp"));

    auto& catalog = g_engine.catalogService().get(db);
    const auto* analytics = catalog.findNamespaceByName("analytics");
    assert(analytics != nullptr);
    const auto* relation = catalog.findClassByName("cp", analytics->oid);
    assert(relation != nullptr && relation->relnatts == 2);
    const dbms::Oid relationOid = relation->oid;
    assert(catalog.findAttribute(relationOid, "id") != nullptr);
    assert(catalog.findAttribute(relationOid, "age") != nullptr);
    assert(catalog.resolveRelation("cp", {"public"}) == nullptr);

    g_engine.catalogService().evict(db);
    auto& reloaded = g_engine.catalogService().get(db);
    relation = reloaded.resolveRelation("cp", {"analytics"});
    assert(relation != nullptr && relation->oid == relationOid);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[CTAS] schema-qualified catalog identity OK" << std::endl;
}

static void test_ctas_drops_source_column_constraints() {
    const std::string db = testDbPath("ctas_plain_columns");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql(
        "CREATE TABLE constrained_source ("
        "id INT PRIMARY KEY, "
        "code INT UNIQUE NOT NULL DEFAULT 9 CHECK (code > 0), "
        "base INT, "
        "doubled INT GENERATED ALWAYS AS (base * 2) STORED)", s));
    assert(g_engine.insert(
               db, "constrained_source",
               {{"id", "1"}, {"code", "5"}, {"base", "3"}}) ==
           dbms::DBStatus::OK);

    assert(!ddl.executeSql(
        "CREATE TABLE plain_copy AS SELECT * FROM constrained_source", s));
    const auto copy = g_engine.getTableSchema(db, "plain_copy");
    assert(copy.len == 4);
    for (size_t column = 0; column < copy.len; ++column) {
        assert(copy.cols[column].isNull);
        assert(!copy.cols[column].isPrimaryKey);
        assert(!copy.cols[column].isUnique);
        assert(!copy.cols[column].isAutoIncrement);
        assert(copy.cols[column].defaultValue.empty());
        assert(copy.cols[column].checkExpr.empty());
        assert(copy.cols[column].checkConstraintName.empty());
        assert(copy.cols[column].generatedExpr.empty());
        assert(copy.cols[column].generatedKind == 0);
    }

    // Duplicates, formerly invalid CHECK values, omitted values, and an
    // explicit value for the former generated column are all legal now.
    assert(g_engine.insert(
               db, "plain_copy",
               {{"id", "1"}, {"code", "5"}, {"base", "4"},
                {"doubled", "999"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(
               db, "plain_copy", {{"code", "-1"}}) ==
           dbms::DBStatus::OK);

    auto& catalog = g_engine.catalogService().get(db);
    const auto* relation =
        catalog.resolveRelation("plain_copy", {"public"});
    assert(relation != nullptr && !relation->relhasindex &&
           relation->relchecks == 0);
    for (const auto& attribute : catalog.findAttributes(relation->oid)) {
        assert(!attribute.attnotnull);
        assert(!attribute.atthasdef);
        assert(attribute.attidentity == '\0');
        assert(attribute.attgenerated == '\0');
    }

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[CTAS] source constraints are not copied OK" << std::endl;
}

static void test_ctas_with_no_data() {
    std::string db = testDbPath("ctas_nodata");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    seed(db, ddl, s);

    assert(!ddl.executeSql("CREATE TABLE cp AS SELECT * FROM src WITH NO DATA", s));
    auto sc = g_engine.getTableSchema(db, "cp");
    assert(sc.len == 3);                 // structure created
    assert(rowCount(db, "cp") == 0);     // but no rows

    cleanup(db);
    std::cout << "[CTAS] WITH NO DATA OK" << std::endl;
}

static void test_ctas_with_data_explicit() {
    std::string db = testDbPath("ctas_withdata");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    seed(db, ddl, s);

    assert(!ddl.executeSql("CREATE TABLE cp AS SELECT * FROM src WITH DATA", s));
    assert(rowCount(db, "cp") == 3);

    cleanup(db);
    std::cout << "[CTAS] WITH DATA (explicit) OK" << std::endl;
}

static void test_ctas_projection() {
    std::string db = testDbPath("ctas_proj");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    seed(db, ddl, s);

    // Projection with WHERE: only id,name for rows with age > 26.
    assert(!ddl.executeSql("CREATE TABLE cp AS SELECT id, name FROM src WHERE age > 26", s));
    auto sc = g_engine.getTableSchema(db, "cp");
    assert(sc.len == 2);
    assert(findCol(sc, "id") && findCol(sc, "name"));
    assert(rowCount(db, "cp") == 2);     // alice(30), carol(40)
    auto rows = rowSet(db, "cp");
    assert(rows.count("1 alice "));
    assert(rows.count("3 carol "));

    cleanup(db);
    std::cout << "[CTAS] projection + WHERE OK" << std::endl;
}

static void test_ctas_no_data_with_projection() {
    std::string db = testDbPath("ctas_proj_nodata");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    seed(db, ddl, s);

    assert(!ddl.executeSql("CREATE TABLE cp AS SELECT id, age FROM src WITH NO DATA", s));
    auto sc = g_engine.getTableSchema(db, "cp");
    assert(sc.len == 2);
    assert(findCol(sc, "id") && findCol(sc, "age"));
    assert(rowCount(db, "cp") == 0);

    cleanup(db);
    std::cout << "[CTAS] projection + WITH NO DATA OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_ctas_star();
    test_schema_qualified_ctas_catalog_identity();
    test_ctas_drops_source_column_constraints();
    test_ctas_with_no_data();
    test_ctas_with_data_explicit();
    test_ctas_projection();
    test_ctas_no_data_with_projection();
    std::cout << "[CTAS] all passed" << std::endl;
    return 0;
}
