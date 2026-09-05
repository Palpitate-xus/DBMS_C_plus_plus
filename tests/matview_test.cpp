#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static std::vector<dbms::StorageEngine::SqlRow> readStructuredRows(
    const std::string& db, const std::string& tableName) {
    const auto schema = g_engine.getTableSchema(db, tableName);
    std::vector<dbms::StorageEngine::SqlRow> rows;
    assert(g_engine.forEachRow(
        db, tableName,
        [&](uint32_t pageId, uint16_t slotId,
            const char* data, size_t length) {
            const int64_t rid =
                dbms::StorageEngine::encodeRid(pageId, slotId);
            dbms::StorageEngine::bindNullRow(
                &g_engine, db, tableName, rid, schema.len);
            dbms::StorageEngine::SqlRow row;
            const std::string buffer(data, length);
            for (size_t column = 0; column < schema.len; ++column) {
                bool isNull = false;
                std::string value = g_engine.extractColumnValue(
                    buffer, schema, column, db, true, &isNull);
                if (isNull) {
                    row[schema.cols[column].dataName] = std::nullopt;
                } else {
                    row[schema.cols[column].dataName] = std::move(value);
                }
            }
            dbms::StorageEngine::unbindNullRow();
            rows.push_back(std::move(row));
        }));
    return rows;
}

static void test_create_matview_select_star() {
    std::string db = testDbPath("matview_star");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"name", "bob"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t", s));

    std::string backing = dbms::StorageEngine::materializedViewPrefix("mv");
    assert(g_engine.tableExists(db, backing));
    assert(g_engine.isMaterializedView(db, "mv"));
    assert(!g_engine.getMaterializedViewSQL(db, "mv").empty());

    const auto backingSchema = g_engine.getTableSchema(db, backing);
    assert(backingSchema.len == 2);
    assert(backingSchema.cols[0].dataName == "id");
    assert(backingSchema.cols[0].dataType == "int");
    assert(backingSchema.cols[0].isNull);
    assert(!backingSchema.cols[0].isPrimaryKey);
    assert(!backingSchema.cols[0].isUnique);
    assert(backingSchema.cols[1].dataName == "name");
    assert(backingSchema.cols[1].dataType == "varchar");
    assert(backingSchema.cols[1].dsize == 50);

    auto rows = g_engine.query(db, backing, {}, {"id", "name"}, {});
    assert(rows.size() == 2);

    dbms::CatalogManager& initialCatalog =
        g_engine.catalogService().get(db);
    const auto* relation = initialCatalog.resolveRelation("mv", {"public"});
    assert(relation != nullptr);
    assert(relation->relkind == 'm');
    assert(relation->relnatts == 2);
    assert(relation->relispopulated);
    const dbms::Oid relationOid = relation->oid;
    const auto attributes = initialCatalog.findAttributes(relationOid);
    assert(attributes.size() == 2);
    assert(attributes[0].attname == "id");
    assert(attributes[1].attname == "name");

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloadedCatalog =
        g_engine.catalogService().get(db);
    relation = reloadedCatalog.findClass(relationOid);
    assert(relation != nullptr && relation->relkind == 'm');
    assert(relation->relispopulated);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[MATVIEW] CREATE SELECT * OK" << std::endl;
}

static void test_create_matview_select_columns() {
    std::string db = testDbPath("matview_cols");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(50), score INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}, {"score", "90"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"name", "bob"}, {"score", "80"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW mv AS SELECT id, score FROM t", s));

    std::string backing = dbms::StorageEngine::materializedViewPrefix("mv");
    assert(g_engine.tableExists(db, backing));

    auto rows = g_engine.query(db, backing, {}, {"id", "score"}, {});
    assert(rows.size() == 2);
    // Verify name column is not present in backing table schema.
    auto schema = g_engine.getTableSchema(db, backing);
    bool hasName = false;
    for (size_t i = 0; i < schema.len; ++i) {
        if (schema.cols[i].dataName == "name") hasName = true;
    }
    assert(!hasName);

    cleanup(db);
    std::cout << "[MATVIEW] CREATE SELECT columns OK" << std::endl;
}

static void test_create_matview_where() {
    std::string db = testDbPath("matview_where");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT PRIMARY KEY, score INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"score", "50"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"score", "90"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW mv AS SELECT id FROM t WHERE score > 60", s));

    std::string backing = dbms::StorageEngine::materializedViewPrefix("mv");
    auto rows = g_engine.query(db, backing, {}, {"id"}, {});
    assert(rows.size() == 1);
    assert(rows[0].find("2") != std::string::npos);

    cleanup(db);
    std::cout << "[MATVIEW] CREATE WHERE OK" << std::endl;
}

static void test_create_matview_reversed_projection() {
    std::string db = testDbPath("matview_rev");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    // Columns id,name,age: selecting "age, id" reverses schema order, which the
    // old set-order mapping got wrong (values landed in the wrong columns).
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20), age INT)", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}, {"age", "30"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"name", "bob"}, {"age", "25"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW mv AS SELECT age, id FROM t", s));
    std::string backing = dbms::StorageEngine::materializedViewPrefix("mv");

    // Backing has only age,id and rows mapped correctly: query in backing schema
    // order. The backing schema column order follows the projection (age, id).
    auto rows = g_engine.query(db, backing, {}, {}, {});
    assert(rows.size() == 2);
    std::set<std::string> got(rows.begin(), rows.end());
    // age then id, each value with trailing space.
    assert(got.count("30 1 "));
    assert(got.count("25 2 "));

    cleanup(db);
    std::cout << "[MATVIEW] reversed projection mapping OK" << std::endl;
}

static void test_create_matview_preserves_exact_sql_values() {
    const std::string db = testDbPath("matview_exact_values");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE source_values (id INT, note VARCHAR(50), marker VARCHAR(50))",
        s));

    using SqlRow = dbms::StorageEngine::SqlRow;
    assert(g_engine.insertRow(
               db, "source_values",
               SqlRow{{"id", std::string("1")},
                      {"note", std::string("hello world")},
                      {"marker", std::string("")}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(
               db, "source_values",
               SqlRow{{"id", std::string("2")},
                      {"note", std::string("")},
                      {"marker", std::nullopt}}) == dbms::DBStatus::OK);
    assert(g_engine.insertRow(
               db, "source_values",
               SqlRow{{"id", std::string("3")},
                      {"note", std::string("NULL")},
                      {"marker", std::string("two words")}}) ==
           dbms::DBStatus::OK);

    assert(!ddl.executeSql(
        "CREATE MATERIALIZED VIEW exact_values AS "
        "SELECT id, note, marker FROM source_values", s));
    const std::string backing =
        dbms::StorageEngine::materializedViewPrefix("exact_values");
    const auto rows = readStructuredRows(db, backing);
    assert(rows.size() == 3);
    const auto findById = [&](const std::string& id) {
        return std::find_if(
            rows.begin(), rows.end(), [&](const SqlRow& row) {
                const auto value = row.find("id");
                return value != row.end() && value->second &&
                       *value->second == id;
            });
    };
    auto row = findById("1");
    assert(row != rows.end());
    assert(row->at("note") && *row->at("note") == "hello world");
    assert(row->at("marker") && row->at("marker")->empty());
    row = findById("2");
    assert(row != rows.end());
    assert(row->at("note") && row->at("note")->empty());
    assert(!row->at("marker"));
    row = findById("3");
    assert(row != rows.end());
    assert(row->at("note") && *row->at("note") == "NULL");
    assert(row->at("marker") && *row->at("marker") == "two words");

    cleanup(db);
    std::cout << "[MATVIEW] exact SQL values preserved OK" << std::endl;
}

static void test_create_matview_with_no_data() {
    std::string db = testDbPath("matview_nodata");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20))", s));
    assert(g_engine.insert(db, "t", {{"id", "1"}, {"name", "alice"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "t", {{"id", "2"}, {"name", "bob"}}) == dbms::DBStatus::OK);

    assert(!ddl.executeSql("CREATE MATERIALIZED VIEW mv AS SELECT * FROM t WITH NO DATA", s));
    std::string backing = dbms::StorageEngine::materializedViewPrefix("mv");
    assert(g_engine.tableExists(db, backing));         // structure created
    auto schema = g_engine.getTableSchema(db, backing);
    assert(schema.len == 2);                            // both columns present
    auto rows = g_engine.query(db, backing, {}, {}, {});
    assert(rows.empty());                               // but no rows

    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* relation = catalog.resolveRelation("mv", {"public"});
    assert(relation != nullptr && relation->relkind == 'm');
    assert(!relation->relispopulated);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[MATVIEW] WITH NO DATA OK" << std::endl;
}

static void test_drop_matview_cleans_catalog_atomically() {
    const std::string db = testDbPath("matview_drop_catalog");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT, name VARCHAR(20))", s));
    assert(!ddl.executeSql(
        "CREATE MATERIALIZED VIEW first_mv AS SELECT * FROM t", s));
    assert(!ddl.executeSql(
        "CREATE MATERIALIZED VIEW second_mv AS SELECT id FROM t", s));

    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* firstRelation =
        catalog.resolveRelation("first_mv", {"public"});
    assert(firstRelation != nullptr && firstRelation->relkind == 'm');
    const dbms::Oid firstOid = firstRelation->oid;
    assert(catalog.findAttributes(firstOid).size() == 2);

    bool handled = false;
    bool error = dbms::tryDdlBridge(
        "drop materialized view first_mv, missing_mv",
        dbms::SqlCommand::DropMaterializedView, s, handled);
    assert(handled && error);
    assert(g_engine.isMaterializedView(db, "first_mv"));
    assert(catalog.findClass(firstOid) != nullptr);

    error = dbms::tryDdlBridge(
        "drop materialized view first_mv, second_mv",
        dbms::SqlCommand::DropMaterializedView, s, handled);
    assert(handled && !error);
    assert(!g_engine.isMaterializedView(db, "first_mv"));
    assert(!g_engine.isMaterializedView(db, "second_mv"));
    assert(!g_engine.tableExists(
        db, dbms::StorageEngine::materializedViewPrefix("first_mv")));
    assert(!g_engine.tableExists(
        db, dbms::StorageEngine::materializedViewPrefix("second_mv")));
    assert(catalog.findClass(firstOid) == nullptr);
    assert(catalog.findAttributes(firstOid).empty());

    g_engine.catalogService().evict(db);
    dbms::CatalogManager& reloaded = g_engine.catalogService().get(db);
    assert(reloaded.resolveRelation("first_mv", {"public"}) == nullptr);
    assert(reloaded.findClass(firstOid) == nullptr);
    assert(reloaded.findAttributes(firstOid).empty());

    error = dbms::tryDdlBridge(
        "drop materialized view if exists first_mv",
        dbms::SqlCommand::DropMaterializedView, s, handled);
    assert(handled && !error);
    error = dbms::tryDdlBridge(
        "drop materialized view first_mv",
        dbms::SqlCommand::DropMaterializedView, s, handled);
    assert(handled && error);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[MATVIEW] DROP catalog lifecycle OK" << std::endl;
}

static void test_matview_metadata_write_failure_rolls_back() {
    const std::string db = testDbPath("matview_metadata_failure");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT)", s));

    const fs::path blockedTarget =
        g_engine.viewsDir(db) / "blocked_mv.mview";
    fs::create_directories(blockedTarget);
    assert(ddl.executeSql(
        "CREATE MATERIALIZED VIEW blocked_mv AS SELECT id FROM t", s));
    assert(fs::is_directory(blockedTarget));
    assert(!g_engine.isMaterializedView(db, "blocked_mv"));
    assert(g_engine.getMaterializedViewSQL(db, "blocked_mv").empty());
    assert(!g_engine.tableExists(
        db, dbms::StorageEngine::materializedViewPrefix("blocked_mv")));
    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    assert(catalog.resolveRelation("blocked_mv", {"public"}) == nullptr);
    for (const auto& entry : fs::directory_iterator(g_engine.viewsDir(db))) {
        assert(entry.path().filename().string().find(
                   "blocked_mv.mview.tmp.") != 0);
    }

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[MATVIEW] metadata write failure rollback OK" << std::endl;
}

static void test_matview_drop_io_failure_rolls_back() {
    const std::string db = testDbPath("matview_drop_io_failure");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT)", s));
    assert(!ddl.executeSql(
        "CREATE MATERIALIZED VIEW mv AS SELECT id FROM t", s));
    const std::string backing =
        dbms::StorageEngine::materializedViewPrefix("mv");
    const fs::path metadata = g_engine.viewsDir(db) / "mv.mview";
    fs::remove(metadata);
    fs::create_directory(metadata);
    std::ofstream(metadata / "keep") << "force non-empty directory";

    bool threw = false;
    bool error = false;
    try {
        error = ddl.executeSql("DROP MATERIALIZED VIEW mv", s);
    } catch (const fs::filesystem_error&) {
        threw = true;
    }
    assert(!threw);
    assert(error);
    assert(g_engine.tableExists(db, backing));
    assert(fs::is_directory(metadata));
    assert(fs::exists(metadata / "keep"));
    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* relation = catalog.resolveRelation("mv", {"public"});
    assert(relation != nullptr && relation->relkind == 'm');

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[MATVIEW] DROP I/O failure rollback OK" << std::endl;
}

static void test_schema_qualified_matview_name() {
    const std::string db = testDbPath("matview_qualified_name");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t (id INT)", s));
    assert(ddl.executeSql(
        "CREATE MATERIALIZED VIEW missing_schema.mv AS SELECT id FROM t",
        s));
    assert(!g_engine.isMaterializedView(db, "missing_schema.mv"));
    assert(!g_engine.isMaterializedView(db, "mv"));

    assert(!ddl.executeSql("CREATE SCHEMA reporting", s));
    assert(!ddl.executeSql(
        "CREATE MATERIALIZED VIEW reporting.mv AS SELECT id FROM t", s));
    assert(g_engine.isMaterializedView(db, "reporting.mv"));
    assert(!g_engine.isMaterializedView(db, "mv"));
    assert(g_engine.tableExists(
        db, dbms::StorageEngine::materializedViewPrefix("reporting.mv")));
    dbms::CatalogManager& catalog = g_engine.catalogService().get(db);
    const auto* relation =
        catalog.resolveRelation("reporting.mv", {"public"});
    assert(relation != nullptr && relation->relkind == 'm');
    const dbms::Oid relationOid = relation->oid;

    assert(!ddl.executeSql("DROP MATERIALIZED VIEW reporting.mv", s));
    assert(!g_engine.isMaterializedView(db, "reporting.mv"));
    assert(!g_engine.tableExists(
        db, dbms::StorageEngine::materializedViewPrefix("reporting.mv")));
    assert(catalog.findClass(relationOid) == nullptr);

    g_engine.catalogService().evict(db);
    cleanup(db);
    std::cout << "[MATVIEW] schema-qualified name lifecycle OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_create_matview_select_star();
    test_create_matview_select_columns();
    test_create_matview_where();
    test_create_matview_reversed_projection();
    test_create_matview_preserves_exact_sql_values();
    test_create_matview_with_no_data();
    test_drop_matview_cleans_catalog_atomically();
    test_matview_metadata_write_failure_rolls_back();
    test_matview_drop_io_failure_rolls_back();
    test_schema_qualified_matview_name();
    std::cout << "[MATVIEW] all passed" << std::endl;
    return 0;
}
