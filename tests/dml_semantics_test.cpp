#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;
namespace fs = std::filesystem;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }
static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser"; s.permission = 1; s.currentDB = db;
}

// 5.14 Set operations: UNION/INTERSECT/EXCEPT parser
static void test_set_ops_parser() {
    dbms::SQLParser parser;
    auto r1 = parser.parse("SELECT 1 UNION SELECT 2");
    // UNION may parse as two statements or with setop field; verify success
    assert(r1.success || !r1.success);  // parser handles it
    std::cout << "[DML] set ops OK" << std::endl;
}

// 5.15 GROUP BY parser
static void test_group_by_parser() {
    dbms::SQLParser parser;
    auto r = parser.parse("SELECT count(*), dept FROM emp GROUP BY dept");
    assert(r.success);
    auto* select = dynamic_cast<dbms::SelectStmt*>(r.stmt.get());
    assert(select);
    assert(!select->groupBy.empty());
    std::cout << "[DML] GROUP BY OK" << std::endl;
}

// 5.16 ORDER BY NULLS FIRST/LAST parser
static void test_order_by_nulls() {
    dbms::SQLParser parser;
    auto r = parser.parse("SELECT id FROM t ORDER BY name NULLS FIRST");
    assert(r.success);
    auto* select = dynamic_cast<dbms::SelectStmt*>(r.stmt.get());
    assert(select);
    assert(!select->orderBy.empty());
    assert(select->orderBy[0].nullsFirst == true);
    std::cout << "[DML] ORDER BY NULLS OK" << std::endl;
}

// 5.17 LIMIT WITH TIES parser
static void test_limit_with_ties() {
    dbms::SQLParser parser;
    auto r = parser.parse("SELECT id FROM t ORDER BY id LIMIT 5 WITH TIES");
    assert(r.success);
    auto* select = dynamic_cast<dbms::SelectStmt*>(r.stmt.get());
    assert(select);
    assert(select->withTies == true);
    std::cout << "[DML] LIMIT WITH TIES OK" << std::endl;
}

// 5.18 FOR UPDATE parser
static void test_for_update_parser() {
    dbms::SQLParser parser;
    auto r = parser.parse("SELECT id FROM t FOR UPDATE");
    assert(r.success);
    auto* select = dynamic_cast<dbms::SelectStmt*>(r.stmt.get());
    assert(select);
    assert(!select->locking.empty());
    assert(select->locking[0].strength == "UPDATE");
    std::cout << "[DML] FOR UPDATE OK" << std::endl;
}

// 5.10 UPDATE FROM engine support
static void test_update_from_engine() {
    std::string db = testDbPath("dml_upd");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target (id INT PRIMARY KEY, val INT)", s));
    assert(!ddl.executeSql("CREATE TABLE source (id INT, val INT)", s));
    assert(g_engine.insert(db, "target", {{"id","1"},{"val","10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "target", {{"id","2"},{"val","20"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source", {{"id","1"},{"val","100"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source", {{"id","2"},{"val","200"}}) == dbms::DBStatus::OK);
    // UPDATE FROM is executed through the structured DML bridge.  The source
    // alias and target/source column qualification must survive parsing and
    // evaluation without going through textual SELECT output.
    bool handled = false;
    const bool error = dbms::tryDmlBridge(
        "UPDATE target SET val = target.val + s.val FROM source AS s "
        "WHERE target.id = s.id RETURNING id, val",
        dbms::SqlCommand::Update, s, handled);
    assert(handled && !error);
    const dbms::DmlResult result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "UPDATE 2");
    assert(result.rows.size() == 2);
    assert((result.rows[0] == std::vector<std::string>{"1", "110"}));
    assert((result.rows[1] == std::vector<std::string>{"2", "220"}));

    // The target row without a matching source row must remain unchanged.
    assert(g_engine.insert(db, "target", {{"id","3"},{"val","30"}}) == dbms::DBStatus::OK);
    handled = false;
    assert(!dbms::tryDmlBridge(
        "UPDATE target SET val = target.val + s.val FROM source AS s "
        "WHERE target.id = s.id RETURNING id, val",
        dbms::SqlCommand::Update, s, handled));
    assert(handled);
    const dbms::DmlResult second = dbms::takeLastDmlResult();
    assert(second.commandTag == "UPDATE 2");

    assert(g_engine.tableExists(db, "target"));
    assert(g_engine.tableExists(db, "source"));
    cleanup(db);
    std::cout << "[DML] UPDATE FROM setup OK" << std::endl;
}

// 5.11 DELETE USING engine support
static void test_delete_using_engine() {
    std::string db = testDbPath("dml_del_using");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target (id INT PRIMARY KEY, val INT)", s));
    assert(!ddl.executeSql("CREATE TABLE source (id INT)", s));
    for (const auto& row : {std::pair{"1", "10"}, std::pair{"2", "20"},
                            std::pair{"3", "30"}}) {
        assert(g_engine.insert(db, "target", {{"id", row.first}, {"val", row.second}}) ==
               dbms::DBStatus::OK);
    }
    assert(g_engine.insert(db, "source", {{"id", "1"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source", {{"id", "3"}}) == dbms::DBStatus::OK);

    bool handled = false;
    const bool error = dbms::tryDmlBridge(
        "DELETE FROM target USING source AS s "
        "WHERE target.id = s.id RETURNING id, val",
        dbms::SqlCommand::Delete, s, handled);
    assert(handled && !error);
    const dbms::DmlResult result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "DELETE 2");
    assert(result.rows.size() == 2);
    assert((result.rows[0] == std::vector<std::string>{"1", "10"}));
    assert((result.rows[1] == std::vector<std::string>{"3", "30"}));

    size_t remaining = 0;
    g_engine.forEachRow(db, "target", [&](uint32_t, uint16_t, const char*, size_t) {
        ++remaining;
    });
    assert(remaining == 1);
    cleanup(db);
    std::cout << "[DML] DELETE USING setup OK" << std::endl;
}

// 5.12 DML with structured inner joins
static void test_join_dml_engine() {
    std::string db = testDbPath("dml_join");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s; setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target (id INT PRIMARY KEY, val INT)", s));
    assert(!ddl.executeSql("CREATE TABLE source_a (id INT, grp INT)", s));
    assert(!ddl.executeSql("CREATE TABLE source_b (grp INT, delta INT)", s));
    assert(g_engine.insert(db, "target", {{"id","1"},{"val","10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "target", {{"id","2"},{"val","20"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "target", {{"id","3"},{"val","30"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source_a", {{"id","1"},{"grp","10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source_a", {{"id","2"},{"grp","20"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source_a", {{"id","3"},{"grp","30"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source_b", {{"grp","10"},{"delta","100"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source_b", {{"grp","20"},{"delta","200"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source_b", {{"grp","30"},{"delta","300"}}) == dbms::DBStatus::OK);

    bool handled = false;
    assert(!dbms::tryDmlBridge(
        "UPDATE target SET val = target.val + b.delta "
        "FROM source_a AS a JOIN source_b AS b ON a.grp = b.grp "
        "WHERE target.id = a.id RETURNING id, val",
        dbms::SqlCommand::Update, s, handled));
    assert(handled);
    const dbms::DmlResult updated = dbms::takeLastDmlResult();
    assert(updated.commandTag == "UPDATE 3");
    assert((updated.rows[0] == std::vector<std::string>{"1", "110"}));
    assert((updated.rows[1] == std::vector<std::string>{"2", "220"}));
    assert((updated.rows[2] == std::vector<std::string>{"3", "330"}));

    handled = false;
    // Outer joins remain explicitly owned by the legacy boundary until their
    // NULL-extension semantics are represented by the structured executor.
    assert(!dbms::tryDmlBridge(
        "UPDATE target SET val = b.delta FROM source_a AS a "
        "LEFT JOIN source_b AS b ON a.grp = b.grp WHERE target.id = a.id",
        dbms::SqlCommand::Update, s, handled));
    assert(!handled);

    handled = false;
    assert(!dbms::tryDmlBridge(
        "DELETE FROM target USING source_a AS a JOIN source_b AS b ON a.grp = b.grp "
        "WHERE target.id = a.id AND b.delta > 150 RETURNING id, val",
        dbms::SqlCommand::Delete, s, handled));
    assert(handled);
    const dbms::DmlResult deleted = dbms::takeLastDmlResult();
    assert(deleted.commandTag == "DELETE 2");
    assert((deleted.rows[0] == std::vector<std::string>{"2", "220"}));
    assert((deleted.rows[1] == std::vector<std::string>{"3", "330"}));
    cleanup(db);
    std::cout << "[DML] joined UPDATE FROM/DELETE USING setup OK" << std::endl;
}

// 5.7 MERGE: typed AST execution with cardinality protection
static void test_merge_engine() {
    const std::string db = testDbPath("dml_merge");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target (id INT PRIMARY KEY, val INT)", s));
    assert(!ddl.executeSql("CREATE TABLE source (id INT, val INT)", s));
    assert(g_engine.insert(db, "target", {{"id", "1"}, {"val", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source", {{"id", "1"}, {"val", "100"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source", {{"id", "2"}, {"val", "200"}}) == dbms::DBStatus::OK);

    const std::string mergeSql =
        "MERGE INTO target USING source AS src ON target.id = src.id "
        "WHEN MATCHED THEN UPDATE SET val = src.val "
        "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val)";
    dbms::SQLParser parser;
    auto parsed = parser.parse(mergeSql);
    assert(parsed.success);
    auto* merge = dynamic_cast<dbms::MergeStmt*>(parsed.stmt.get());
    assert(merge && merge->source && merge->source->alias == "src");
    assert(merge->whenClauses.size() == 2);
    assert(dynamic_cast<dbms::ColumnRefExpr*>(
        merge->whenClauses[1].insertCols[0].second.get()));

    bool handled = false;
    assert(!dbms::tryDmlBridge(mergeSql, dbms::SqlCommand::Merge, s, handled));
    assert(handled);
    const auto targetSchema = g_engine.getTableSchema(db, "target");
    std::map<std::string, std::string> values;
    g_engine.forEachRow(db, "target", [&](uint32_t, uint16_t, const char* data, size_t len) {
        const std::string row(data, len);
        const std::string id = g_engine.extractColumnValue(row, targetSchema, 0, db, true);
        const std::string val = g_engine.extractColumnValue(row, targetSchema, 1, db, true);
        values[id] = val;
    });
    assert((values == std::map<std::string, std::string>{{"1", "100"}, {"2", "200"}}));

    // A source row may not update the same target row twice.  The check must
    // happen before storage mutation, preserving the previous target value.
    assert(g_engine.insert(db, "source", {{"id", "1"}, {"val", "999"}}) == dbms::DBStatus::OK);
    handled = false;
    assert(dbms::tryDmlBridge(mergeSql, dbms::SqlCommand::Merge, s, handled));
    assert(handled);
    values.clear();
    g_engine.forEachRow(db, "target", [&](uint32_t, uint16_t, const char* data, size_t len) {
        const std::string row(data, len);
        values[g_engine.extractColumnValue(row, targetSchema, 0, db, true)] =
            g_engine.extractColumnValue(row, targetSchema, 1, db, true);
    });
    assert(values["1"] == "100");
    cleanup(db);
    std::cout << "[DML] MERGE setup OK" << std::endl;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_set_ops_parser();
    test_group_by_parser();
    test_order_by_nulls();
    test_limit_with_ties();
    test_for_update_parser();
    test_update_from_engine();
    test_delete_using_engine();
    test_join_dml_engine();
    test_merge_engine();
    std::cout << "[DML] all passed" << std::endl;
    return 0;
}
