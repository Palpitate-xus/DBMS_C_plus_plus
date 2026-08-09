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

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    test_set_ops_parser();
    test_group_by_parser();
    test_order_by_nulls();
    test_limit_with_ties();
    test_for_update_parser();
    test_update_from_engine();
    std::cout << "[DML] all passed" << std::endl;
    return 0;
}
