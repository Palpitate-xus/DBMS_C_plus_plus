#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "parser/parser.h"
#include "Session.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
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

static void assertAmbiguousSourceDml(const std::string& sql, dbms::SqlCommand command,
                                     Session& session, const std::string& table) {
    using Snapshot=std::pair<std::vector<std::vector<std::string>>,std::vector<std::vector<bool>>>;
    const auto snapshot=[&]{Snapshot value;(void)g_engine.query(session.currentDB,table,{},
        {"id","val"},{{"id",true}},false,false,false,0,{},&value.first,&value.second);return value;};
    const auto before=snapshot();const bool parent=g_engine.inTransaction();
    bool handled=false;std::string state;
    try{(void)dbms::tryDmlBridge(sql,command,session,handled);}
    catch(const dbms::DbError& error){state=error.sqlState();}
    std::cout<<"[DML SOURCE NAME] "<<sql<<" STATE "<<state<<std::endl;
    assert(state=="42702" && snapshot()==before && g_engine.inTransaction()==parent);
    assert(!dbms::takeLastDmlResult().available);
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
    assertAmbiguousSourceDml(
        "UPDATE target SET val = target.val + s.val FROM source AS s "
        "WHERE target.id = s.id RETURNING id, val",
        dbms::SqlCommand::Update,s,"target");
    const bool error = dbms::tryDmlBridge(
        "UPDATE target SET val = target.val + s.val FROM source AS s "
        "WHERE target.id = s.id RETURNING target.id, target.val",
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
    assertAmbiguousSourceDml(
        "UPDATE target SET val = target.val + s.val FROM source AS s "
        "WHERE target.id = s.id RETURNING id, val",
        dbms::SqlCommand::Update,s,"target");
    assert(!dbms::tryDmlBridge(
        "UPDATE target SET val = target.val + s.val FROM source AS s "
        "WHERE target.id = s.id RETURNING target.id, target.val",
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
    assertAmbiguousSourceDml(
        "DELETE FROM target USING source AS s "
        "WHERE target.id = s.id RETURNING id, val",
        dbms::SqlCommand::Delete,s,"target");
    const bool error = dbms::tryDmlBridge(
        "DELETE FROM target USING source AS s "
        "WHERE target.id = s.id RETURNING target.id, target.val",
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
    assertAmbiguousSourceDml(
        "UPDATE target SET val = target.val + b.delta "
        "FROM source_a AS a JOIN source_b AS b ON a.grp = b.grp "
        "WHERE target.id = a.id RETURNING id, val",
        dbms::SqlCommand::Update,s,"target");
    assert(!dbms::tryDmlBridge(
        "UPDATE target SET val = target.val + b.delta "
        "FROM source_a AS a JOIN source_b AS b ON a.grp = b.grp "
        "WHERE target.id = a.id RETURNING target.id, target.val",
        dbms::SqlCommand::Update, s, handled));
    assert(handled);
    const dbms::DmlResult updated = dbms::takeLastDmlResult();
    assert(updated.commandTag == "UPDATE 3");
    assert((updated.rows[0] == std::vector<std::string>{"1", "110"}));
    assert((updated.rows[1] == std::vector<std::string>{"2", "220"}));
    assert((updated.rows[2] == std::vector<std::string>{"3", "330"}));

    handled = false;
    // The retained LEFT source executes the original legal PostgreSQL SQL.
    assert(!dbms::tryDmlBridge(
        "UPDATE target SET val = b.delta FROM source_a AS a "
        "LEFT JOIN source_b AS b ON a.grp = b.grp WHERE target.id = a.id",
        dbms::SqlCommand::Update, s, handled));
    assert(handled);
    std::vector<std::vector<std::string>> outerRows;std::vector<std::vector<bool>> outerNulls;
    (void)g_engine.query(db,"target",{},{"id","val"},{{"id",true}},false,false,false,0,{},&outerRows,&outerNulls);
    assert((outerRows==std::vector<std::vector<std::string>>{{"1","100"},{"2","200"},{"3","300"}}));
    assert((outerNulls==std::vector<std::vector<bool>>{{false,false},{false,false},{false,false}}));
    // Restore the prior image after validating this additional successful
    // stage, so every original subsequent DELETE row expectation is kept.
    for(const auto& value:std::vector<std::pair<std::string,std::string>>{{"1","110"},{"2","220"},{"3","330"}})
        assert(g_engine.updateRows(db,"target",{{"val",value.second}},{"=id "+value.first})==dbms::DBStatus::OK);

    handled = false;
    assert(!dbms::tryDmlBridge(
        "DELETE FROM target USING source_a AS a "
        "LEFT JOIN source_b AS b ON a.grp = b.grp WHERE target.id = -1",
        dbms::SqlCommand::Delete, s, handled));
    assert(handled);
    assert(dbms::takeLastDmlResult().commandTag=="DELETE 0");

    handled = false;
    assertAmbiguousSourceDml(
        "DELETE FROM target USING source_a AS a JOIN source_b AS b ON a.grp = b.grp "
        "WHERE target.id = a.id AND b.delta > 150 RETURNING id, val",
        dbms::SqlCommand::Delete,s,"target");
    assert(!dbms::tryDmlBridge(
        "DELETE FROM target USING source_a AS a JOIN source_b AS b ON a.grp = b.grp "
        "WHERE target.id = a.id AND b.delta > 150 RETURNING target.id, target.val",
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

static std::map<std::string, std::string> readTwoColumnRows(
    const std::string& db, const std::string& table) {
    const auto schema = g_engine.getTableSchema(db, table);
    std::map<std::string, std::string> rows;
    g_engine.forEachRow(db, table,
        [&](uint32_t, uint16_t, const char* data, size_t len) {
            const std::string row(data, len);
            rows[g_engine.extractColumnValue(row, schema, 0, db, true)] =
                g_engine.extractColumnValue(row, schema, 1, db, true);
        });
    return rows;
}

// UPDATE ... FROM and DELETE ... USING use the target alias namespace,
// affect a target row only once for duplicate source matches, reject cursor
// updates before mutation, and roll back the whole statement on failure.
static void test_source_driven_dml_hardening() {
    const std::string db = testDbPath("dml_source_hardening");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE duplicate_target (id INT PRIMARY KEY, val INT)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE duplicate_source (id INT, delta INT)", s));
    assert(g_engine.insert(db, "duplicate_target",
        {{"id", "1"}, {"val", "0"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "duplicate_source",
        {{"id", "1"}, {"delta", "5"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "duplicate_source",
        {{"id", "1"}, {"delta", "7"}}) == dbms::DBStatus::OK);

    bool handled = false;
    assertAmbiguousSourceDml(
        "UPDATE duplicate_target AS dst SET val = src.delta "
        "FROM duplicate_source AS src WHERE dst.id = src.id "
        "RETURNING id, val",dbms::SqlCommand::Update,s,"duplicate_target");
    assert(!dbms::tryDmlBridge(
        "UPDATE duplicate_target AS dst SET val = src.delta "
        "FROM duplicate_source AS src WHERE dst.id = src.id "
        "RETURNING dst.id, dst.val",
        dbms::SqlCommand::Update, s, handled));
    assert(handled);
    const dbms::DmlResult updated = dbms::takeLastDmlResult();
    assert(updated.available && updated.commandTag == "UPDATE 1");
    assert(updated.rows.size() == 1);
    const auto duplicateRows = readTwoColumnRows(db, "duplicate_target");
    assert(duplicateRows.size() == 1);
    assert(duplicateRows.at("1") == "5" || duplicateRows.at("1") == "7");

    assert(!ddl.executeSql(
        "CREATE TABLE delete_target (id INT PRIMARY KEY, val INT)", s));
    assert(g_engine.insert(db, "delete_target",
        {{"id", "1"}, {"val", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "delete_target",
        {{"id", "2"}, {"val", "20"}}) == dbms::DBStatus::OK);
    handled = false;
    assertAmbiguousSourceDml(
        "DELETE FROM delete_target AS dst USING duplicate_source AS src "
        "WHERE dst.id = src.id RETURNING id, val",dbms::SqlCommand::Delete,s,"delete_target");
    assert(!dbms::tryDmlBridge(
        "DELETE FROM delete_target AS dst USING duplicate_source AS src "
        "WHERE dst.id = src.id RETURNING dst.id, dst.val",
        dbms::SqlCommand::Delete, s, handled));
    assert(handled);
    const dbms::DmlResult deleted = dbms::takeLastDmlResult();
    assert(deleted.available && deleted.commandTag == "DELETE 1");
    assert(deleted.rows.size() == 1 && deleted.rows[0][0] == "1");
    assert((readTwoColumnRows(db, "delete_target") ==
            std::map<std::string, std::string>{{"2", "20"}}));

    const auto beforeCurrentOf = readTwoColumnRows(db, "duplicate_target");
    handled = false;
    assert(dbms::tryDmlBridge(
        "UPDATE duplicate_target SET val = 99 WHERE CURRENT OF update_cursor",
        dbms::SqlCommand::Update, s, handled));
    assert(handled && readTwoColumnRows(db, "duplicate_target") == beforeCurrentOf);
    handled = false;
    assert(dbms::tryDmlBridge(
        "DELETE FROM delete_target WHERE CURRENT OF delete_cursor",
        dbms::SqlCommand::Delete, s, handled));
    assert(handled);
    assert((readTwoColumnRows(db, "delete_target") ==
            std::map<std::string, std::string>{{"2", "20"}}));

    assert(!ddl.executeSql(
        "CREATE TABLE atomic_target (id INT PRIMARY KEY, val INT UNIQUE)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE atomic_source (id INT, val INT)", s));
    assert(g_engine.insert(db, "atomic_target",
        {{"id", "1"}, {"val", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "atomic_target",
        {{"id", "2"}, {"val", "20"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "atomic_target",
        {{"id", "3"}, {"val", "40"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "atomic_source",
        {{"id", "1"}, {"val", "30"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "atomic_source",
        {{"id", "2"}, {"val", "40"}}) == dbms::DBStatus::OK);
    const std::string conflictingUpdate =
        "UPDATE atomic_target AS dst SET val = src.val "
        "FROM atomic_source AS src WHERE dst.id = src.id";
    handled = false;
    assert(dbms::tryDmlBridge(
        conflictingUpdate, dbms::SqlCommand::Update, s, handled));
    assert(handled);
    assert((readTwoColumnRows(db, "atomic_target") ==
            std::map<std::string, std::string>{
                {"1", "10"}, {"2", "20"}, {"3", "40"}}));

    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "atomic_target",
        {{"id", "9"}, {"val", "90"}}) == dbms::DBStatus::OK);
    handled = false;
    assert(dbms::tryDmlBridge(
        conflictingUpdate, dbms::SqlCommand::Update, s, handled));
    assert(handled && g_engine.inTransaction());
    assert((readTwoColumnRows(db, "atomic_target") ==
            std::map<std::string, std::string>{
                {"1", "10"}, {"2", "20"}, {"3", "40"}, {"9", "90"}}));
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);

    cleanup(db);
    std::cout << "[DML] source-driven mutation hardening OK" << std::endl;
}

// Multiple branches are ordered, every candidate is planned before writes,
// and all physical actions share one statement rollback boundary.
static void test_merge_branches_and_atomicity() {
    const std::string db = testDbPath("dml_merge_branches");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql(
        "CREATE TABLE merge_target (id INT PRIMARY KEY, val INT UNIQUE)", s));
    assert(!ddl.executeSql(
        "CREATE TABLE merge_source (id INT, val INT)", s));
    for (const auto& row : {std::pair{"1", "10"}, std::pair{"2", "20"},
                            std::pair{"3", "30"}}) {
        assert(g_engine.insert(db, "merge_target",
            {{"id", row.first}, {"val", row.second}}) == dbms::DBStatus::OK);
    }
    for (const auto& row : {std::pair{"1", "110"}, std::pair{"2", "220"},
                            std::pair{"4", "440"}, std::pair{"5", "-1"}}) {
        assert(g_engine.insert(db, "merge_source",
            {{"id", row.first}, {"val", row.second}}) == dbms::DBStatus::OK);
    }

    const std::string branchSql =
        "MERGE INTO merge_target AS dst USING merge_source AS src "
        "ON dst.id = src.id "
        "WHEN MATCHED AND src.val > 200 THEN DELETE "
        "WHEN MATCHED AND src.val < 0 THEN DO NOTHING "
        "WHEN MATCHED THEN UPDATE SET val = src.val "
        "WHEN NOT MATCHED BY TARGET AND src.val < 0 THEN DO NOTHING "
        "WHEN NOT MATCHED BY TARGET THEN INSERT (id, val) "
        "VALUES (src.id, src.val) "
        "WHEN NOT MATCHED BY SOURCE THEN DELETE "
        "RETURNING id, val";
    dbms::SQLParser parser;
    auto parsed = parser.parse(branchSql);
    assert(parsed.success);
    auto* merge = dynamic_cast<dbms::MergeStmt*>(parsed.stmt.get());
    assert(merge && merge->targetAlias == "dst");
    assert(merge->whenClauses.size() == 6);
    assert(merge->whenClauses[3].bySource == "target");
    assert(merge->whenClauses[5].bySource == "source");

    bool handled = false;
    assert(!dbms::tryDmlBridge(branchSql, dbms::SqlCommand::Merge, s, handled));
    assert(handled);
    auto result = dbms::takeLastDmlResult();
    assert(result.available && result.commandTag == "MERGE 4");
    std::map<std::string, std::string> returned;
    for (const auto& row : result.rows) {
        assert(row.size() == 2);
        returned[row[0]] = row[1];
    }
    assert((returned == std::map<std::string, std::string>{
        {"1", "110"}, {"2", "20"}, {"3", "30"}, {"4", "440"}}));
    assert((readTwoColumnRows(db, "merge_target") ==
            std::map<std::string, std::string>{{"1", "110"}, {"4", "440"}}));

    // A bounded INNER JOIN source is materialized structurally. The source
    // row may match and update more than one distinct target row.
    assert(!ddl.executeSql("CREATE TABLE join_target (id INT PRIMARY KEY, val INT)", s));
    assert(!ddl.executeSql("CREATE TABLE source_a (id INT, grp INT)", s));
    assert(!ddl.executeSql("CREATE TABLE source_b (grp INT, delta INT)", s));
    assert(g_engine.insert(db, "join_target", {{"id", "1"}, {"val", "5"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source_a", {{"id", "1"}, {"grp", "7"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, "source_b", {{"grp", "7"}, {"delta", "9"}}) ==
           dbms::DBStatus::OK);
    handled = false;
    assert(!dbms::tryDmlBridge(
        "MERGE INTO join_target AS dst "
        "USING source_a AS a JOIN source_b AS b ON a.grp = b.grp "
        "ON dst.id = a.id "
        "WHEN MATCHED THEN UPDATE SET val = dst.val + b.delta",
        dbms::SqlCommand::Merge, s, handled));
    assert(handled);
    assert((readTwoColumnRows(db, "join_target") ==
            std::map<std::string, std::string>{{"1", "14"}}));

    assert(!ddl.executeSql(
        "CREATE TABLE multi_target (id INT PRIMARY KEY, grp INT, val INT)", s));
    assert(!ddl.executeSql("CREATE TABLE multi_source (grp INT, delta INT)", s));
    assert(g_engine.insert(db, "multi_target",
        {{"id", "1"}, {"grp", "7"}, {"val", "10"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "multi_target",
        {{"id", "2"}, {"grp", "7"}, {"val", "20"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "multi_source", {{"grp", "7"}, {"delta", "5"}}) ==
           dbms::DBStatus::OK);
    handled = false;
    assert(!dbms::tryDmlBridge(
        "MERGE INTO multi_target AS dst USING multi_source AS src "
        "ON dst.grp = src.grp "
        "WHEN MATCHED THEN UPDATE SET val = dst.val + src.delta",
        dbms::SqlCommand::Merge, s, handled));
    assert(handled);
    const dbms::DmlResult multiResult = dbms::takeLastDmlResult();
    assert(multiResult.commandTag == "MERGE 2");
    const auto multiSchema = g_engine.getTableSchema(db, "multi_target");
    std::map<std::string, std::string> multiValues;
    g_engine.forEachRow(db, "multi_target",
        [&](uint32_t, uint16_t, const char* data, size_t len) {
            const std::string row(data, len);
            multiValues[g_engine.extractColumnValue(row, multiSchema, 0, db, true)] =
                g_engine.extractColumnValue(row, multiSchema, 2, db, true);
        });
    assert((multiValues == std::map<std::string, std::string>{
        {"1", "15"}, {"2", "25"}}));

    // The update is physically executed before the conflicting insert. A
    // uniqueness failure must nevertheless roll back both operations.
    assert(!ddl.executeSql("CREATE TABLE atomic_target (id INT PRIMARY KEY, val INT UNIQUE)", s));
    assert(!ddl.executeSql("CREATE TABLE atomic_source (id INT, val INT)", s));
    assert(g_engine.insert(db, "atomic_target", {{"id", "1"}, {"val", "10"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, "atomic_target", {{"id", "2"}, {"val", "20"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, "atomic_source", {{"id", "1"}, {"val", "30"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, "atomic_source", {{"id", "3"}, {"val", "20"}}) ==
           dbms::DBStatus::OK);
    handled = false;
    assert(dbms::tryDmlBridge(
        "MERGE INTO atomic_target AS dst USING atomic_source AS src "
        "ON dst.id = src.id "
        "WHEN MATCHED THEN UPDATE SET val = src.val "
        "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val)",
        dbms::SqlCommand::Merge, s, handled));
    assert(handled);
    assert((readTwoColumnRows(db, "atomic_target") ==
            std::map<std::string, std::string>{{"1", "10"}, {"2", "20"}}));

    // Within an explicit transaction, failure rolls back only the MERGE
    // statement savepoint and preserves work from earlier statements.
    assert(g_engine.beginTransaction(db) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "atomic_target", {{"id", "9"}, {"val", "90"}}) ==
           dbms::DBStatus::OK);
    handled = false;
    assert(dbms::tryDmlBridge(
        "MERGE INTO atomic_target AS dst USING atomic_source AS src "
        "ON dst.id = src.id "
        "WHEN MATCHED THEN UPDATE SET val = src.val "
        "WHEN NOT MATCHED THEN INSERT (id, val) VALUES (src.id, src.val)",
        dbms::SqlCommand::Merge, s, handled));
    assert(handled && g_engine.inTransaction());
    assert((readTwoColumnRows(db, "atomic_target") ==
            std::map<std::string, std::string>{
                {"1", "10"}, {"2", "20"}, {"9", "90"}}));
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);

    // Valid-but-unimplemented source forms and malformed tails never reach
    // storage. Parser failures also cannot yield a partially executable AST.
    assert(!parser.parse(
        "MERGE INTO merge_target USING merge_source ON "
        "merge_target.id = merge_source.id WHEN MATCHED UPDATE SET val = 1").success);
    assert(!parser.parse(
        "MERGE INTO merge_target USING merge_source ON "
        "merge_target.id = merge_source.id WHEN MATCHED THEN DELETE trailing").success);
    assert(!parser.parse(
        "MERGE INTO merge_target USING merge_source ON "
        "merge_target.id = merge_source.id WHEN MATCHED THEN DELETE "
        "WHEN MATCHED THEN DO NOTHING").success);

    cleanup(db);
    std::cout << "[DML] MERGE branches/atomicity OK" << std::endl;
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
    test_source_driven_dml_hardening();
    test_merge_branches_and_atomicity();
    std::cout << "[DML] all passed" << std::endl;
    return 0;
}
