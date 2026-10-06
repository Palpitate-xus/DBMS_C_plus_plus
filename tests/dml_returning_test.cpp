#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "catalog/type_registry.h"
#include "parser/parser.h"
#include "Session.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>

extern dbms::StorageEngine g_engine;

static void cleanup(const std::string& db) {
    if (std::filesystem::exists(db)) std::filesystem::remove_all(db);
}

static bool runDml(const std::string& sql, Session& session) {
    bool handled = false;
    const auto command = dbms::SQLParser::classify(sql);
    const bool error = dbms::tryDmlBridge(
        sql, command, session, handled, sql);
    assert(handled);
    return error;
}

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();

    const std::string db = testDbPath("dml_returning");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session session;
    session.username = "testuser";
    session.permission = 1;
    session.currentDB = db;

    dbms::TableSchema table;
    table.tablename = "ret";
    dbms::Column id;
    id.dataName = "id";
    id.dataType = "integer";
    id.dsize = 4;
    table.append(id);
    dbms::Column name;
    name.dataName = "name";
    name.dataType = "varchar";
    name.isVariableLength = true;
    name.dsize = 32;
    table.append(name);
    assert(g_engine.createTable(db, table) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "ret", {{"id", "1"}, {"name", "before"}}) == dbms::DBStatus::OK);

    dbms::TableSchema source = table;
    source.tablename = "src";
    dbms::TableSchema copy = table;
    copy.tablename = "copy";
    assert(g_engine.createTable(db, source) == dbms::DBStatus::OK);
    assert(g_engine.createTable(db, copy) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "src", {{"id", "1"}, {"name", "one"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "src", {{"id", "2"}, {"name", "two"}}) == dbms::DBStatus::OK);

    dbms::DmlResult result;
    dbms::SQLParser parser;
    auto parsedInsertSelect = parser.parse(
        "INSERT INTO copy (id, name) SELECT id, name FROM src WHERE id >= 2 RETURNING id, name");
    assert(parsedInsertSelect.success);
    auto* parsedInsert = dynamic_cast<dbms::InsertStmt*>(parsedInsertSelect.stmt.get());
    assert(parsedInsert && parsedInsert->selectSource);
    auto* parsedSelect = dynamic_cast<dbms::SelectStmt*>(parsedInsert->selectSource.get());
    assert(parsedSelect && parsedSelect->whereClause);
    assert(!runDml("INSERT INTO copy (id, name) SELECT id, name FROM src "
                   "WHERE id >= 2 RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert((result.rows[0] == std::vector<std::string>{"2", "two"}));

    assert(!runDml("INSERT INTO copy (id, name) SELECT id + 10, name FROM src "
                   "WHERE id = 1 RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert((result.rows[0] == std::vector<std::string>{"11", "one"}));

    dbms::TableSchema conflict = table;
    conflict.tablename = "conflict_t";
    conflict.cols[0].isPrimaryKey = true;
    conflict.cols[1].isUnique = true;
    assert(g_engine.createTable(db, conflict) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "conflict_t", {{"id", "1"}, {"name", "old"}}) == dbms::DBStatus::OK);
    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'ignored'), (2, 'new') "
                   "ON CONFLICT DO NOTHING RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert(result.rows.size() == 1);
    assert((result.rows[0] == std::vector<std::string>{"2", "new"}));
    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'ignored-again') "
                   "ON CONFLICT DO NOTHING RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 0");
    assert(result.rows.empty());

    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'updated'), (3, 'third') "
                   "ON CONFLICT (id) DO UPDATE SET name = 'upserted' "
                   "RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 2");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"1", "upserted"}, {"3", "third"}}));

    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'excluded-update'), (4, 'four') "
                   "ON CONFLICT (id) DO UPDATE SET name = excluded.name "
                   "RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 2");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"1", "excluded-update"}, {"4", "four"}}));

    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'expression'), (5, 'five') "
                   "ON CONFLICT (id) DO UPDATE SET name = excluded.name || '-x' "
                   "RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 2");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"1", "expression-x"}, {"5", "five"}}));

    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'where-update'), (6, 'six') "
                   "ON CONFLICT (id) DO UPDATE SET name = excluded.name "
                   "WHERE conflict_t.name = 'expression-x' RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 2");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"1", "where-update"}, {"6", "six"}}));

    assert(!runDml("INSERT INTO conflict_t VALUES (1, 'skipped'), (7, 'seven') "
                   "ON CONFLICT (id) DO UPDATE SET name = excluded.name "
                   "WHERE conflict_t.name = 'does-not-match' RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"7", "seven"}}));

    dbms::TableSchema compositeConflict = table;
    compositeConflict.tablename = "composite_conflict";
    compositeConflict.uniqueConstraints = {{0, 1}};
    compositeConflict.uniqueConstraintNames = {"composite_conflict_key"};
    assert(g_engine.createTable(db, compositeConflict) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "composite_conflict",
                           {{"id", "1"}, {"name", "old"}}) == dbms::DBStatus::OK);
    assert(!runDml("INSERT INTO composite_conflict VALUES (1, 'old') "
                   "ON CONFLICT (name, id) DO UPDATE SET name = 'updated' "
                   "WHERE composite_conflict.name = 'old' "
                   "RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"1", "updated"}}));

    assert(!runDml("INSERT INTO composite_conflict VALUES (1, 'updated') "
                   "ON CONFLICT (id, name) DO NOTHING RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 0");
    assert(result.rows.empty());

    // A target-specific DO NOTHING must not hide a duplicate from another
    // unique constraint.
    assert(runDml("INSERT INTO conflict_t VALUES (8, 'four') "
                  "ON CONFLICT (id) DO NOTHING", session));
    assert(!runDml("INSERT INTO conflict_t VALUES (8, 'four') "
                   "ON CONFLICT (name) DO NOTHING RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 0");
    assert(result.rows.empty());

    // A nullable component of a composite UNIQUE key is not a conflict with
    // another NULL-containing key. The direct storage API uses an explicit
    // NULL marker so a real empty string remains an ordinary value.
    dbms::TableSchema nullableUnique = table;
    nullableUnique.tablename = "nullable_unique";
    nullableUnique.cols[0].isNull = true;
    nullableUnique.cols[1].isNull = true;
    nullableUnique.uniqueConstraints = {{0, 1}};
    assert(g_engine.createTable(db, nullableUnique) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "nullable_unique",
                           {{"id", "1"}, {"name", "NULL"}}) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, "nullable_unique",
                           {{"id", "1"}, {"name", "NULL"}}) == dbms::DBStatus::OK);

    assert(!runDml("INSERT INTO ret VALUES (3, 'inserted') RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert((result.rows[0] == std::vector<std::string>{"3", "inserted"}));
    assert(!runDml("INSERT INTO ret VALUES (4, 'expr') "
                   "RETURNING id + 10 AS next_id, name || '-x' AS tagged", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "INSERT 0 1");
    assert((result.columns == std::vector<std::string>{"next_id", "tagged"}));
    assert((result.rows[0] == std::vector<std::string>{"14", "expr-x"}));

    assert(!runDml("UPDATE ret SET id = id + 10, name = ret.name || '-row' "
                   "WHERE id = 1 RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "UPDATE 1");
    assert((result.rows[0] == std::vector<std::string>{"11", "before-row"}));

    // The predicate column changes. A post-update query using the old WHERE
    // clause would return nothing; storage-boundary capture must return id=2.
    assert(!runDml("UPDATE ret SET id = 2, name = 'after' WHERE id = 11 RETURNING id, name", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "UPDATE 1");
    assert((result.columns == std::vector<std::string>{"id", "name"}));
    assert(result.rows.size() == 1);
    assert((result.rows[0] == std::vector<std::string>{"2", "after"}));

    assert(!runDml("UPDATE ret SET name = 'after2' WHERE id = 2 "
                   "RETURNING id + 10 AS next_id, name || '-x' AS tagged", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "UPDATE 1");
    assert((result.rows[0] == std::vector<std::string>{"12", "after2-x"}));

    assert(!runDml("DELETE FROM ret WHERE id = 2 RETURNING *", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "DELETE 1");
    assert((result.columns == std::vector<std::string>{"id", "name"}));
    assert((result.rows[0] == std::vector<std::string>{"2", "after2"}));
    assert(!runDml("DELETE FROM ret WHERE id = 3", session));
    assert(!runDml("DELETE FROM ret WHERE id = 4 "
                   "RETURNING id * 2 AS doubled, name || '-deleted' AS tagged", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert(result.commandTag == "DELETE 1");
    assert((result.rows[0] == std::vector<std::string>{"8", "expr-deleted"}));
    assert(g_engine.query(db, "ret", {}, {}, {}).empty());

    // RETURNING must distinguish a stored empty string from physical NULL
    // for direct projections and expression evaluation on every DML kind.
    dbms::TableSchema emptyReturning;
    emptyReturning.tablename = "empty_returning";
    dbms::Column emptyId = id;
    emptyId.isPrimaryKey = true;
    emptyId.isNull = false;
    emptyReturning.append(emptyId);
    dbms::Column requiredText = name;
    requiredText.dataName = "required_text";
    requiredText.isNull = false;
    emptyReturning.append(requiredText);
    dbms::Column optionalText = name;
    optionalText.dataName = "optional_text";
    optionalText.isNull = true;
    emptyReturning.append(optionalText);
    assert(g_engine.createTable(db, emptyReturning) == dbms::DBStatus::OK);

    assert(!runDml(
        "INSERT INTO empty_returning VALUES (1, '', '') "
        "RETURNING required_text, optional_text, "
        "required_text || '-x' AS tagged", session));
    result = dbms::takeLastDmlResult();
    assert(result.available);
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"", "", "-x"}}));

    assert(!runDml(
        "INSERT INTO empty_returning VALUES (2, 'value', NULL) "
        "RETURNING required_text, optional_text, "
        "optional_text || '-x' AS tagged", session));
    result = dbms::takeLastDmlResult();
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"value", "NULL", "NULL"}}));

    assert(!runDml(
        "UPDATE empty_returning SET required_text = '', optional_text = '' "
        "WHERE id = 2 RETURNING required_text, optional_text, "
        "optional_text || '-x' AS tagged", session));
    result = dbms::takeLastDmlResult();
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"", "", "-x"}}));

    assert(!runDml(
        "DELETE FROM empty_returning WHERE id = 1 "
        "RETURNING required_text, optional_text", session));
    result = dbms::takeLastDmlResult();
    assert((result.rows == std::vector<std::vector<std::string>>{{"", ""}}));

    // PostgreSQL 18 exposes both tuple versions in RETURNING. Missing tuple
    // versions are SQL NULL (OLD for a plain INSERT, NEW for DELETE), while
    // unqualified target columns retain the statement's normal row image.
    assert(!runDml(
        "INSERT INTO empty_returning VALUES (3, 'created', 'optional') "
        "RETURNING old.id AS old_id, new.id AS new_id, "
        "old.required_text IS NULL AS old_is_null, required_text", session));
    result = dbms::takeLastDmlResult();
    assert(result.commandTag == "INSERT 0 1");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"NULL", "3", "t", "created"}}));
    assert((result.nulls == std::vector<std::vector<bool>>{
        {true, false, false, false}}));

    assert(!runDml(
        "UPDATE empty_returning SET required_text = 'changed' WHERE id = 3 "
        "RETURNING old.required_text AS before, "
        "new.required_text AS after, required_text AS current, "
        "old.id + new.id AS id_sum", session));
    result = dbms::takeLastDmlResult();
    assert(result.commandTag == "UPDATE 1");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"created", "changed", "changed", "6"}}));
    assert((result.nulls == std::vector<std::vector<bool>>{
        {false, false, false, false}}));

    assert(!runDml(
        "UPDATE empty_returning SET optional_text = 'renamed' WHERE id = 3 "
        "RETURNING WITH (OLD AS before_row, NEW AS after_row) "
        "before_row.required_text, after_row.optional_text", session));
    result = dbms::takeLastDmlResult();
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"changed", "renamed"}}));

    // Renaming OLD hides its default name. The typed executor owns the new
    // syntax and must fail closed before changing the row.
    bool hiddenOldRejected = false;
    try {
        (void)runDml(
            "UPDATE empty_returning SET required_text = 'must-not-stick' "
            "WHERE id = 3 RETURNING WITH (OLD AS before_row) old.id", session);
    } catch (const dbms::DbError& error) {
        hiddenOldRejected = error.sqlState() == "42P01";
    }
    assert(hiddenOldRejected);
    assert(!runDml(
        "UPDATE empty_returning SET required_text = required_text WHERE id = 3 "
        "RETURNING required_text", session));
    result = dbms::takeLastDmlResult();
    assert((result.rows == std::vector<std::vector<std::string>>{{"changed"}}));

    assert(!runDml(
        "DELETE FROM empty_returning WHERE id = 3 "
        "RETURNING old.id AS old_id, new.id AS new_id, "
        "new.optional_text IS NULL AS new_is_null", session));
    result = dbms::takeLastDmlResult();
    assert(result.commandTag == "DELETE 1");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"3", "NULL", "t"}}));
    assert((result.nulls == std::vector<std::vector<bool>>{
        {false, true, false}}));

    // ON CONFLICT DO UPDATE is the INSERT case where OLD is populated.
    assert(!runDml(
        "INSERT INTO conflict_t VALUES (1, 'old-new') "
        "ON CONFLICT (id) DO UPDATE SET name = excluded.name "
        "RETURNING old.name AS before, new.name AS after", session));
    result = dbms::takeLastDmlResult();
    assert(result.commandTag == "INSERT 0 1");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"where-update", "old-new"}}));

    dbms::TableSchema mergeTarget = table;
    mergeTarget.tablename = "merge_returning_target";
    mergeTarget.cols[0].isPrimaryKey = true;
    dbms::TableSchema mergeSource = table;
    mergeSource.tablename = "merge_returning_source";
    assert(g_engine.createTable(db, mergeTarget) == dbms::DBStatus::OK);
    assert(g_engine.createTable(db, mergeSource) == dbms::DBStatus::OK);
    assert(g_engine.insert(db, mergeTarget.tablename,
                           {{"id", "1"}, {"name", "target-old"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, mergeSource.tablename,
                           {{"id", "1"}, {"name", "source-new"}}) ==
           dbms::DBStatus::OK);
    assert(g_engine.insert(db, mergeSource.tablename,
                           {{"id", "2"}, {"name", "source-insert"}}) ==
           dbms::DBStatus::OK);
    assert(!runDml(
        "MERGE INTO merge_returning_target AS dst "
        "USING merge_returning_source AS src ON dst.id = src.id "
        "WHEN MATCHED THEN UPDATE SET name = src.name "
        "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (src.id, src.name) "
        "RETURNING old.name AS before, new.name AS after, merge_action() AS action",
        session));
    result = dbms::takeLastDmlResult();
    assert(result.commandTag == "MERGE 2");
    assert((result.rows == std::vector<std::vector<std::string>>{
        {"target-old", "source-new", "UPDATE"},
        {"NULL", "source-insert", "INSERT"}}));
    assert((result.nulls == std::vector<std::vector<bool>>{
        {false, false, false}, {true, false, false}}));

    cleanup(db);
    std::cout << "[DML-RETURNING] INSERT SELECT and storage-boundary RETURNING OK" << std::endl;
    return 0;
}
