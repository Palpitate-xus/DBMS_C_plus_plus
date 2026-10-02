#include "Session.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

static bool hasCheck(const dbms::TableSchema& table, const std::string& name) {
    for (size_t column = 0; column < table.len; ++column)
        if (table.cols[column].checkConstraintName == name) return true;
    for (const auto& check : table.additionalCheckConstraints)
        if (check.name == name) return true;
    return false;
}

int main() {
    using dbms::DBStatus;
    dbms::TypeRegistry::instance().bootstrap();
    const std::string name = "check_add_validation";
    const std::string database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = database;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE checked_rows(id INT PRIMARY KEY,value INT)", session));
    const auto* relation = g_engine.catalogService().get(database).resolveRelation("checked_rows", {"public"});
    assert(relation && relation->relchecks == 0);
    const dbms::Oid relationOid = relation->oid;
    const auto checkCount = [&]() {
        const auto* current = g_engine.catalogService().get(database).findClass(relationOid);
        assert(current && current->relname == "checked_rows");
        return current->relchecks;
    };
    assert(g_engine.insert(database, "checked_rows", {{"id", "1"}, {"value", "-1"}}) == DBStatus::OK);
    assert(g_engine.insert(database, "checked_rows", {{"id", "2"}}) == DBStatus::OK);
    assert(g_engine.alterTableAddCheckConstraint(database, "checked_rows", "positive_value", "value > 0") ==
           DBStatus::CHECK_VIOLATION);
    assert(!hasCheck(g_engine.getTableSchema(database, "checked_rows"), "positive_value"));
    assert(g_engine.query(database, "checked_rows", {}, {"id"}).size() == 2);
    assert(g_engine.alterTableAddCheckConstraint(database, "checked_rows", "invalid_value", "value >") ==
           DBStatus::INVALID_VALUE);
    assert(!hasCheck(g_engine.getTableSchema(database, "checked_rows"), "invalid_value"));
    {
        dbms::StorageEngine reloaded;
        assert(!hasCheck(reloaded.getTableSchema(database, "checked_rows"), "positive_value"));
    }
    bool preciseError = false;
    try {
        ddl.executeSql("ALTER TABLE checked_rows ADD CONSTRAINT positive_value CHECK(value > 0)", session);
    } catch (const dbms::DbError& error) {
        preciseError = error.sqlState() == "23514";
    }
    assert(preciseError);
    assert(!hasCheck(g_engine.getTableSchema(database, "checked_rows"), "positive_value"));
    assert(checkCount() == 0);
    assert(g_engine.update(database, "checked_rows", {{"value", "1"}}, {"=id 1"}) == DBStatus::OK);
    assert(!ddl.executeSql("ALTER TABLE checked_rows ADD CONSTRAINT positive_value CHECK(value > 0)", session));
    assert(hasCheck(g_engine.getTableSchema(database, "checked_rows"), "positive_value"));
    assert(checkCount() == 1);
    assert(g_engine.insert(database, "checked_rows", {{"id", "3"}}) == DBStatus::OK);
    assert(g_engine.insert(database, "checked_rows", {{"id", "4"}, {"value", "-1"}}) == DBStatus::CHECK_VIOLATION);
    assert(g_engine.update(database, "checked_rows", {{"value", "-1"}}, {"=id 1"}) == DBStatus::CHECK_VIOLATION);
    // A SQL command scope, as used by Extended Query and explicit BEGIN,
    // must not hide replacement heap tuples from the next ALTER action.
    assert(g_engine.beginTransaction(database, true) == DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    const auto statementSnapshot = *g_engine.getCurrentReadView();
    assert(!ddl.executeSql("ALTER TABLE checked_rows ADD COLUMN left_value INT, ADD COLUMN right_value INT", session));
    const auto* afterActions = g_engine.getCurrentReadView();
    assert(afterActions && afterActions->commandIdVisibility);
    assert(afterActions->creatorTxnId == statementSnapshot.creatorTxnId);
    assert(afterActions->lowLimitId == statementSnapshot.lowLimitId);
    assert(afterActions->activeTxnIds == statementSnapshot.activeTxnIds);
    assert(afterActions->currentCommandId == statementSnapshot.currentCommandId + 2);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.query(database, "checked_rows", {}, {"id"}).size() == 3);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(g_engine.getTableSchema(database, "checked_rows").len == 2);
    assert(g_engine.query(database, "checked_rows", {}, {"id"}).size() == 3);
    assert(g_engine.beginTransaction(database, true) == DBStatus::OK);
    assert(g_engine.savepoint("failed_add") == DBStatus::OK);
    assert(g_engine.beginSqlCommand());
    preciseError = false;
    try {
        ddl.executeSql("ALTER TABLE checked_rows ADD COLUMN hidden INT, ADD CONSTRAINT negative_value CHECK(value < 0)", session);
    } catch (const dbms::DbError& error) {
        preciseError = error.sqlState() == "23514";
    }
    assert(preciseError);
    // Verify the immediate statement restore, then the user's subsequent
    // savepoint restore. Neither may keep the rejected column or undo rows
    // that the physical image has already restored.
    assert(g_engine.getTableSchema(database, "checked_rows").len == 2);
    assert(g_engine.query(database, "checked_rows", {}, {"id"}).size() == 3);
    assert(g_engine.finishSqlCommand());
    assert(g_engine.rollbackToSavepoint("failed_add") == DBStatus::OK);
    assert(g_engine.getTableSchema(database, "checked_rows").len == 2);
    assert(g_engine.query(database, "checked_rows", {}, {"id"}).size() == 3);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    {
        dbms::StorageEngine reloaded;
        assert(hasCheck(reloaded.getTableSchema(database, "checked_rows"), "positive_value"));
        assert(reloaded.insert(database, "checked_rows", {{"id", "5"}, {"value", "-1"}}) == DBStatus::CHECK_VIOLATION);
    }
    assert(dbms::sqlstateForDBStatus(DBStatus::CHECK_VIOLATION) == "23514");
    cleanupTestDb(name);
    finalCleanupTestData();
    std::cout << "[CHECK ADD VALIDATION] passed\n";
}
