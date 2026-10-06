#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "sequence_ddl_rollback_generation";
    const std::string db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    Session session;
    session.username = "admin";
    session.permission = 1;
    session.currentDB = db;
    DdlExecutor ddl;
    const auto sql = [&](const std::string& text) { assert(!ddl.executeSql(text, session)); };
    sql("CREATE SEQUENCE durable");
    assert(g_engine.nextval(db, "durable") == 1);
    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    sql("CREATE TABLE ddl_marker(id INT)");
    // Ordinary CREATE uses catalog undo; force a real file-rewriting DDL
    // image rather than assuming its backup was marked dirty.
    sql("ALTER TABLE ddl_marker ADD COLUMN extra INT");
    assert(g_engine.transactionBackupDirty());
    assert(g_engine.nextval(db, "durable") == 2);
    assert(g_engine.savepoint("q") == DBStatus::OK);
    assert(g_engine.nextval(db, "durable") == 3);
    assert(g_engine.nextval(db, "durable") == 4);
    assert(g_engine.rollbackToSavepoint("q") == DBStatus::OK);
    assert(g_engine.currval(db, "durable") == 4);
    const auto afterSavepoint = g_engine.nextval(db, "durable");
    std::cout << "COUNTER AFTER DDL SAVEPOINT " << afterSavepoint << " expected 5" << std::endl;
    assert(afterSavepoint == 5);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(!g_engine.tableExists(db, "ddl_marker"));
    assert(g_engine.currval(db, "durable") == 5);
    assert(g_engine.nextval(db, "durable") == 6);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    sql("CREATE TABLE second_marker(id INT)");
    sql("ALTER TABLE second_marker ADD COLUMN extra INT");
    assert(g_engine.savepoint("q") == DBStatus::OK);
    assert(g_engine.setval(db, "durable", 50, true) == 50);
    assert(g_engine.rollbackToSavepoint("q") == DBStatus::OK);
    assert(g_engine.currval(db, "durable") == 50);
    assert(g_engine.nextval(db, "durable") == 51);
    assert(g_engine.setval(db, "durable", 12, false) == 12);
    assert(g_engine.rollbackToSavepoint("q") == DBStatus::OK);
    assert(g_engine.currval(db, "durable") == 51);
    assert(g_engine.nextval(db, "durable") == 12);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(g_engine.nextval(db, "durable") == 13);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.savepoint("q") == DBStatus::OK);
    assert(g_engine.nextval(db, "durable") == 14);
    sql("ALTER SEQUENCE durable RESTART WITH 100");
    assert(g_engine.nextval(db, "durable") == 100);
    assert(g_engine.rollbackToSavepoint("q") == DBStatus::OK);
    assert(g_engine.currval(db, "durable") == 100);
    assert(g_engine.nextval(db, "durable") == 15);
    assert(g_engine.rollbackTransaction() == DBStatus::OK);
    assert(g_engine.nextval(db, "durable") == 16);

    assert(g_engine.beginTransaction(db) == DBStatus::OK);
    assert(g_engine.savepoint("q") == DBStatus::OK);
    assert(g_engine.nextval(db, "durable") == 17);
    sql("DROP SEQUENCE durable");
    sql("CREATE SEQUENCE durable START 500");
    assert(g_engine.nextval(db, "durable") == 500);
    assert(g_engine.rollbackToSavepoint("q") == DBStatus::OK);
    assert(g_engine.nextval(db, "durable") == 18);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    assert(std::distance(std::filesystem::directory_iterator(db + "/.sequence_runtime"),
                         std::filesystem::directory_iterator{}) == 1);

    const std::string backup = db + "_physical_backup";
    assert(g_engine.physicalBackup(db, backup));
    assert(g_engine.nextval(db, "durable") == 19);
    assert(g_engine.nextval(db, "durable") == 20);
    assert(g_engine.physicalRestore(db, backup));
    assert(g_engine.nextval(db, "durable") == 19); // explicit restore remains exact
    std::filesystem::remove_all(backup);
    assert(g_engine.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "[SEQUENCE DDL ROLLBACK GENERATION] passed\n";
}
