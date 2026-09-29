#include "TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <thread>

int main() {
    const std::string oldName = "database_rename_storage_old";
    const std::string newName = "database_rename_storage_new";
    cleanupTestDb(oldName);
    cleanupTestDb(newName);
    const std::string oldDb = testDbPath(oldName);
    const std::string newDb = testDbPath(newName);
    dbms::StorageEngine engine;
    assert(engine.createDatabase(oldDb) == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "items";
    schema.formatVersion = dbms::DATA_FILE_FORMAT_VERSION;
    schema.append(dbms::makeIntColumn("id", false, 0, true));
    assert(engine.createTable(oldDb, schema) == dbms::DBStatus::OK);
    assert(engine.insert(oldDb, "items", {{"id", "42"}}) == dbms::DBStatus::OK);

    assert(engine.beginTransaction(oldDb) == dbms::DBStatus::OK);
    assert(engine.createDatabase(oldDb) ==
           dbms::DBStatus::TABLE_ALREADY_EXISTS);
    assert(engine.renameDatabase(oldDb, newDb) == dbms::DBStatus::DATABASE_IN_USE);
    dbms::DBStatus concurrent = dbms::DBStatus::OK;
    std::thread renamer([&] { concurrent = engine.renameDatabase(oldDb, newDb); });
    renamer.join();
    assert(concurrent == dbms::DBStatus::DATABASE_IN_USE);
    assert(engine.databaseExists(oldDb));
    assert(!engine.databaseExists(newDb));
    assert(engine.rollbackTransaction() == dbms::DBStatus::OK);

    assert(engine.renameDatabase(oldDb, newDb) == dbms::DBStatus::OK);
    assert(!engine.databaseExists(oldDb));
    assert(engine.databaseExists(newDb));
    assert(engine.tableExists(newDb, "items"));
    assert(engine.renameDatabase(newDb, newDb) ==
           dbms::DBStatus::TABLE_ALREADY_EXISTS);
    assert(engine.createDatabase(oldDb) == dbms::DBStatus::OK);
    assert(engine.renameDatabase(newDb, oldDb) ==
           dbms::DBStatus::TABLE_ALREADY_EXISTS);
    assert(engine.dropDatabase(newDb) == dbms::DBStatus::OK);
    assert(engine.dropDatabase(oldDb) == dbms::DBStatus::OK);
}
