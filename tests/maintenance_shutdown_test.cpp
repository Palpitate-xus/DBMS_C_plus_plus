#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "test_utils.h"
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <thread>

extern dbms::StorageEngine g_engine;
namespace {
std::string database;
void lateMaintenance() {
    // The global engine is still alive here. A fresh worker owns live
    // thread-local storage while doing shutdown maintenance.
    std::thread worker([] {
        assert(g_engine.checkpoint(database));
        assert(g_engine.dropDatabase(database)==dbms::DBStatus::OK);
        std::cout<<"[MAINTENANCE SHUTDOWN] passed\n";
    });
    worker.join();
}
}
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    g_engine.setBackgroundIntervals(60000,600000);
    database=testDbPath("maintenance_shutdown");
    assert(g_engine.createDatabase(database)==DBStatus::OK);
    TableSchema table;table.tablename="rows";table.append(makeIntColumn("id",false,4,true));
    assert(g_engine.createTable(database,table)==DBStatus::OK);
    assert(g_engine.insertRow(database,"rows",{{"id","1"}})==DBStatus::OK);
    // Warm the transaction/catalog/WAL owners before registering shutdown
    // work. A new worker below gets its own live thread-local engine context.
    assert(std::atexit(lateMaintenance)==0);
    assert(g_engine.checkpoint(database));
}
