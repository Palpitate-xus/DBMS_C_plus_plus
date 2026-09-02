#include "BPTree.h"
#include "Config.h"
#include "TableManage.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>

dbms::Config g_config;

using namespace dbms;

int main() {
    const std::string dbname = "expression_index_db";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");

    {
        StorageEngine engine;
        assert(engine.createDatabase(dbname) == DBStatus::OK);

        TableSchema table;
        table.tablename = "people";
        table.formatVersion = 2;
        table.append(makeIntColumn("id", false, 4, true));
        table.append(makeVarCharColumn("name", false, 12000, false));
        assert(engine.createTable(dbname, table) == DBStatus::OK);

        assert(engine.createIndex(dbname, "people", "name", true, {}, "",
                                  "UPPER(name)") == DBStatus::OK);
        assert(engine.getIndexedColumns(dbname, "people").empty());
        assert(engine.createIndex(dbname, "people", "name", true, {}, "",
                                  "LENGTH(name)") == DBStatus::INVALID_VALUE);
        assert(engine.createIndex(dbname, "people", "name", true, {}, "",
                                  "UPPER(name) junk") == DBStatus::INVALID_VALUE);

        BPTree* index =
            engine.getSecondaryIndex(dbname, "people", "UPPER(name)");
        assert(index != nullptr);
        assert(engine.insert(dbname, "people", {{"id", "1"}, {"name", "Alice"}})
               == DBStatus::OK);
        assert(engine.insert(dbname, "people", {{"id", "2"}, {"name", "Bob"}})
               == DBStatus::OK);
        assert(index->searchMulti("ALICE").size() == 1);
        assert(index->searchMulti("BOB").size() == 1);
        assert(engine.query(dbname, "people", {"=UPPER(name) ALICE"}, {"id"}).size()
               == 1);

        assert(engine.update(dbname, "people", {{"name", "Carol"}}, {"=id 1"})
               == DBStatus::OK);
        assert(index->searchMulti("ALICE").empty());
        assert(index->searchMulti("CAROL").size() == 1);

        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        assert(engine.update(dbname, "people", {{"name", "Dave"}}, {"=id 1"})
               == DBStatus::OK);
        assert(index->searchMulti("DAVE").size() == 1);
        assert(engine.rollbackTransaction() == DBStatus::OK);
        assert(index->searchMulti("CAROL").size() == 1);
        assert(index->searchMulti("DAVE").empty());

        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        assert(engine.savepoint("before_expression_update") == DBStatus::OK);
        assert(engine.update(dbname, "people", {{"name", "Eve"}}, {"=id 1"})
               == DBStatus::OK);
        assert(engine.rollbackToSavepoint("before_expression_update") == DBStatus::OK);
        assert(engine.commitTransaction() == DBStatus::OK);
        assert(index->searchMulti("CAROL").size() == 1);
        assert(index->searchMulti("EVE").empty());

        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        assert(engine.insert(dbname, "people", {{"id", "3"}, {"name", "Frank"}})
               == DBStatus::OK);
        assert(index->searchMulti("FRANK").size() == 1);
        assert(engine.rollbackTransaction() == DBStatus::OK);
        assert(index->searchMulti("FRANK").empty());

        assert(engine.beginTransaction(dbname) == DBStatus::OK);
        assert(engine.remove(dbname, "people", {"=id 2"}) == DBStatus::OK);
        assert(index->searchMulti("BOB").empty());
        assert(engine.rollbackTransaction() == DBStatus::OK);
        assert(index->searchMulti("BOB").size() == 1);
        assert(engine.remove(dbname, "people", {"=id 2"}) == DBStatus::OK);
        assert(index->searchMulti("BOB").empty());

        assert(engine.reindex(dbname, "people") == DBStatus::OK);
        index = engine.getSecondaryIndex(dbname, "people", "UPPER(name)");
        assert(index != nullptr);
        assert(index->searchMulti("CAROL").size() == 1);
        std::cout << "[EXPRESSION INDEX] DML + rollback + reindex OK\n";
    }

    {
        StorageEngine reopened;
        BPTree* index =
            reopened.getSecondaryIndex(dbname, "people", "UPPER(name)");
        assert(index != nullptr);
        assert(index->searchMulti("CAROL").size() == 1);
        assert(reopened.insert(dbname, "people",
                               {{"id", "4"}, {"name", "Grace"}}) ==
               DBStatus::OK);
        assert(index->searchMulti("GRACE").size() == 1);
        std::cout << "[EXPRESSION INDEX] restart maintenance OK\n";
    }

    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::filesystem::remove_all(".txnid");
    std::cout << "[EXPRESSION INDEX] all passed\n";
    return 0;
}
