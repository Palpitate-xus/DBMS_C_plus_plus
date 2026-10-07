#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    const std::string db = "__t_two_engine_row_probe";
    StorageEngine first, second;
    assert(first.createDatabase(db) == DBStatus::OK);
    TableSchema table;
    table.tablename = "items";
    table.append(makeIntColumn("id", false, 2, true));
    table.pkColIndices.push_back(0);
    assert(first.createTable(db, table) == DBStatus::OK);
    first.setIsolationLevel(IsolationLevel::SERIALIZABLE);
    second.setIsolationLevel(IsolationLevel::SERIALIZABLE);
    assert(first.beginTransaction(db) == DBStatus::OK);
    assert(second.beginTransaction(db) == DBStatus::OK);
    assert(first.query(db, "items", {"=id 99"}, {"id"}).empty());
    assert(second.query(db, "items", {"=id 100"}, {"id"}).empty());
    assert(first.insert(db, "items", {{"id", "99"}}) == DBStatus::OK);
    assert(second.insert(db, "items", {{"id", "100"}}) == DBStatus::OK);
    const auto firstCommit = first.commitTransaction();
    const auto secondCommit = second.commitTransaction();
    std::cout << "[TWO ENGINE ROW PROBE] commits=" << sqlstateForDBStatus(firstCommit)
              << "," << sqlstateForDBStatus(secondCommit) << std::endl;
    StorageEngine reader;
    const auto rows = reader.query(db, "items", {}, {"id"});
    for (const auto& row : rows) std::cout << "[TWO ENGINE ROW PROBE] row=" << row << std::endl;
    if (firstCommit != DBStatus::OK || secondCommit != DBStatus::OK) return 2;
    assert(rows.size() == 2);
}
