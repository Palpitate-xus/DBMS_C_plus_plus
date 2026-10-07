#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "access/BPTree.h"
#include "storage/PageAllocator.h"

#include <cassert>
#include <iostream>
#include <set>
#include <sys/wait.h>
#include <unistd.h>

using namespace dbms;
static const std::string database = "__t_shared_btree_crash";

static void writer() {
    StorageEngine first, second;
    assert(first.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "items";
    table.append(makeIntColumn("id", false, 2, true));
    table.pkColIndices.push_back(0);
    table.append(makeVarCharColumn("tag", false, 20));
    assert(first.createTable(database, table) == DBStatus::OK);
    assert(first.createIndex(database, "items", "tag") == DBStatus::OK);
    assert(second.getPKIndex(database, "items")->allValues().empty());
    assert(second.getSecondaryIndex(database, "items", "tag")->allValues().empty());
    assert(first.beginTransaction(database) == DBStatus::OK);
    assert(second.beginTransaction(database) == DBStatus::OK);
    assert(first.insert(database, "items", {{"id", "99"}, {"tag", "same"}}) == DBStatus::OK);
    assert(second.insert(database, "items", {{"id", "100"}, {"tag", "same"}}) == DBStatus::OK);
    assert(first.commitTransaction() == DBStatus::OK);
    assert(second.commitTransaction() == DBStatus::OK);
    assert(first.beginTransaction(database) == DBStatus::OK);
    assert(first.insert(database, "items", {{"id", "101"}, {"tag", "same"}}) == DBStatus::OK);
    assert(first.getPageAllocator(database, "items")->flush());
    assert(first.getPKIndex(database, "items")->flush());
    assert(first.getSecondaryIndex(database, "items", "tag")->flush());
    _exit(0);
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--writer") writer();
    char executable[4096];
    const auto length = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
    assert(length > 0);
    executable[length] = '\0';
    const auto child = fork();
    assert(child >= 0);
    if (child == 0) {
        execl(executable, executable, "--writer", static_cast<char*>(nullptr));
        _exit(127);
    }
    int status = 0;
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    StorageEngine recovered;
    assert(recovered.recoverAllDatabases());
    const auto rows = recovered.query(database, "items", {}, {"id"});
    std::set<std::string> actual(rows.begin(), rows.end());
    assert(actual == (std::set<std::string>{"99 ", "100 "}));
    assert(recovered.getPKIndex(database, "items")->allValues().size() == 2);
    assert(recovered.getSecondaryIndex(database, "items", "tag")->allValues().size() == 2);
    assert(recovered.getSecondaryIndex(database, "items", "tag")->searchMulti("same").size() == 2);
    std::cout << "[SHARED BTREE CRASH] both commits and exact indexes survive, uncommitted row is undone\n";
}
