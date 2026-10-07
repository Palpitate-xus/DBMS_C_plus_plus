#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "storage/PageAllocator.h"

#include <cassert>
#include <iostream>
#include <set>
#include <sys/wait.h>
#include <unistd.h>

using namespace dbms;
static const std::string database = "__t_shared_heap_engine_crash";

static void writeThenExit() {
    StorageEngine first, second;
    assert(first.createDatabase(database) == DBStatus::OK);
    TableSchema table;
    table.tablename = "items";
    table.append(makeIntColumn("id", false, 2, false));
    assert(first.createTable(database, table) == DBStatus::OK);
    assert(first.beginTransaction(database) == DBStatus::OK);
    assert(second.beginTransaction(database) == DBStatus::OK);
    assert(first.insert(database, "items", {{"id", "99"}}) == DBStatus::OK);
    assert(second.insert(database, "items", {{"id", "100"}}) == DBStatus::OK);
    assert(first.commitTransaction() == DBStatus::OK);
    assert(second.commitTransaction() == DBStatus::OK);
    assert(first.beginTransaction(database) == DBStatus::OK);
    assert(first.insert(database, "items", {{"id", "101"}}) == DBStatus::OK);
    // Force steal of the uncommitted third tuple. Recovery must undo only
    // that transaction, preserving both independently committed rows.
    assert(first.getPageAllocator(database, "items")->flush());
    _exit(0);
}

int main(int argc, char** argv) {
    if (argc == 2 && std::string(argv[1]) == "--writer") writeThenExit();
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
    std::set<std::string> values;
    for (auto row : recovered.query(database, "items", {}, {"id"})) {
        while (!row.empty() && row.back() == ' ') row.pop_back();
        std::cerr << "[SHARED HEAP CRASH] row=" << row << '\n';
        assert(values.insert(row).second);
    }
    assert(values == (std::set<std::string>{"99", "100"}));
    std::cout << "[SHARED HEAP CRASH] both commits survive and abandoned insert is undone\n";
}
