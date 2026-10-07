#include "commands/TableManage.h"
#include "storage/PageAllocator.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <set>
#include <sys/wait.h>
#include <unistd.h>

using namespace dbms;

static TableSchema schema(const std::string& name) {
    TableSchema result;
    result.tablename = name;
    result.append(makeIntColumn("id", false, 4, true));
    return result;
}

static void ddlSnapshot(StorageEngine& engine, const std::string& database) {
    assert(engine.beginTransaction(database) == DBStatus::OK);
    engine.preserveTransactionBackupOnRollback(true);
    assert(engine.createTransactionBackup());
    engine.restoreTransactionBackupBeforeRowUndo(true);
    engine.markTransactionBackupDirty();
}

static void writer(const std::string& scenario) {
    const std::string database = "__t_heap_wal_crash_" + scenario;
    StorageEngine engine;
    assert(engine.createDatabase(database) == DBStatus::OK);
    assert(engine.createTable(database, schema("items")) == DBStatus::OK);
    const uint64_t original = engine.getTableSchema(database, "items").physicalRelationId;
    assert(original != 0);
    assert(engine.insert(database, "items", {{"id", "99"}}) == DBStatus::OK);
    ddlSnapshot(engine, database);
    if (scenario == "drop") {
        assert(engine.dropTable(database, "items") == DBStatus::OK);
    } else {
        assert(engine.alterTableRenameTable(database, "items", "archived") == DBStatus::OK);
        assert(engine.getTableSchema(database, "archived").physicalRelationId == original);
    }
    assert(engine.createTable(database, schema("items")) == DBStatus::OK);
    assert(engine.getTableSchema(database, "items").physicalRelationId > original);
    assert(engine.insert(database, "items", {{"id", "100"}}) == DBStatus::OK);
    if (scenario == "abort") {
        assert(engine.getPageAllocator(database, "items")->flush());
        _exit(0);
    }
    assert(engine.commitTransaction() == DBStatus::OK);
    assert(engine.beginTransaction(database) == DBStatus::OK);
    assert(engine.insert(database, "items", {{"id", "101"}}) == DBStatus::OK);
    assert(engine.getPageAllocator(database, "items")->flush());
    _exit(0);
}

static void checkRows(StorageEngine& engine, const std::string& database,
                      const std::string& table, const std::set<std::string>& expected) {
    const auto rows = engine.query(database, table, {}, {"id"});
    std::cerr << "[HEAP WAL IDENTITY CRASH] " << database << '/' << table
              << " actual=" << rows.size() << " expected=" << expected.size() << '\n';
    for (const auto& row : rows) std::cerr << "actual row [" << row << "]\n";
    assert(rows.size() == expected.size());
    assert(std::set<std::string>(rows.begin(), rows.end()) == expected);
}

int main(int argc, char** argv) {
    if (argc == 3 && std::string(argv[1]) == "--writer") writer(argv[2]);
    char executable[4096];
    const auto length = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
    assert(length > 0);
    executable[length] = '\0';
    const std::vector<std::string> scenarios = argc == 2
        ? std::vector<std::string>{argv[1]} : std::vector<std::string>{"rename", "drop", "abort"};
    for (const std::string& scenario : scenarios) {
        const auto child = fork();
        assert(child >= 0);
        if (child == 0) {
            execl(executable, executable, "--writer", scenario.c_str(), static_cast<char*>(nullptr));
            _exit(127);
        }
        int status = 0;
        assert(waitpid(child, &status, 0) == child);
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        const std::string database = "__t_heap_wal_crash_" + std::string(scenario);
        StorageEngine recovered;
        assert(recovered.recoverAllDatabases());
        if (std::string(scenario) == "abort") {
            assert(!recovered.tableExists(database, "archived"));
            checkRows(recovered, database, "items", {"99 "});
        } else {
            checkRows(recovered, database, "items", {"100 "});
            if (std::string(scenario) == "rename")
                checkRows(recovered, database, "archived", {"99 "});
        }
        std::cout << "[HEAP WAL IDENTITY CRASH] " << scenario << " passed\n";
    }
}
