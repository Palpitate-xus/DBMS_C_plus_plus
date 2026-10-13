#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <iostream>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace dbms;

static int worker(const std::string& database) {
    {
        StorageEngine engine;
        assert(engine.createDatabase(database) == DBStatus::OK);
        TableSchema table;
        table.tablename = "rows";
        table.append(makeIntColumn("id", false, 2, true));
        assert(engine.createTable(database, table) == DBStatus::OK);
        assert(engine.insert(database, "rows", {{"id", "7"}}) == DBStatus::OK);
        assert(engine.beginTransaction(database, true) == DBStatus::OK);
        assert(engine.createTransactionBackup());
        engine.preserveTransactionBackupOnRollback(true);
        engine.restoreTransactionBackupBeforeRowUndo(true);
        engine.markTransactionBackupDirty();
        const fs::path image = database + ".txn_backup." + std::to_string(engine.currentTxnId());
        const fs::path saved = image.string() + ".saved";
        assert(engine.alterTableAddColumn(database, "rows", makeIntColumn("added", true, 2)) == DBStatus::OK);
        fs::rename(image, saved);
        assert(engine.rollbackTransaction() == DBStatus::IO_ERROR);
        assert(engine.inTransaction());
        // The unavailable directory is now healthy again, but leave the
        // failed graph unresolved to exercise orderly engine destruction.
        fs::rename(saved, image);
        assert(fs::is_directory(image));
    }
    std::cout << "[DDL RESTART worker] orderly incomplete-owner shutdown returned normally\n";
    return 0;
}

int main(int argc, char** argv) {
    const auto database = testDbPath("ddl_rollback_restart");
    if (argc == 2 && std::string(argv[1]) == "worker") return worker(database);
    const pid_t pid = ::fork();
    assert(pid >= 0);
    if (pid == 0) {
        ::execl(argv[0], argv[0], "worker", static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    assert(::waitpid(pid, &status, 0) == pid);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    bool retained = false;
    for (const auto& entry : fs::directory_iterator(".")) {
        if (entry.path().filename().string().rfind(database + ".txn_backup.", 0) == 0)
            retained = true;
    }
    assert(retained);
    StorageEngine restarted;
    assert(restarted.getTableSchema(database, "rows").len == 1);
    assert(restarted.query(database, "rows", {}, {"id"}) == std::vector<std::string>{"7 "});
    for (const auto& entry : fs::directory_iterator(".")) {
        assert(entry.path().filename().string().rfind(database + ".txn_backup.", 0) != 0);
    }
    assert(restarted.insert(database, "rows", {{"id", "8"}}) == DBStatus::OK);
    assert(restarted.query(database, "rows", {}, {"id"}) == (std::vector<std::string>{"7 ", "8 "}));
    assert(restarted.dropDatabase(database) == DBStatus::OK);
    std::cout << "[DDL RESTART] incomplete snapshot retained across process exit and restored before new writes\n";
}
