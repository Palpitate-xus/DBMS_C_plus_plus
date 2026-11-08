#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <iostream>
#include <spawn.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

extern dbms::StorageEngine g_engine;
extern char** environ;
using namespace dbms;

namespace {
bool require(bool condition, const char* message) {
    if (!condition) std::cerr << "ACTIVE_EXIT_FAIL=" << message << '\n';
    return condition;
}

int runChild(const std::string& executable, const char* mode, const char* isolation) {
    char* args[] = {const_cast<char*>(executable.c_str()), const_cast<char*>(mode),
                    const_cast<char*>(isolation), nullptr};
    pid_t pid = -1;
    const int error = posix_spawn(&pid, executable.c_str(), nullptr, nullptr, args, environ);
    if (error != 0) {
        std::cerr << "ACTIVE_EXIT_SPAWN_ERROR=" << error << '\n';
        return 255;
    }
    int status = 0;
    while (waitpid(pid, &status, 0) == -1) {
        if (errno != EINTR) return 255;
    }
    if (WIFEXITED(status)) {
        std::cout << "ACTIVE_EXIT_CHILD=" << mode << ':' << isolation
                  << " EXIT=" << WEXITSTATUS(status) << std::endl;
        return WEXITSTATUS(status);
    }
    if (WIFSIGNALED(status)) {
        std::cout << "ACTIVE_EXIT_CHILD=" << mode << ':' << isolation
                  << " SIGNAL=" << WTERMSIG(status) << std::endl;
        return 128 + WTERMSIG(status);
    }
    return 255;
}

int leaveActive(const std::string& name, bool serializable) {
    cleanupTestDb(name);
    const auto db = testDbPath(name);
    if (!require(g_engine.createDatabase(db, "utf8") == DBStatus::OK, "create database")) return 1;
    TableSchema table;
    table.tablename = "items";
    table.formatVersion = DATA_FILE_FORMAT_VERSION;
    table.append(makeIntColumn("id", false, 2, true));
    if (!require(g_engine.createTable(db, table) == DBStatus::OK, "create table") ||
        !require(g_engine.insert(db, "items", {{"id", "1"}}) == DBStatus::OK, "commit prefix") ||
        !require(g_engine.checkpoint(db), "checkpoint committed prefix") ||
        !require(g_engine.setIsolationLevel(serializable ? IsolationLevel::SERIALIZABLE : IsolationLevel::READ_COMMITTED), "isolation") ||
        !require(g_engine.beginTransaction(db) == DBStatus::OK, "begin transaction") ||
        !require(g_engine.query(db, "items", {}, {"id"}) == std::vector<std::string>{"1 "}, "real transactional read") ||
        !require(g_engine.insert(db, "items", {{"id", "2"}}) == DBStatus::OK, "insert uncommitted row") ||
        !require(g_engine.inTransaction() && g_engine.currentTxnId() != 0, "active transaction owner")) return 1;
    std::cout << "ACTIVE_EXIT_PREPARED=" << (serializable ? "serializable" : "read_committed") << std::endl;
    // Return normally with an actual active transaction: no signal or worker
    // wake-order assertion. The global engine is destroyed by normal exit.
    return 0;
}

int verifyRecovered(const std::string& name) {
    const auto db = testDbPath(name);
    if (!require(g_engine.databaseExists(db), "database after restart")) return 1;
    const auto rows = g_engine.query(db, "items", {}, {"id"});
    if (!require(rows == std::vector<std::string>{"1 "}, "committed prefix only after restart")) {
        for (const auto& row : rows) std::cerr << "ACTIVE_EXIT_OBSERVED_ROW=" << row << '\n';
        return 1;
    }
    if (!require(g_engine.beginTransaction(db) == DBStatus::OK, "begin post-recovery transaction") ||
        !require(g_engine.insert(db, "items", {{"id", "3"}}) == DBStatus::OK, "post-recovery insert") ||
        !require(g_engine.commitTransaction() == DBStatus::OK, "post-recovery commit")) return 1;
    auto committed = g_engine.query(db, "items", {}, {"id"});
    std::sort(committed.begin(), committed.end());
    if (!require(committed == std::vector<std::string>({"1 ", "3 "}), "subsequent committed write visible")) return 1;
    std::cout << "ACTIVE_EXIT_RECOVERED=" << name << " ROWS=1 POST_COMMIT_ROWS=2" << std::endl;
    return 0;
}
}

int main(int argc, char** argv) {
    TypeRegistry::instance().bootstrap();
    g_engine.setBackgroundIntervals(60000, 60000);
    if (argc == 3) {
        const std::string mode = argv[1];
        const std::string isolation = argv[2];
        if (isolation != "ordinary" && isolation != "serializable") return 2;
        const auto name = "active_exit_" + isolation;
        if (mode == "leave") return leaveActive(name, isolation == "serializable");
        if (mode == "verify") return verifyRecovered(name);
        return 2;
    }
    if (argc != 1) return 2;
    const auto self = std::filesystem::canonical(argv[0]).string();
    bool ok = true;
    for (const char* isolation : {"ordinary", "serializable"}) {
        const auto leave = runChild(self, "leave", isolation);
        if (leave != 0) { ok = false; continue; }
        if (runChild(self, "verify", isolation) != 0) ok = false;
    }
    std::cout << "ACTIVE_TRANSACTION_EXIT_CHECKED=2 FAILED=" << (ok ? 0 : 1) << std::endl;
    return ok ? 0 : 1;
}
