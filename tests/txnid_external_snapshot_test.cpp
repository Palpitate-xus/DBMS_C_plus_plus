#include "commands/TableManage.h"
#include "transaction/TxnIdGenerator.h"

#include <cassert>
#include <iostream>
#include <set>
#include <sys/wait.h>
#include <unistd.h>

using namespace dbms;
static const std::string database = "__t_txnid_external_snapshot";

static void writeRows(const std::string& mode) {
    StorageEngine engine;
    if (mode == "initial") {
        assert(engine.createDatabase(database) == DBStatus::OK);
        TableSchema table;
        table.tablename = "items";
        table.append(makeIntColumn("id", false, 4, true));
        assert(engine.createTable(database, table) == DBStatus::OK);
    }
    assert(engine.beginTransaction(database) == DBStatus::OK);
    assert(engine.insert(database, "items", {{"id", mode == "initial" ? "99" : "100"}}) == DBStatus::OK);
    assert(engine.commitTransaction() == DBStatus::OK);
    _exit(0);
}

static void externalWriter(const char* executable, const char* mode) {
    const auto child = fork();
    assert(child >= 0);
    if (child == 0) {
        execl(executable, executable, "--writer", mode, static_cast<char*>(nullptr));
        _exit(127);
    }
    int status = 0;
    assert(waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

static void rows(StorageEngine& engine, const std::set<std::string>& expected) {
    const auto result = engine.query(database, "items", {}, {"id"});
    std::cerr << "[EXTERNAL TXN SNAPSHOT] rows=" << result.size()
              << " expected=" << expected.size() << '\n';
    assert(result.size() == expected.size());
    assert(std::set<std::string>(result.begin(), result.end()) == expected);
}

int main(int argc, char** argv) {
    if (argc == 3 && std::string(argv[1]) == "--writer") writeRows(argv[2]);
    // Warm the parent singleton before a distinct process allocates/commits.
    assert(TxnIdGenerator::instance().maxCommittedTxId() == 0);
    char executable[4096];
    const auto length = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
    assert(length > 0);
    executable[length] = '\0';
    externalWriter(executable, "initial");
    {
        StorageEngine reader;
        rows(reader, {"99 "});
        reader.setIsolationLevel(IsolationLevel::REPEATABLE_READ);
        assert(reader.beginTransaction(database) == DBStatus::OK);
        rows(reader, {"99 "});
        const uint64_t oldLimit = reader.getCurrentReadView()->lowLimitId;
        externalWriter(executable, "later");
        assert(TxnIdGenerator::instance().maxCommittedTxId() >= oldLimit);
        assert(reader.getCurrentReadView()->lowLimitId == oldLimit);
        rows(reader, {"99 "});
        assert(reader.commitTransaction() == DBStatus::OK);
    }
    StorageEngine current;
    rows(current, {"99 ", "100 "});
    std::cout << "[EXTERNAL TXN SNAPSHOT] cold reader sees external commits; acquired RR boundary remains fixed\n";
}
