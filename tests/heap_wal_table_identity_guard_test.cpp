#include "commands/TableManage.h"
#include "access/IndexFileUtil.h"

#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sys/wait.h>
#include <unistd.h>

using namespace dbms;

static TableSchema schema(const std::string& name) {
    TableSchema result;
    result.tablename = name;
    result.append(makeIntColumn("id", false, 4, true));
    return result;
}

static std::string readBytes(const std::filesystem::path& path) {
    std::ifstream stream(path, std::ios::binary);
    assert(stream);
    return std::string(std::istreambuf_iterator<char>(stream), {});
}

static bool startupFails() {
    try {
        StorageEngine recovered;
        return false;
    } catch (const std::runtime_error&) {
        return true;
    }
}

static void run(const std::string& scenario) {
    const std::string database = "__t_heap_identity_guard";
    uint64_t firstIdentity = 0;
    {
        StorageEngine engine;
        assert(engine.createDatabase(database) == DBStatus::OK);
        assert(engine.createTable(database, schema("items")) == DBStatus::OK);
        firstIdentity = engine.getTableSchema(database, "items").physicalRelationId;
        assert(firstIdentity != 0);
        if (scenario == "missing")
            assert(engine.insert(database, "items", {{"id", "99"}}) == DBStatus::OK);
        if (scenario == "rollback") {
            assert(engine.beginTransaction(database) == DBStatus::OK);
            engine.preserveTransactionBackupOnRollback(true);
            assert(engine.createTransactionBackup());
            engine.restoreTransactionBackupBeforeRowUndo(true);
            engine.markTransactionBackupDirty();
            assert(engine.createTable(database, schema("burned")) == DBStatus::OK);
            const auto burned = engine.getTableSchema(database, "burned").physicalRelationId;
            assert(burned > firstIdentity);
            assert(engine.rollbackTransaction() == DBStatus::OK);
            assert(!engine.tableExists(database, "burned"));
            assert(engine.createTable(database, schema("later")) == DBStatus::OK);
            assert(engine.getTableSchema(database, "later").physicalRelationId > burned);
        }
    }
    const std::filesystem::path schemaPath = database + "/items.stc";
    auto bytes = readBytes(schemaPath);
    int32_t format = 0;
    std::memcpy(&format, bytes.data(), sizeof(format));
    assert(format == 0x4442000A);
    uint32_t magic = 0;
    uint64_t identity = 0;
    assert(bytes.size() >= sizeof(magic) + sizeof(identity));
    std::memcpy(&magic, bytes.data() + bytes.size() - sizeof(magic) - sizeof(identity), sizeof(magic));
    std::memcpy(&identity, bytes.data() + bytes.size() - sizeof(identity), sizeof(identity));
    assert(magic == 0x31444952 && identity == firstIdentity);
    if (scenario == "missing") {
        const auto heap = std::filesystem::path(database) / "items.dt";
        assert(std::filesystem::remove(heap));
        assert(startupFails());
        assert(!std::filesystem::exists(heap));
    } else if (scenario == "truncated") {
        bytes.resize(bytes.size() - sizeof(identity));
        assert(index_file::writeAtomically(schemaPath, bytes));
        assert(startupFails());
    } else if (scenario == "duplicate") {
        {
            StorageEngine engine;
            assert(engine.createTable(database, schema("other")) == DBStatus::OK);
        }
        const std::filesystem::path otherPath = database + "/other.stc";
        auto other = readBytes(otherPath);
        std::memcpy(other.data() + other.size() - sizeof(identity), &identity, sizeof(identity));
        assert(index_file::writeAtomically(otherPath, other));
        assert(startupFails());
    } else if (scenario == "legacy" || scenario == "legacy-reuse") {
        // Construct an explicit valid 09 schema fixture. Production does not
        // downgrade or migrate schemas. This table has no identity page WAL.
        format = 0x44420009;
        std::memcpy(bytes.data(), &format, sizeof(format));
        bytes.resize(bytes.size() - sizeof(magic) - sizeof(identity));
        assert(index_file::writeAtomically(schemaPath, bytes));
        {
            StorageEngine engine;
            assert(engine.getTableSchema(database, "items").physicalRelationId == 0);
            assert(engine.insert(database, "items", {{"id", "99"}}) == DBStatus::OK);
            if (scenario == "legacy-reuse") {
                assert(engine.dropTable(database, "items") == DBStatus::OK);
                assert(engine.createTable(database, schema("items")) == DBStatus::OK);
                assert(engine.query(database, "items", {}, {"id"}).empty());
            }
        }
        if (scenario == "legacy-reuse") assert(startupFails());
        else {
            StorageEngine recovered;
            assert(recovered.query(database, "items", {}, {"id"}) ==
                   std::vector<std::string>{"99 "});
            assert(recovered.getTableSchema(database, "items").physicalRelationId == 0);
            assert(readBytes(schemaPath) == bytes);
        }
    } else if (scenario == "counter-restore") {
        // The backed-up DB floor protects allocation when the independent
        // cluster counter is absent (e.g. copied physical backup).
        assert(std::filesystem::remove(".physical_relation_ids"));
        StorageEngine recovered;
        assert(recovered.createTable(database, schema("later")) == DBStatus::OK);
        assert(recovered.getTableSchema(database, "later").physicalRelationId > firstIdentity);
    } else {
        assert(scenario == "rollback");
        StorageEngine recovered;
        assert(recovered.tableExists(database, "later"));
        assert(!recovered.tableExists(database, "burned"));
    }
    std::cout << "[HEAP WAL IDENTITY GUARD] " << scenario << " passed\n";
}

int main(int argc, char** argv) {
    if (argc == 3 && std::string(argv[1]) == "--scenario") {
        run(argv[2]);
        return 0;
    }
    char executable[4096];
    const auto length = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
    assert(length > 0);
    executable[length] = '\0';
    for (const char* scenario : {"missing", "truncated", "duplicate", "legacy",
                                  "legacy-reuse", "counter-restore", "rollback"}) {
        assert(std::filesystem::create_directory(scenario));
        const auto child = fork();
        assert(child >= 0);
        if (child == 0) {
            assert(chdir(scenario) == 0);
            execl(executable, executable, "--scenario", scenario, static_cast<char*>(nullptr));
            _exit(127);
        }
        int status = 0;
        assert(waitpid(child, &status, 0) == child);
        assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }
}
