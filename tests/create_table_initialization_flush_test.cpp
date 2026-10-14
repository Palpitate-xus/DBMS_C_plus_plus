#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <cerrno>
#include <filesystem>
#include <iostream>
#include <string>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
extern dbms::StorageEngine g_engine;

namespace {
enum class Fault { None, HeapMarker, HeapData, Partition, ToastHeap, PrimaryIndex, ToastIndex, SchemaWrite };
thread_local Fault armed = Fault::None;
thread_local std::string target;
thread_local unsigned hits = 0;

std::string descriptorPath(int fd) {
    char bytes[4096];
    const auto size = ::readlink(("/proc/self/fd/" + std::to_string(fd)).c_str(), bytes, sizeof(bytes));
    return size < 0 ? "" : std::string(bytes, static_cast<size_t>(size));
}

void coldReload(const std::string& executable, const std::string& database, bool candidate) {
    const pid_t child = ::fork();
    assert(child >= 0);
    if (child == 0) {
        ::execl(executable.c_str(), executable.c_str(), "--reload", database.c_str(),
                candidate ? "success" : "failure", static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    assert(::waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

void coldPartitionWrite(const std::string& executable, const std::string& database) {
    const pid_t child = ::fork();
    assert(child >= 0);
    if (child == 0) {
        ::execl(executable.c_str(), executable.c_str(), "--write-partition", database.c_str(),
                static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    assert(::waitpid(child, &status, 0) == child);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}
} // namespace

// Real file fsync/write failures in this executable only. One-shot TLS state
// cannot be consumed by the engine's background writer or another test thread.
extern "C" int fsync(int fd) {
    const auto path = descriptorPath(fd);
    if (armed != Fault::None && armed != Fault::SchemaWrite &&
        ((armed == Fault::HeapMarker || armed == Fault::ToastHeap || armed == Fault::Partition)
             ? path.find(target + ".extent_pending.tmp.") == 0 : path == target)) {
        armed = Fault::None;
        ++hits;
        errno = EIO;
        return -1;
    }
    return static_cast<int>(::syscall(SYS_fsync, fd));
}

extern "C" ssize_t write(int fd, const void* bytes, size_t size) {
    if (armed == Fault::SchemaWrite && descriptorPath(fd) == target) {
        armed = Fault::None;
        ++hits;
        errno = EIO;
        return -1;
    }
    return static_cast<ssize_t>(::syscall(SYS_write, fd, bytes, size));
}

int main(int argc, char** argv) {
    dbms::TypeRegistry::instance().bootstrap();
    if (argc == 3 && std::string(argv[1]) == "--write-partition") {
        const auto inserted = g_engine.insert(argv[2], "candidate", {{"id", "29"}});
        std::cout << "[CREATE partition healthy native writer] status=" << static_cast<int>(inserted) << std::endl;
        assert(inserted == dbms::DBStatus::OK);
        return 0;
    }
    if (argc == 4 && std::string(argv[1]) == "--reload") {
        const std::string database = argv[2];
        const bool candidate = std::string(argv[3]) == "success";
        assert(g_engine.query(database, "survivor", {}, {"id"}) == std::vector<std::string>{"17 "});
        assert(g_engine.tableExists(database, "candidate") == candidate);
        if (candidate)
            assert(g_engine.query(database, "candidate", {}, {"id"}) == std::vector<std::string>{"29 "});
        else assert(g_engine.getTableNames(database) == std::vector<std::string>{"survivor"});
        return 0;
    }
    assert(argc == 1);
    const auto executable = fs::absolute(argv[0]).string();
    const std::string database = testDbPath("initialization_flush");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    dbms::TableSchema survivor;
    survivor.tablename = "survivor";
    survivor.append(dbms::makeIntColumn("id", false, 4, true));
    assert(g_engine.createTable(database, survivor) == dbms::DBStatus::OK);
    assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "survivor", {{"id", "17"}}) == dbms::DBStatus::OK);
    assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
    const auto originalNames = g_engine.getTableNames(database);

    auto unchanged = [&] {
        assert(g_engine.getTableNames(database) == originalNames);
        assert(!g_engine.tableExists(database, "candidate"));
        assert(g_engine.query(database, "survivor", {}, {"id"}) == std::vector<std::string>{"17 "});
        for (const auto& entry : fs::directory_iterator(database))
            assert(entry.path().filename().string().find("candidate") != 0);
    };

    for (const auto fault : {Fault::HeapMarker, Fault::HeapData, Fault::Partition, Fault::ToastHeap,
                              Fault::PrimaryIndex, Fault::ToastIndex, Fault::SchemaWrite}) {
        auto schema = survivor;
        schema.tablename = "candidate";
        const auto absolute = fs::absolute(database).string() + "/candidate";
        target = absolute + ".dt";
        if (fault == Fault::Partition) {
            schema.partitionType = dbms::TableSchema::PartitionType::List;
            schema.partitionKey = "id";
            schema.listPartitions = {{"p1", {"29"}}};
            target = absolute + "#p1.dt";
        } else if (fault == Fault::ToastHeap || fault == Fault::ToastIndex) {
            schema.append(dbms::makeVarCharColumn("payload", true, 12000, false));
            target = absolute + (fault == Fault::ToastHeap ? ".toast.dt" : ".toast.idx");
        } else if (fault == Fault::PrimaryIndex) target = absolute + ".idx";
        else if (fault == Fault::SchemaWrite) target = absolute + ".stc";
        hits = 0;
        armed = fault;
        std::string message;
        const auto status = g_engine.createTable(database, schema, &message);
        assert(hits == 1 && armed == Fault::None);
        std::cout << "[CREATE initializer actual flush failure] fault=" << static_cast<int>(fault)
                  << " status=" << static_cast<int>(status)
                  << " visible=" << g_engine.tableExists(database, "candidate")
                  << " cause=" << message << std::endl;
        assert(status == dbms::DBStatus::IO_ERROR);
        assert(std::string(dbms::sqlstateForDBStatus(status)) == "58030");
        const char* cause = fault == Fault::Partition ? "could not initialize table partition"
            : fault == Fault::ToastHeap ? "could not initialize TOAST heap"
            : fault == Fault::PrimaryIndex ? "could not initialize primary key index"
            : fault == Fault::ToastIndex ? "could not initialize TOAST index"
            : fault == Fault::SchemaWrite ? "could not write table schema" : "could not initialize table heap";
        assert(message == cause);
        unchanged();
        coldReload(executable, database, false);
        assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);
        const bool partition = schema.partitionType != dbms::TableSchema::PartitionType::None;
        if (!partition) assert(g_engine.beginTransaction(database) == dbms::DBStatus::OK);
        std::map<std::string, std::string> row{{"id", "29"}};
        if (schema.len == 2) row["payload"] = "kept";
        if (partition) {
            // The supported nontransactional partition writer uses a real
            // exec, not another live engine sharing the parent's caches.
            coldPartitionWrite(executable, database);
        } else {
            const auto inserted = g_engine.insert(database, "candidate", row);
            if (inserted != dbms::DBStatus::OK)
                std::cerr << "[CREATE initializer healthy retry insert] fault=" << static_cast<int>(fault)
                          << " status=" << static_cast<int>(inserted) << " SQLSTATE="
                          << dbms::sqlstateForDBStatus(inserted) << std::endl;
            assert(inserted == dbms::DBStatus::OK);
        }
        // Ordinary-table writes cross the actual COMMIT barrier. The legacy
        // partition owner has already closed above. Cold exec inherits no
        // engine cache or pending-marker owner from either path.
        if (!partition) assert(g_engine.commitTransaction() == dbms::DBStatus::OK);
        coldReload(executable, database, true);
        assert(g_engine.createTable(database, schema) == dbms::DBStatus::TABLE_ALREADY_EXISTS);
        assert(g_engine.query(database, "candidate", {}, {"id"}) == std::vector<std::string>{"29 "});
        assert(g_engine.dropTable(database, "candidate") == dbms::DBStatus::OK);
        unchanged();
    }
    assert(g_engine.createTable(database, survivor) == dbms::DBStatus::TABLE_ALREADY_EXISTS);
    unchanged();
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    std::cout << "[CREATE initializer checked flush, rollback, retry and cold exec controls] complete\n";
}
