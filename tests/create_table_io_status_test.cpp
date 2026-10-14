#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <string>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
extern dbms::StorageEngine g_engine;

namespace {
enum class Fault { None, Identity, Schema, HeapOpen, TableList };
thread_local Fault armed = Fault::None;
thread_local bool identitySync = false;
thread_local struct stat directoryOwner {};
thread_local std::string faultDatabase, faultTable;
thread_local unsigned hits = 0;

dbms::TableSchema table(const std::string& name) {
    dbms::TableSchema result;
    result.tablename = name;
    result.append(dbms::makeIntColumn("id", false, 4, true));
    return result;
}

std::string readBytes(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input);
    std::string result((std::istreambuf_iterator<char>(input)), {});
    assert(!input.bad());
    return result;
}

void writeBytes(const fs::path& path, const std::string& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    assert(output);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    output.close();
    assert(output);
}

void encodeCounter(std::string& bytes, uint64_t value) {
    assert(bytes.size() == 24);
    std::memcpy(bytes.data() + 8, &value, sizeof(value));
    uint64_t checksum = 1469598103934665603ULL;
    for (size_t i = 0; i < 16; ++i) {
        checksum ^= static_cast<unsigned char>(bytes[i]);
        checksum *= 1099511628211ULL;
    }
    std::memcpy(bytes.data() + 16, &checksum, sizeof(checksum));
}
} // namespace

// These hooks live only in this standalone test binary. They select the real
// CREATE path after namespace validation, and otherwise use real syscalls.
// The directory-sync failure belongs to this thread and exact directory inode.
extern "C" int rename(const char* from, const char* to) noexcept {
    if (armed == Fault::TableList &&
        std::string(to) == faultDatabase + "/tlist.lst") {
        armed = Fault::None;
        ++hits;
        errno = EIO;
        return -1;
    }
    const int status = static_cast<int>(
        ::syscall(SYS_renameat, AT_FDCWD, from, AT_FDCWD, to));
    if (status == 0 && armed != Fault::None &&
        std::string(to) == faultDatabase + "/.physical_relation_id_highwater") {
        if (armed == Fault::Identity) {
            assert(::stat(faultDatabase.c_str(), &directoryOwner) == 0);
            identitySync = true;
        } else if (armed == Fault::Schema || armed == Fault::HeapOpen) {
            const auto blocked = faultDatabase + "/" + faultTable +
                (armed == Fault::Schema ? ".stc" : ".dt");
            assert(::syscall(SYS_mkdirat, AT_FDCWD, blocked.c_str(), 0700) == 0);
            armed = Fault::None;
            ++hits;
        }
    }
    return status;
}

extern "C" int fsync(int fd) {
    if (identitySync) {
        struct stat owner {};
        assert(::fstat(fd, &owner) == 0 && S_ISDIR(owner.st_mode) &&
               owner.st_dev == directoryOwner.st_dev && owner.st_ino == directoryOwner.st_ino);
        identitySync = false;
        armed = Fault::None;
        ++hits;
        errno = EIO;
        return -1;
    }
    return static_cast<int>(::syscall(SYS_fsync, fd));
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("create_io_status");
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    assert(g_engine.createTable(database, table("survivor")) == dbms::DBStatus::OK);
    assert(g_engine.insert(database, "survivor", {{"id", "17"}}) == dbms::DBStatus::OK);
    const auto originalNames = g_engine.getTableNames(database);
    const auto originalRows = g_engine.query(database, "survivor", {}, {"id"});
    assert(originalRows == std::vector<std::string>{"17 "});

    auto unchanged = [&] {
        assert(g_engine.getTableNames(database) == originalNames);
        assert(!g_engine.tableExists(database, "candidate"));
        assert(g_engine.query(database, "survivor", {}, {"id"}) == originalRows);
        for (const auto& entry : fs::directory_iterator(database))
            assert(entry.path().filename().string().find("candidate") != 0);
    };
    auto retry = [&] {
        unchanged();
        // A genuinely new engine reloads the real catalog and old rows before
        // the same-name retry. This is not a simulated catalog/cache check.
        {
            dbms::StorageEngine reader;
            assert(reader.getTableNames(database) == originalNames);
            assert(!reader.tableExists(database, "candidate"));
            assert(reader.query(database, "survivor", {}, {"id"}) == originalRows);
        }
        assert(g_engine.createTable(database, table("candidate")) == dbms::DBStatus::OK);
        assert(g_engine.insert(database, "candidate", {{"id", "29"}}) == dbms::DBStatus::OK);
        assert(g_engine.query(database, "candidate", {}, {"id"}) == std::vector<std::string>{"29 "});
        assert(g_engine.dropTable(database, "candidate") == dbms::DBStatus::OK);
        unchanged();
    };

    for (const auto fault : {Fault::Identity, Fault::Schema, Fault::HeapOpen, Fault::TableList}) {
        faultDatabase = database;
        faultTable = "candidate";
        hits = 0;
        armed = fault;
        std::string message;
        const auto status = g_engine.createTable(database, table(faultTable), &message);
        assert(hits == 1 && armed == Fault::None && !identitySync);
        std::cout << "[CREATE actual I/O] fault=" << static_cast<int>(fault)
                  << " status=" << static_cast<int>(status)
                  << " SQLSTATE=" << dbms::sqlstateForDBStatus(status)
                  << " cause=" << message << std::endl;
        assert(status == dbms::DBStatus::IO_ERROR);
        assert(std::string(dbms::sqlstateForDBStatus(status)) == "58030");
        const char* expected = fault == Fault::Identity ? "could not allocate durable physical relation identity"
            : fault == Fault::Schema ? "could not create table schema"
            : fault == Fault::HeapOpen ? "could not initialize table heap" : "could not update table list";
        assert(message == expected);
        retry();
    }

    // Durable counter contents are data, not I/O errors or SQL argument errors.
    const fs::path highwater = fs::path(database) / ".physical_relation_id_highwater";
    const auto originalCounter = readBytes(highwater);
    for (const bool exhausted : {false, true}) {
        auto counter = originalCounter;
        if (exhausted) encodeCounter(counter, std::numeric_limits<uint64_t>::max());
        else counter[0] ^= 1;
        writeBytes(highwater, counter);
        std::string message;
        const auto status = g_engine.createTable(database, table("candidate"), &message);
        const auto expected = exhausted ? dbms::DBStatus::INVALID_VALUE : dbms::DBStatus::CORRUPTED_DATA;
        std::cout << "[CREATE counter control] exhausted=" << exhausted
                  << " status=" << static_cast<int>(status)
                  << " SQLSTATE=" << dbms::sqlstateForDBStatus(status) << std::endl;
        assert(status == expected);
        assert(std::string(dbms::sqlstateForDBStatus(status)) == (exhausted ? "22023" : "XX001"));
        assert(message == "could not allocate durable physical relation identity");
        assert(readBytes(highwater) == counter);
        unchanged();
        writeBytes(highwater, originalCounter);
        retry();
    }

    auto invalid = table("semantic_reject");
    dbms::ForeignKey foreign;
    foreign.colNames = {"id"};
    foreign.refTable = "survivor";
    foreign.refCols = {"id"};
    foreign.onDelete = "definitely-not-an-action";
    invalid.appendFK(foreign);
    std::string reason;
    assert(g_engine.createTable(database, invalid, &reason) == dbms::DBStatus::INVALID_ARGUMENT);
    std::cerr << "[CREATE semantic control] action reason=" << reason << std::endl;
    assert(reason == "unsupported foreign key referential action");
    invalid.tablename = "semantic_missing_column";
    invalid.fks[0].onDelete = "restrict";
    invalid.fks[0].colNames = {"missing"};
    assert(g_engine.createTable(database, invalid, &reason) == dbms::DBStatus::INVALID_VALUE);
    assert(reason == "foreign key local column does not exist: missing");
    assert(g_engine.createTable(database, table("survivor")) == dbms::DBStatus::TABLE_ALREADY_EXISTS);
    unchanged();
    assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    std::cout << "[CREATE typed native I/O and semantic controls] complete\n";
}
