// test_link_flags: -ldl

#include "access/IndexFileUtil.h"
#include "commands/TableManage.h"
#include "common/DataDirectory.h"
#include "common/sha256.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <dlfcn.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <spawn.h>
#include <stdexcept>
#include <string>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

extern char** environ;

namespace {
namespace fs = std::filesystem;
using namespace dbms;

thread_local struct stat interruptedCatalog {};
thread_local bool interruptCatalogUnlink = false;
thread_local unsigned interruptedUnlinks = 0;

bool failCatalogUnlink(int parent, const char* file) noexcept {
    if (!interruptCatalogUnlink) return false;
    struct stat entry {};
    if (::fstatat(parent, file, &entry, AT_SYMLINK_NOFOLLOW) != 0 ||
        entry.st_dev != interruptedCatalog.st_dev || entry.st_ino != interruptedCatalog.st_ino)
        return false;
    interruptCatalogUnlink = false;
    ++interruptedUnlinks;
    errno = EIO;
    return true;
}
}  // namespace

// Only the actual transaction-owned catalog image selected by the private
// fixture can fail. Old cleanup uses remove_all, while new cleanup unlinks
// the verified files individually.
extern "C" int unlinkat(int directory, const char* file, int flags) noexcept {
    if (failCatalogUnlink(directory, file)) return -1;
    return static_cast<int>(::syscall(SYS_unlinkat, directory, file, flags));
}

extern "C" int remove(const char* file) noexcept {
    if (failCatalogUnlink(AT_FDCWD, file)) return -1;
    using Remove = int (*)(const char*);
    static const auto realRemove = reinterpret_cast<Remove>(::dlsym(RTLD_NEXT, "remove"));
    if (!realRemove) std::abort();
    return realRemove(file);
}

namespace {
void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

TableSchema table(const std::string& name) {
    TableSchema result;
    result.tablename = name;
    result.append(makeIntColumn("id", false, 4));
    return result;
}

std::string read(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    require(bool(input), "open sentinel");
    const std::string bytes{std::istreambuf_iterator<char>(input), {}};
    require(!input.bad(), "read sentinel");
    return bytes;
}

struct Attempt {
    DBStatus create;
    DBStatus comment;
    DBStatus index;
};

Attempt attemptFromPeer(StorageEngine& peer, const std::string& database,
                        const std::string& name, const std::string& text,
                        const std::string& indexTable) {
    std::promise<Attempt> promise;
    auto future = promise.get_future();
    std::thread worker([&] {
        try {
            const auto created = peer.createTable(database, table(name));
            const auto commented = peer.commentOnTable(database, "existing", text);
            const auto indexed = peer.createIndex(database, indexTable, "id");
            promise.set_value({created, commented, indexed});
        } catch (...) {
            promise.set_exception(std::current_exception());
        }
    });
    if (future.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
        std::cerr << "[CREATE UNDO OWNERSHIP] competing metadata write hung\n";
        std::abort();
    }
    worker.join();
    return future.get();
}

void checkUnrelatedDirectory(const std::string& database,
                             const std::string& directory) {
    const auto owned = fs::path(database) / directory;
    require(fs::create_directory(owned), "create unrelated directory");
    // The old implementation ignored the name 'catalog' in an incomplete
    // active journal and unconditionally removed an unmarked retired path.
    const auto sentinel = owned / "catalog";
    const std::string contents = "unrelated user data\n";
    { std::ofstream out(sentinel, std::ios::binary); out << contents; require(bool(out), "write sentinel"); }
    bool rejected = false;
    try {
        StorageEngine shouldReject;
    } catch (const std::exception&) {
        rejected = true;
    }
    require(rejected, "startup must reject unrelated journal-shaped directory");
    require(fs::is_directory(owned) && fs::is_regular_file(sentinel),
            "unrelated directory survived rejected startup");
    require(read(sentinel) == contents, "sentinel bytes unchanged");
    require(std::distance(fs::directory_iterator(owned), fs::directory_iterator{}) == 1,
            "unrelated directory entries unchanged");
    require(fs::remove(sentinel) && fs::remove(owned), "remove test-owned sentinel");
}

enum class ResumeRetirement { PartialFiles, EmptyDirectory, StandaloneProof, LegacyProof, Backup };

void checkInterruptedRetirement(const std::string& database, ResumeRetirement resume) {
    const fs::path home = database;
    const auto active = home / ".create_table_undo";
    const auto retired = home / ".create_table_undo.retired";
    const auto proof = home / ".create_table_undo.cleanup";
    const bool commit = resume != ResumeRetirement::StandaloneProof;
    std::string name;
    switch (resume) {
    case ResumeRetirement::PartialFiles: name = "interrupted_commit"; break;
    case ResumeRetirement::EmptyDirectory: name = "interrupted_empty"; break;
    case ResumeRetirement::StandaloneProof: name = "interrupted_rollback"; break;
    case ResumeRetirement::LegacyProof: name = "interrupted_legacy"; break;
    case ResumeRetirement::Backup: name = "interrupted_before_backup"; break;
    }
    std::string legacyHeader;
    {
        StorageEngine engine;
        require(engine.beginTransaction(database) == DBStatus::OK, "begin retirement test");
        require(engine.beginCreateTableUndo(database), "take retirement test ownership");
        require(engine.createTable(database, table(name)) == DBStatus::OK,
                "create retirement test table");
        engine.registerDdlUndo([&engine, database, name] {
            return engine.dropTable(database, name) == DBStatus::OK;
        });
        if (commit) {
            require(engine.insert(database, name, {{"id", "93"}}) == DBStatus::OK,
                    "insert retirement test row");
        }
        require(::stat((active / "catalog").c_str(), &interruptedCatalog) == 0,
                "identify real transaction-owned catalog image");
        if (resume == ResumeRetirement::LegacyProof) legacyHeader = read(active / "header");
        interruptedUnlinks = 0;
        interruptCatalogUnlink = true;
        const auto status = commit ? engine.commitTransaction() : engine.rollbackTransaction();
        interruptCatalogUnlink = false;
        require(status == DBStatus::OK, "cleanup failure must preserve terminal transaction status");
        require(interruptedUnlinks == 1, "fault must hit the owned catalog image once");
        require(engine.tableExists(database, name) == commit, "table matches transaction outcome");
        if (resume == ResumeRetirement::Backup) {
            require(fs::is_directory(retired) && fs::is_regular_file(proof),
                    "retirement was interrupted before the next transaction");
            require(engine.beginTransaction(database, true) == DBStatus::OK,
                    "begin next transaction under the database lock");
            require(engine.createTransactionBackup(), "take the next transaction backup");
            require(!fs::exists(retired) && !fs::exists(proof),
                    "transaction backup must finish earlier retirement before copying");
            require(engine.rollbackTransaction() == DBStatus::OK, "discard the next transaction backup");
        }
    }

    // A failure in the old random parking directory would leave the image
    // outside the next startup's discovery paths.
    for (const auto& entry : fs::directory_iterator(home)) {
        const auto stem = entry.path().filename().string();
        if (stem.rfind(".create_table_undo.stage.", 0) == 0 &&
            fs::is_regular_file(entry.path() / "records" / "catalog")) {
            const auto catalog = entry.path() / "records" / "catalog";
            struct stat parked {};
            if (::stat(catalog.c_str(), &parked) == 0 &&
                parked.st_dev == interruptedCatalog.st_dev && parked.st_ino == interruptedCatalog.st_ino) {
                StorageEngine reopened;
                require(reopened.tableExists(database, name) == commit,
                        "old path still preserved the transaction outcome");
                require(fs::is_regular_file(catalog), "old path reproduced the retained catalog image");
                throw std::runtime_error("owned retirement image stranded in a temporary directory");
            }
        }
    }
    if (resume != ResumeRetirement::Backup) {
        require(!fs::exists(active) && fs::is_regular_file(proof) && fs::is_directory(retired),
                "interrupted retirement retained its independent proof and directory");
        require(fs::is_regular_file(retired / "catalog"), "catalog image survived the injected fault");
    }

    if (resume == ResumeRetirement::PartialFiles) {
        // Advance to a partial cleanup in which the header is gone, but the
        // internal and standalone proofs still identify the same records.
        const auto header = retired / "header";
        if (fs::exists(header)) require(index_file::removeDurably(header), "remove test-owned header");
        require(fs::equivalent(retired / "cleanup", proof), "directory still carries its proof");
    } else if (resume == ResumeRetirement::EmptyDirectory) {
        // Advance to the point where the original directory is empty but
        // removing the directory itself has not finished.
        std::vector<fs::path> remaining;
        for (const auto& entry : fs::directory_iterator(retired)) remaining.push_back(entry.path());
        for (const auto& member : remaining)
            require(fs::remove(member), "remove test-owned member from the retired directory");
        require(index_file::syncDirectory(retired), "sync test-owned empty directory");
    } else if (resume == ResumeRetirement::StandaloneProof) {
        // Advance to a durable retired-directory removal before the final
        // proof was unlinked.
        require(fs::remove_all(retired) != 0 && index_file::syncDirectory(home),
                "remove only the retired records belonging to the fixture");
    } else if (resume == ResumeRetirement::LegacyProof) {
        // Reproduce the older fixed-directory layout using this same
        // transaction's real header, before-image and retirement binding.
        require(index_file::removeDurably(proof) && index_file::removeDurably(retired / "cleanup"),
                "replace the fixture's modern proof with the legacy proof");
        const auto header = retired / "header";
        if (!fs::exists(header)) require(index_file::writeAtomically(header, legacyHeader),
                                         "restore the fixture's actual header");
        const auto binding = SHA256::hash(legacyHeader + clusterSystemIdentifier() + database);
        require(index_file::writeAtomically(retired / "retired", binding), "persist legacy retirement proof");
    }
    {
        StorageEngine cold;
        require(!fs::exists(proof) && !fs::exists(retired), "restart finished retirement");
        require(cold.tableExists(database, name) == commit, "restart preserved transaction outcome");
        require(cold.query(database, "peer_after_commit", {"=id 32"}, {"id"}).size() == 1,
                "restart preserved the committed peer row");
        if (commit) {
            require(cold.query(database, name, {"=id 93"}, {"id"}).size() == 1,
                    "restart preserved the just-committed row");
        }
    }
}

void worker() {
    const std::string database = "__t_create_undo_ownership";
    {
        // Both backends exist before a transaction starts. Constructing the
        // second backend later would itself perform recovery of the live xid.
        StorageEngine owner;
        StorageEngine peer;
        require(owner.createDatabase(database) == DBStatus::OK, "create database");
        require(owner.createTable(database, table("existing")) == DBStatus::OK,
                "create existing table");
        for (const auto* name : {"index_before_rollback", "index_before_commit"}) {
            require(owner.createTable(database, table(name)) == DBStatus::OK,
                    "create existing index target");
            require(owner.insert(database, name, {{"id", "42"}}) == DBStatus::OK,
                    "seed existing index target");
        }
        require(owner.commentOnTable(database, "existing", "before") == DBStatus::OK,
                "initial comment");

        for (bool commit : {false, true}) {
            const std::string mine = commit ? "owner_committed" : "owner_rolled_back";
            const std::string other = commit ? "peer_after_commit" : "peer_after_rollback";
            const std::string comment = commit ? "after commit" : "after rollback";
            const std::string indexTable = commit ? "index_before_commit" : "index_before_rollback";
            require(owner.beginTransaction(database) == DBStatus::OK, "begin transaction");
            require(owner.beginCreateTableUndo(database), "take compact CREATE ownership");
            require(owner.createTable(database, table(mine)) == DBStatus::OK,
                    "create journal-owned table");
            // SQL DdlTransaction normally installs this ownership callback.
            owner.registerDdlUndo([&owner, database, mine] {
                return owner.dropTable(database, mine) == DBStatus::OK;
            });
            require(fs::is_directory(fs::path(database) / ".create_table_undo"),
                    "compact journal exists");
            const auto namesBefore = read(fs::path(database) / "tlist.lst");
            const auto commentsBefore = read(fs::path(database) / ".comments");

            const auto blocked = attemptFromPeer(peer, database, other, "must not publish", indexTable);
            require(blocked.create == DBStatus::LOCK_CONFLICT,
                    "concurrent native CREATE must conflict");
            require(blocked.comment == DBStatus::LOCK_CONFLICT,
                    "concurrent native COMMENT must conflict");
            require(blocked.index == DBStatus::LOCK_CONFLICT,
                    "concurrent native CREATE INDEX must conflict");
            require(!fs::exists(fs::path(database) / (other + ".stc")),
                    "conflicting CREATE did not publish schema");
            require(!fs::exists(fs::path(database) / (indexTable + "_id.idx")),
                    "conflicting CREATE INDEX did not publish a file");
            require(read(fs::path(database) / "tlist.lst") == namesBefore &&
                    read(fs::path(database) / ".comments") == commentsBefore,
                    "conflicting writes left metadata unchanged");

            const auto completed = commit ? owner.commitTransaction() : owner.rollbackTransaction();
            require(completed == DBStatus::OK, "complete owner transaction");
            const auto allowed = attemptFromPeer(peer, database, other, comment, indexTable);
            require(allowed.create == DBStatus::OK && allowed.comment == DBStatus::OK &&
                    allowed.index == DBStatus::OK,
                    "peer writes succeed after ownership release");
            const auto indexes = peer.getIndexMetadata(database, indexTable);
            require(std::any_of(indexes.begin(), indexes.end(),
                                [](const auto& index) { return index.name == "id"; }),
                    "peer index metadata retained");
            require(peer.insert(database, other, {{"id", commit ? "32" : "31"}}) == DBStatus::OK,
                    "peer table accepts a row");
            require(owner.tableExists(database, mine) == commit, "owner outcome matches terminal");
            require(owner.tableExists(database, other), "peer CREATE retained");
            const auto names = owner.getTableNames(database);
            require(std::find(names.begin(), names.end(), other) != names.end(),
                    "peer CREATE remains in the relation list");
            require(owner.getTableComment(database, "existing") == comment,
                    "peer comment retained");
        }
    }
    {
        StorageEngine cold;
        require(cold.tableExists(database, "owner_committed"), "committed table survives reopen");
        require(!cold.tableExists(database, "owner_rolled_back"), "rolled-back table stays absent");
        require(cold.tableExists(database, "peer_after_rollback") &&
                cold.tableExists(database, "peer_after_commit"), "peer tables survive reopen");
        const auto names = cold.getTableNames(database);
        require(std::find(names.begin(), names.end(), "peer_after_rollback") != names.end() &&
                std::find(names.begin(), names.end(), "peer_after_commit") != names.end(),
                "peer relation list survives reopen");
        require(cold.query(database, "peer_after_rollback", {"=id 31"}, {"id"}).size() == 1 &&
                cold.query(database, "peer_after_commit", {"=id 32"}, {"id"}).size() == 1,
                "peer rows survive reopen");
        for (const auto* name : {"index_before_rollback", "index_before_commit"}) {
            const auto indexes = cold.getIndexMetadata(database, name);
            require(std::any_of(indexes.begin(), indexes.end(),
                                [](const auto& index) { return index.name == "id"; }),
                    "peer index metadata survives reopen");
            require(cold.query(database, name, {"=id 42"}, {"id"}).size() == 1,
                    "peer index target row survives reopen");
        }
        require(cold.getTableComment(database, "existing") == "after commit",
                "peer comment survives reopen");
    }
    checkInterruptedRetirement(database, ResumeRetirement::PartialFiles);
    checkInterruptedRetirement(database, ResumeRetirement::EmptyDirectory);
    checkInterruptedRetirement(database, ResumeRetirement::StandaloneProof);
    checkInterruptedRetirement(database, ResumeRetirement::LegacyProof);
    checkInterruptedRetirement(database, ResumeRetirement::Backup);
    checkUnrelatedDirectory(database, ".create_table_undo");
    checkUnrelatedDirectory(database, ".create_table_undo.retired");
    checkUnrelatedDirectory(database, ".create_table_undo.cleanup");
    {
        StorageEngine cold;
        require(cold.getTableComment(database, "existing") == "after commit",
                "healthy startup after removing test sentinels");
        require(cold.dropDatabase(database) == DBStatus::OK, "drop test database");
    }
}
}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 2 && std::string(argv[1]) == "--worker") {
            worker();
            std::cout << "[CREATE UNDO OWNERSHIP] concurrent writes, interrupted retirement and unrelated directories passed\n";
            return 0;
        }
        require(argc == 1, "unexpected arguments");
        std::string pattern = (fs::temp_directory_path() / "dbms-create-undo-ownership-XXXXXX").string();
        std::vector<char> mutablePattern(pattern.begin(), pattern.end());
        mutablePattern.push_back('\0');
        const char* created = ::mkdtemp(mutablePattern.data());
        require(created != nullptr, "create private test root");
        const fs::path root(created);
        const std::string executable = fs::canonical("/proc/self/exe").string();
        char* args[] = {const_cast<char*>(executable.c_str()), const_cast<char*>("--worker"), nullptr};
        posix_spawn_file_actions_t actions;
        require(posix_spawn_file_actions_init(&actions) == 0, "initialize worker actions");
        require(posix_spawn_file_actions_addchdir_np(&actions, root.c_str()) == 0, "set private worker cwd");
        pid_t pid = 0;
        const int spawned = posix_spawn(&pid, executable.c_str(), &actions, nullptr, args, environ);
        posix_spawn_file_actions_destroy(&actions);
        require(spawned == 0 && pid > 0, "spawn owned worker");
        int status = 0;
        pid_t waited;
        do { waited = waitpid(pid, &status, 0); } while (waited < 0 && errno == EINTR);
        const bool passed = waited == pid && WIFEXITED(status) && WEXITSTATUS(status) == 0;
        std::error_code error;
        fs::remove_all(root, error);
        require(!error, "remove private test root");
        require(passed, "private worker failed");
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "[CREATE UNDO OWNERSHIP] " << error.what() << '\n';
        return 1;
    }
}
