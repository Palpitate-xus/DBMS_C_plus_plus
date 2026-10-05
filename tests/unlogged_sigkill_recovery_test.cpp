#include "catalog/type_registry.h"
#include "commands/TableManage.h"

#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <poll.h>
#include <signal.h>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr const char* kDatabase = "sigkill_unlogged_db";
constexpr const char* kTable = "cache";

[[noreturn]] void crashWriter(int readyFd) {
    dbms::StorageEngine engine;
    assert(engine.createDatabase(kDatabase) == dbms::DBStatus::OK);

    dbms::TableSchema table;
    table.tablename = kTable;
    table.formatVersion = 2;
    table.isUnlogged = true;
    table.append(dbms::makeIntColumn("id", false, 4, true));
    table.append(dbms::makeVarCharColumn("payload", false, 64, false));
    assert(engine.createTable(kDatabase, table) == dbms::DBStatus::OK);
    assert(engine.insert(kDatabase, kTable,
                         {{"id", "1"}, {"payload", "before SIGKILL"}}) ==
           dbms::DBStatus::OK);
    assert(engine.query(kDatabase, kTable, {}, {"id"}).size() == 1);

    const char ready = 'R';
    assert(::write(readyFd, &ready, 1) == 1);
    (void)::close(readyFd);
    for (;;) ::pause();
}

void killWriter(const std::string& executable, const fs::path& workingDirectory) {
    int readyPipe[2] = {-1, -1};
    assert(::pipe(readyPipe) == 0);
    const pid_t child = ::fork();
    assert(child >= 0);
    if (child == 0) {
        (void)::close(readyPipe[0]);
        if (::chdir(workingDirectory.c_str()) != 0) ::_exit(126);
        const std::string readyFd = std::to_string(readyPipe[1]);
        ::execl(executable.c_str(), executable.c_str(), "--crash-writer",
                readyFd.c_str(), static_cast<char*>(nullptr));
        ::_exit(127);
    }

    (void)::close(readyPipe[1]);
    struct pollfd event {readyPipe[0], POLLIN | POLLHUP, 0};
    int polled = -1;
    do {
        polled = ::poll(&event, 1, 30000);
    } while (polled < 0 && errno == EINTR);
    char ready = '\0';
    ssize_t received = -1;
    if (polled > 0) {
        do {
            received = ::read(readyPipe[0], &ready, 1);
        } while (received < 0 && errno == EINTR);
    }
    (void)::close(readyPipe[0]);
    if (received != 1 || ready != 'R') {
        (void)::kill(child, SIGKILL);
        int failedStatus = 0;
        (void)::waitpid(child, &failedStatus, 0);
        assert(false && "UNLOGGED child did not finish its write");
    }

    assert(::kill(child, SIGKILL) == 0);
    int status = 0;
    assert(::waitpid(child, &status, 0) == child);
    assert(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL);
}

size_t rowCount(dbms::StorageEngine& engine) {
    size_t count = 0;
    assert(engine.forEachRow(
        kDatabase, kTable,
        [&](uint32_t, uint16_t, const char*, size_t) { ++count; }));
    return count;
}

}  // namespace

int main(int argc, char** argv) {
    dbms::TypeRegistry::instance().bootstrap();
    if (argc == 3 && std::string(argv[1]) == "--crash-writer") {
        crashWriter(std::stoi(argv[2]));
    }

    const std::string tempTemplate =
        (fs::temp_directory_path() / "dbms_unlogged_sigkill_XXXXXX").string();
    std::vector<char> tempPath(tempTemplate.begin(), tempTemplate.end());
    tempPath.push_back('\0');
    char* createdPath = ::mkdtemp(tempPath.data());
    if (createdPath == nullptr) {
        std::perror("mkdtemp for unlogged SIGKILL test");
        return 1;
    }
    const fs::path workingDirectory(createdPath);
    killWriter(fs::absolute(argv[0]).string(), workingDirectory);

    std::ifstream lifecycle(
        workingDirectory / kDatabase / ".unlogged_lifecycle", std::ios::binary);
    const std::string lifecycleState{
        std::istreambuf_iterator<char>(lifecycle),
        std::istreambuf_iterator<char>()};
    assert(lifecycleState == "DBMS_UNLOGGED_LIFECYCLE_V1\nRUNNING\n");

    const fs::path originalDirectory = fs::current_path();
    fs::current_path(workingDirectory);
    {
        dbms::StorageEngine recovered;
        assert(recovered.getTableSchema(kDatabase, kTable).isUnlogged);
        assert(rowCount(recovered) == 0);
        const fs::path relation = fs::path(kDatabase) / (std::string(kTable) + ".dt");
        assert(fs::file_size(relation) == fs::file_size(relation.string() + ".init"));
    }
    fs::current_path(originalDirectory);
    fs::remove_all(workingDirectory);
    std::cout << "[UNLOGGED SIGKILL RECOVERY] passed\n";
    return 0;
}
