#include "access/IndexFileUtil.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

static std::string readBytes(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    assert(input);
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

static std::vector<std::string> readTableNames(const fs::path& path) {
    const std::string bytes = readBytes(path);
    assert(bytes.size() % dbms::MAX_TABLE_NAME_LEN == 0);
    std::vector<std::string> names;
    for (size_t offset = 0; offset < bytes.size();
         offset += dbms::MAX_TABLE_NAME_LEN) {
        const size_t end = bytes.find('\0', offset);
        names.push_back(bytes.substr(
            offset, (end == std::string::npos
                        ? offset + dbms::MAX_TABLE_NAME_LEN : end) - offset));
    }
    return names;
}

static dbms::TableSchema makeTable(const std::string& name) {
    dbms::TableSchema table;
    table.tablename = name;
    table.append(dbms::makeIntColumn("id", true, 2));
    return table;
}

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();

    const std::string database = testDbPath("table_list_atomicity");
    dbms::StorageEngine engine;
    assert(engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    assert(engine.createTable(database, makeTable("alpha")) ==
           dbms::DBStatus::OK);
    assert(engine.createTable(database, makeTable("remove_me")) ==
           dbms::DBStatus::OK);
    assert(engine.createTable(database, makeTable("omega")) ==
           dbms::DBStatus::OK);

    const fs::path tableList = fs::path(database) / "tlist.lst";

    // Initial heap publication performs one sync through IndexFileUtil for
    // its allocation marker (marker removal uses a separate helper).  Let
    // that through, then fail tlist.lst's parent sync after its atomic rename;
    // the writer confirms the exact bytes and retries the barrier.
    dbms::index_file::failDirectorySyncAfterForTesting(1);
    std::ostringstream createDiagnostics;
    auto* previousCerr = std::cerr.rdbuf(createDiagnostics.rdbuf());
    const auto createStatus = engine.createTable(
        database, makeTable("created"));
    std::cerr.rdbuf(previousCerr);
    assert(createStatus == dbms::DBStatus::OK);
    assert(createDiagnostics.str().find(
               "failed to flush heap allocator") == std::string::npos);
    assert(dbms::index_file::syncDirectory(database));
    assert((readTableNames(tableList) ==
            std::vector<std::string>{"alpha", "remove_me", "omega",
                                     "created"}));

    // DROP TABLE uses the same atomic publication path; no interrupted
    // rewrite may truncate the unrelated surviving table names.
    dbms::index_file::failNextDirectorySyncForTesting();
    assert(engine.dropTable(database, "remove_me") == dbms::DBStatus::OK);
    assert(dbms::index_file::syncDirectory(database));
    assert((readTableNames(tableList) ==
            std::vector<std::string>{"alpha", "omega", "created"}));

    assert(engine.dropDatabase(database) == dbms::DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[TABLE LIST ATOMICITY] passed\n";
    return 0;
}
