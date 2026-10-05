#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

static dbms::TableSchema makeTable(const std::string& name) {
    dbms::TableSchema table;
    table.tablename = name;
    table.append(dbms::makeIntColumn("id", true, 2));
    return table;
}

int main() {
    cleanupAllTestData();
    dbms::TypeRegistry::instance().bootstrap();

    const std::string database = testDbPath("stale_temp_startup_recovery");
    const std::string staleTemporary = "__tmp_1_abandoned";
    const fs::path tableList = fs::path(database) / "tlist.lst";
    const fs::path staleHeap = fs::path(database) /
        (staleTemporary + ".dt");
    {
        dbms::StorageEngine setup;
        assert(setup.createDatabase(database, "utf8") ==
               dbms::DBStatus::OK);
        assert(setup.createTable(database, makeTable("survivor")) ==
               dbms::DBStatus::OK);

        // Model a process dying after the temporary relation name became
        // visible in tlist.lst but before its schema marker was durable.
        std::string encodedName = staleTemporary;
        encodedName.resize(dbms::MAX_TABLE_NAME_LEN, '\0');
        std::ofstream list(tableList, std::ios::binary | std::ios::app);
        list.write(encodedName.data(),
                   static_cast<std::streamsize>(encodedName.size()));
        assert(list);
        std::ofstream(staleHeap, std::ios::binary) << "stale";
        assert(fs::exists(staleHeap));
    }

    // Recovery must not ask for an incomplete temp table's schema while
    // rebuilding persistent specialized indexes. Startup cleanup removes the
    // stale name and fork after recovery, without hiding the ordinary table.
    dbms::StorageEngine restarted;
    assert((restarted.getTableNames(database) ==
            std::vector<std::string>{"survivor"}));
    assert(restarted.tableExists(database, "survivor"));
    assert(!fs::exists(staleHeap));
    assert(restarted.dropDatabase(database) == dbms::DBStatus::OK);
    finalCleanupTestData();
    std::cout << "[STALE TEMP STARTUP RECOVERY] passed\n";
    return 0;
}
