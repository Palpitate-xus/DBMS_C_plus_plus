#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "access/BPTree.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>

namespace fs = std::filesystem;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture = "stale_temp_file_owner", database = testDbPath(fixture);
    cleanupTestDb(fixture);
    const std::set<std::string> names{"__tmp_12_", "__tmp_34", "__tmp", "survivor"};
    {
        StorageEngine setup;
        assert(setup.createDatabase(database) == DBStatus::OK);
        for (const auto& name : names) {
            TableSchema table;
            table.tablename = name;
            table.append(makeIntColumn("id", false, 2, true));
            table.append(makeIntColumn("12_orphan", true, 2));
            table.append(makeVarCharColumn("tag", true, 32));
            assert(setup.createTable(database, table) == DBStatus::OK);
            assert(setup.createIndex(database, name, "12_orphan") == DBStatus::OK);
            assert(setup.insertRow(database, name, {{"id", "7"}, {"12_orphan", "9"}, {"tag", "persistent"}}) == DBStatus::OK);
        }
    }
    for (const auto& name : {"__tmp_12_.dot", "__tmp_34_stale"}) {
        std::string record = name;
        record.resize(MAX_TABLE_NAME_LEN, '\0');
        std::ofstream list(fs::path(database) / "tlist.lst", std::ios::binary | std::ios::app);
        list.write(record.data(), static_cast<std::streamsize>(record.size()));
        assert(list);
        std::ofstream(fs::path(database) / (std::string(name) + ".dt"), std::ios::binary) << "stale";
    }
    const auto orphan = fs::path(database) / "__tmp_12_orphan.dt";
    std::ofstream(orphan, std::ios::binary) << "orphan heap, not __tmp's column index";
    {
        StorageEngine restarted;
        const auto actualNames = restarted.getTableNames(database);
        assert(std::set<std::string>(actualNames.begin(), actualNames.end()) == names);
        for (const auto& name : names) {
            assert(restarted.tableExists(database, name));
            for (const auto& suffix : {".stc", ".dt", ".idx", ".toast.dt", ".toast.idx", ".toastmeta"})
                assert(fs::is_regular_file(fs::path(database) / (name + suffix)));
            assert(restarted.query(database, name, {}, {"id", "12_orphan", "tag"}) ==
                   std::vector<std::string>{"7 9 persistent "});
            int64_t rid = -1;
            auto* primary = restarted.getPKIndex(database, name);
            auto* secondary = restarted.getSecondaryIndex(database, name, "12_orphan");
            assert(primary && secondary);
            assert(primary->search("7", rid));
            assert(secondary->searchMulti("9") == std::vector<int64_t>{rid});
        }
        assert(!fs::exists(fs::path(database) / "__tmp_12_.dot.dt"));
        assert(!fs::exists(fs::path(database) / "__tmp_34_stale.dt"));
        assert(!fs::exists(orphan));
        // A shared-looking prefix is not enough to delete this live index.
        assert(fs::is_regular_file(fs::path(database) / "__tmp_12_orphan.idx"));
        assert(restarted.dropDatabase(database) == DBStatus::OK);
    }
    cleanupTestDb(fixture);
    std::cout << "[STALE TEMP FILE OWNER] four persistent heaps, primary/secondary indexes, TOAST and three actual TEMP heaps passed\n";
}
