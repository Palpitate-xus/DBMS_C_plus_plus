#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "storage/WAL.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <set>

namespace fs = std::filesystem;

static std::string readFile(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    assert(in);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

static void appendName(const std::string& database, const std::string& name) {
    std::string record = name;
    record.resize(dbms::MAX_TABLE_NAME_LEN, '\0');
    std::ofstream list(fs::path(database) / "tlist.lst", std::ios::binary | std::ios::app);
    list.write(record.data(), static_cast<std::streamsize>(record.size()));
    assert(list);
}

static bool validDatabase(bool unlogged) {
    using namespace dbms;
    const std::string fixture = unlogged ? "stale_temp_enum_no_wal" : "stale_temp_enum_wal";
    const std::string database = testDbPath(fixture);
    cleanupTestDb(fixture);
    // A numeric-looking name without the backend/name separator is not TEMP.
    // The independent empty-tail cleanup defect retains its original fixture
    // unchanged under compat/stale_temp_prefix_cleanup_known_gap.cpp (R40).
    const std::set<std::string> expected{"survivor", "__tmp_text_keep", "__tmp_12"};
    {
        StorageEngine setup;
        assert(setup.createDatabase(database) == DBStatus::OK);
        for (const auto& name : expected) {
            TableSchema table;
            table.tablename = name;
            table.isUnlogged = unlogged;
            table.append(makeIntColumn("id", false, 2, true));
            assert(setup.createTable(database, table) == DBStatus::OK);
            if (!unlogged) assert(setup.insertRow(database, name, {{"id", "7"}}) == DBStatus::OK);
        }
        const auto* wal = setup.getWAL(database);
        assert(wal && (unlogged ? wal->currentWriteLsn() == 0 : wal->currentWriteLsn() > 0));
    }
    for (const auto& name : {"__tmp_1_partial", "__tmp_2_broken"}) {
        appendName(database, name);
        std::ofstream(fs::path(database) / (std::string(name) + ".dt"), std::ios::binary) << "stale";
    }
    std::ofstream(fs::path(database) / "__tmp_2_broken.stc", std::ios::binary) << "incomplete";
    std::ofstream(fs::path(database) / "__tmp_2147483648_orphan.dt", std::ios::binary) << "orphan";
    std::ofstream(fs::path(database) / "__tmp_1_partial.toast", std::ios::binary) << "toast";
    bool success = false;
    try {
        StorageEngine restarted;
        const auto names = restarted.getTableNames(database);
        assert(std::set<std::string>(names.begin(), names.end()) == expected);
        for (const auto& name : expected) {
            assert(restarted.tableExists(database, name));
            const auto rows = restarted.query(database, name, {}, {"id"});
            assert(rows.size() == (unlogged ? 0u : 1u));
        }
        for (const auto& file : {"__tmp_1_partial.dt", "__tmp_1_partial.toast", "__tmp_2_broken.dt",
                                 "__tmp_2_broken.stc", "__tmp_2147483648_orphan.dt"})
            assert(!fs::exists(fs::path(database) / file));
        assert(restarted.dropDatabase(database) == DBStatus::OK);
        success = true;
    } catch (const std::exception& error) {
        std::cerr << "STALE_TEMP_ENUM " << unlogged << " ERROR " << error.what() << '\n';
    }
    cleanupTestDb(fixture);
    std::cout << "STALE_TEMP_ENUM " << unlogged << " PASS " << success << std::endl;
    return success;
}

static void persistentFailureIsNotTemporary() {
    using namespace dbms;
    const std::string fixture = "stale_temp_persistent_missing";
    const std::string database = testDbPath(fixture);
    cleanupTestDb(fixture);
    { StorageEngine setup; assert(setup.createDatabase(database) == DBStatus::OK); }
    const std::string name = "__tmp_text_missing";
    appendName(database, name);
    const auto heap = fs::path(database) / (name + ".dt");
    std::ofstream(heap, std::ios::binary) << "persistent-owned-marker";
    const auto before = readFile(fs::path(database) / "tlist.lst");
    bool rejected = false;
    try { StorageEngine restarted; }
    catch (const std::runtime_error&) { rejected = true; }
    assert(rejected);
    assert(readFile(heap) == "persistent-owned-marker");
    assert(readFile(fs::path(database) / "tlist.lst") == before);
    cleanupTestDb(fixture);
    std::cout << "STALE_TEMP_ENUM persistent missing schema correctly rejected and retained\n";
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const bool logged = validDatabase(false);
    const bool unlogged = validDatabase(true);
    persistentFailureIsNotTemporary();
    std::cout << "[STALE TEMP ENUMERATION] logged/no-WAL startup and exact namespace controls\n";
    return logged && unlogged ? 0 : 1;
}
