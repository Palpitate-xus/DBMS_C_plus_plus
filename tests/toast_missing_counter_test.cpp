#include "access/IndexFileUtil.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("toast_missing_counter");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    size_t groups = 0;
    for (const bool populated : {true, false}) {
        TableSchema table;
        table.tablename = populated ? "populated" : "empty";
        table.formatVersion = 2;
        table.append(makeIntColumn("id", false, 4, true));
        table.append(makeVarCharColumn("payload", true, 12000));
        assert(g_engine.createTable(database, table) == DBStatus::OK);
        const std::string original(10000, 'a'), attempted(11000, 'b');
        if (populated) {
            assert(g_engine.beginTransaction(database) == DBStatus::OK);
            assert(g_engine.insert(database, table.tablename,
                {{"id", "1"}, {"payload", original}}) == DBStatus::OK);
            assert(g_engine.commitTransaction() == DBStatus::OK);
        }
        const auto before = g_engine.query(database, table.tablename, {}, {"payload"});
        assert(before.size() == static_cast<size_t>(populated));
        if (populated) assert(before.front() == original + " ");
        const auto counter = std::filesystem::path(database) / (table.tablename + ".toastmeta");
        std::ifstream input(counter, std::ios::binary);
        assert(input);
        const std::string saved{std::istreambuf_iterator<char>(input), {}};
        input.close();
        assert(saved.size() == 24);
        auto moved = counter; moved += ".saved_test";
        std::filesystem::rename(counter, moved);
        assert(!std::filesystem::exists(counter));
        const auto status = g_engine.insert(database, table.tablename,
            {{"id", "2"}, {"payload", attempted}});
        const bool recreated = std::filesystem::exists(counter);
        const auto after = g_engine.query(database, table.tablename, {}, {"payload"});
        const bool unchanged = after == before;
        std::cout << "TOAST_MISSING_COUNTER populated=" << populated
                  << " io_error=" << (status == DBStatus::IO_ERROR)
                  << " recreated=" << recreated << " unchanged=" << unchanged
                  << " before_rows=" << before.size() << " after_rows=" << after.size()
                  << std::endl;
        if (status != DBStatus::IO_ERROR || recreated || !unchanged) return 1;
        if (populated) {
            const auto updated = g_engine.update(database, table.tablename,
                {{"payload", attempted}}, {"=id 1"});
            const bool kept = g_engine.query(database, table.tablename,
                {}, {"payload"}) == before;
            std::cout << "TOAST_MISSING_COUNTER_UPDATE io_error="
                      << (updated == DBStatus::IO_ERROR) << " unchanged=" << kept << std::endl;
            assert(updated == DBStatus::IO_ERROR && kept);
            assert(!std::filesystem::exists(counter));
        }
        // Repeated allocation cannot heal missing metadata from a cached range.
        assert(g_engine.allocToastId(database, table.tablename) == 0);
        assert(!std::filesystem::exists(counter));
        assert(index_file::writeAtomically(counter, saved));
        assert(g_engine.insert(database, table.tablename,
            {{"id", "2"}, {"payload", attempted}}) == DBStatus::OK);
        assert(g_engine.query(database, table.tablename, {"=id 2"}, {"payload"}) ==
            std::vector<std::string>{attempted + " "});
        if (populated) assert(g_engine.query(database, table.tablename,
            {"=id 1"}, {"payload"}) == before);
        ++groups;
    }
    std::cout << "TOAST_MISSING_COUNTER_COMPLETE groups=" << groups << '\n';
}
