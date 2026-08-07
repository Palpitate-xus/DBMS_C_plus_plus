// Current schema format is single-versioned and must fail closed on corruption.

#include "commands/TableManage.h"

#include <cassert>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>

using namespace dbms;

int main() {
    const std::string dbname = "__t_schema_format";
    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");

    {
        StorageEngine engine;
        assert(engine.createDatabase(dbname) == DBStatus::OK);

        TableSchema tbl;
        tbl.tablename = "t";
        tbl.append(makeIntColumn("id", false, 0, true));
        assert(engine.createTable(dbname, tbl) == DBStatus::OK);
        assert(engine.getTableSchema(dbname, "t").len == 1);
    }

    const auto schemaPath = std::filesystem::path(dbname) / "t.stc";
    std::filesystem::resize_file(schemaPath, 8);
    {
        StorageEngine engine;
        // A truncated current-format schema must not become a partially parsed
        // schema that callers could accidentally use for writes.
        assert(engine.getTableSchema(dbname, "t").len == 0);
    }

    {
        std::ofstream out(schemaPath, std::ios::binary | std::ios::trunc);
        const int32_t unsupportedMagic = 0x44420001;
        out.write(reinterpret_cast<const char*>(&unsupportedMagic), sizeof(unsupportedMagic));
    }
    {
        StorageEngine engine;
        assert(engine.getTableSchema(dbname, "t").len == 0);
    }

    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::cout << "[SCHEMA FORMAT] strict current-format reads OK\n";
    return 0;
}
