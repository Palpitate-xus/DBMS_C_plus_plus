// Current schema format is single-versioned and must fail closed on corruption.

#include "commands/TableManage.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>

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
    std::string validSchema;
    {
        std::ifstream in(schemaPath, std::ios::binary);
        validSchema.assign(std::istreambuf_iterator<char>(in),
                           std::istreambuf_iterator<char>());
        assert(in.eof());
    }
    int corruptionVersion = 0;
    const auto writeCorruptDsize = [&](int32_t dsize) {
        std::ofstream out(schemaPath, std::ios::binary | std::ios::trunc);
        assert(out);
        out.write(validSchema.data(), static_cast<std::streamsize>(validSchema.size()));
        out.seekp(static_cast<std::streamoff>(sizeof(int32_t) * 2 + sizeof(uint8_t) +
                                              MAX_TYPE_NAME_LEN + MAX_COL_NAME_LEN));
        out.write(reinterpret_cast<const char*>(&dsize), sizeof(dsize));
        out.flush();
        assert(out);
        out.close();

        // Force the process-wide schema cache to observe every same-size
        // corruption, including on filesystems with coarse write timestamps.
        std::error_code ec;
        std::filesystem::last_write_time(
            schemaPath,
            std::filesystem::file_time_type::clock::now() +
                std::chrono::seconds(++corruptionVersion),
            ec);
        assert(!ec);
    };

    writeCorruptDsize(-1);
    {
        StorageEngine engine;
        // A negative on-disk width must not wrap to SIZE_MAX.
        assert(engine.getTableSchema(dbname, "t").len == 0);
    }

    writeCorruptDsize(65536);
    {
        StorageEngine engine;
        // Current column types never persist widths above the format limit.
        assert(engine.getTableSchema(dbname, "t").len == 0);
    }

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
