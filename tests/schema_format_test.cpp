// Current schema format is single-versioned and must fail closed on corruption.

#include "commands/TableManage.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
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
        // istreambuf_iterator reads the stream buffer directly and is not
        // required to set the owning stream's eofbit.  Verify the property
        // this test actually needs: a complete, error-free schema image.
        assert(!in.bad());
        assert(validSchema.size() == std::filesystem::file_size(schemaPath));
    }
    // The additional-CHECK extension is optional so databases written by the
    // immediately preceding schema format remain readable.  A schema with no
    // additional checks ends with the extension magic and a zero count.
    // The current writer also appends one identity-kind byte per column.
    constexpr size_t identityExtensionSize =
        sizeof(uint32_t) + sizeof(uint16_t) + 1;
    constexpr size_t checkExtensionSize =
        sizeof(uint32_t) + sizeof(int32_t);
    assert(validSchema.size() >= identityExtensionSize + checkExtensionSize);
    // This actual current file has RID1, preceded by empty DFT1/RLB1. Make
    // an explicit legacy-9 parser image using that validated version layout;
    // never pretend that its last seven bytes are the old IDN1 extension.
    auto legacySchema = validSchema;
    int32_t currentVersion = 0;
    std::memcpy(&currentVersion,validSchema.data(),sizeof(currentVersion));
    if(currentVersion==0x4442000A) {
        constexpr size_t suffix=6+8+12; // DFT1(0), RLB1(0), RID1(identity)
        assert(validSchema.size()>=suffix+identityExtensionSize+checkExtensionSize);
        const auto offset=validSchema.size()-suffix;
        uint32_t magic=0;uint16_t defaults=1;int32_t ranges=1;uint64_t relationId=0;
        std::memcpy(&magic,validSchema.data()+offset,4);assert(magic==0x31544644);
        std::memcpy(&defaults,validSchema.data()+offset+4,2);assert(defaults==0);
        std::memcpy(&magic,validSchema.data()+offset+6,4);assert(magic==0x31424C52);
        std::memcpy(&ranges,validSchema.data()+offset+10,4);assert(ranges==0);
        std::memcpy(&magic,validSchema.data()+offset+14,4);assert(magic==0x31444952);
        std::memcpy(&relationId,validSchema.data()+offset+18,8);assert(relationId!=0);
        legacySchema.resize(offset);
        const int32_t legacyVersion=0x44420009;
        std::memcpy(legacySchema.data(),&legacyVersion,4);
    } else assert(currentVersion==0x44420009);
    // Already-open owner tests the parser directly. A cold owner now also
    // validates every recovery schema before the caller can request one.
    StorageEngine parserOwner;
    {
        std::ofstream out(schemaPath, std::ios::binary | std::ios::trunc);
        out.write(legacySchema.data(), static_cast<std::streamsize>(
                                          legacySchema.size() -
                                          identityExtensionSize));
        assert(out);
    }
    {
        assert(parserOwner.getTableSchema(dbname, "t").len == 1);
    }
    {
        std::ofstream out(schemaPath, std::ios::binary | std::ios::trunc);
        out.write(legacySchema.data(), static_cast<std::streamsize>(
                                          legacySchema.size() -
                                          identityExtensionSize -
                                          checkExtensionSize));
        assert(out);
    }
    {
        assert(parserOwner.getTableSchema(dbname, "t").len == 1);
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
        // A negative on-disk width must not wrap to SIZE_MAX.
        assert(parserOwner.getTableSchema(dbname, "t").len == 0);
    }

    writeCorruptDsize(65536);
    {
        // Current column types never persist widths above the format limit.
        assert(parserOwner.getTableSchema(dbname, "t").len == 0);
    }

    std::filesystem::resize_file(schemaPath, 8);
    {
        // A truncated current-format schema must not become a partially parsed
        // schema that callers could accidentally use for writes.
        assert(parserOwner.getTableSchema(dbname, "t").len == 0);
    }

    {
        std::ofstream out(schemaPath, std::ios::binary | std::ios::trunc);
        const int32_t unsupportedMagic = 0x44420001;
        out.write(reinterpret_cast<const char*>(&unsupportedMagic), sizeof(unsupportedMagic));
    }
    {
        assert(parserOwner.getTableSchema(dbname, "t").len == 0);
    }
    bool coldRejected=false;
    try { StorageEngine cold; }
    catch(const std::runtime_error&) { coldRejected=true; }
    assert(coldRejected);
    {std::ofstream restored(schemaPath,std::ios::binary|std::ios::trunc);
     restored.write(validSchema.data(),validSchema.size());assert(restored);}

    std::filesystem::remove_all(dbname);
    std::filesystem::remove_all(dbname + ".txn_backup");
    std::cout << "[SCHEMA FORMAT] strict current-format reads OK\n";
    return 0;
}
