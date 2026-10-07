#include "commands/TableManage.h"
#include "test_utils.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

int main() {
    const std::string database = testDbPath("long_default_format");
    cleanupTestDb("long_default_format");
    const std::string expression = "'" + std::string(180, 'a') + "'";
    {
        dbms::StorageEngine engine;
        assert(engine.createDatabase(database) == dbms::DBStatus::OK);
        dbms::TableSchema table;
        table.tablename = "defaults";
        auto value = dbms::makeVarCharColumn("value", false, 512);
        value.defaultValue = expression;
        table.append(value);
        auto second = dbms::makeVarCharColumn("second", false, 512);
        second.defaultValue = "'" + std::string(180, 'b') + "'";
        table.append(second);
        assert(engine.createTable(database, table) == dbms::DBStatus::OK);
    }
    const auto schema = std::filesystem::path(database) / "defaults.stc";
    std::ifstream input(schema, std::ios::binary);
    const std::string original{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
    input.close();
    { dbms::StorageEngine engine;
      const auto table = engine.getTableSchema(database, "defaults");
      assert(table.cols[0].defaultValue == expression);
      assert(table.cols[1].defaultValue == "'" + std::string(180, 'b') + "'"); }
    const size_t entrySize = sizeof(uint16_t) + sizeof(uint32_t) + expression.size();
    // Native columns here are legacy/frozen origins, so the current file is
    // schema9 or schema10, not domain-origin B. Validate and subtract its
    // actual RLB1/RID1 suffix rather than treating DFT1 as the end of file.
    uint32_t version=0;std::memcpy(&version,original.data(),4);
    size_t defaultEnd=original.size();
    if(version==0x4442000A) {
        assert(defaultEnd>=20);
        uint32_t magic=0;int32_t ranges=1;uint64_t identity=0;
        std::memcpy(&magic,original.data()+defaultEnd-20,4);assert(magic==0x31424C52);
        std::memcpy(&ranges,original.data()+defaultEnd-16,4);assert(ranges==0);
        std::memcpy(&magic,original.data()+defaultEnd-12,4);assert(magic==0x31444952);
        std::memcpy(&identity,original.data()+defaultEnd-8,8);assert(identity!=0);
        defaultEnd-=20;
    } else assert(version==0x44420009);
    const size_t extension = defaultEnd - (sizeof(uint32_t) + sizeof(uint16_t) + entrySize * 2);
    uint32_t magic = 0;
    std::memcpy(&magic, original.data() + extension, sizeof(magic));
    assert(magic == 0x31544644);
    dbms::StorageEngine parserOwner;
    int revision = 0;
    const auto checkRejected = [&](const std::string& image) {
        { std::ofstream output(schema, std::ios::binary | std::ios::trunc); output.write(image.data(), image.size()); assert(output); }
        std::filesystem::last_write_time(schema, std::filesystem::file_time_type::clock::now() + std::chrono::seconds(++revision));
        assert(parserOwner.getTableSchema(database, "defaults").len == 0);
    };
    checkRejected(original.substr(0, original.size() - 1));
    auto corrupt = original;
    const uint32_t excessive = 1024 * 1024 + 1;
    std::memcpy(corrupt.data() + extension + sizeof(uint32_t) + sizeof(uint16_t) * 2, &excessive, sizeof(excessive));
    checkRejected(corrupt);
    corrupt = original;
    const uint16_t missingColumn = 2;
    std::memcpy(corrupt.data() + extension + sizeof(uint32_t) + sizeof(uint16_t), &missingColumn, sizeof(missingColumn));
    checkRejected(corrupt);
    corrupt = original;
    const uint16_t duplicateColumn = 0;
    std::memcpy(corrupt.data() + extension + sizeof(uint32_t) + sizeof(uint16_t) + entrySize, &duplicateColumn, sizeof(duplicateColumn));
    checkRejected(corrupt);
    corrupt = original;
    corrupt[extension + sizeof(uint32_t) + sizeof(uint16_t) + entrySize - 1] = '\0';
    checkRejected(corrupt);
    checkRejected(original + "unexpected");
    // Files from before the optional extension remain readable; their lost
    // suffix cannot be reconstructed and is not silently fabricated.
    auto legacy=original.substr(0,extension);
    const uint32_t legacyVersion=0x44420009;
    std::memcpy(legacy.data(),&legacyVersion,4);
    { std::ofstream output(schema, std::ios::binary | std::ios::trunc); output.write(legacy.data(), legacy.size()); assert(output); }
    std::filesystem::last_write_time(schema, std::filesystem::file_time_type::clock::now() + std::chrono::seconds(++revision));
    {
        assert(parserOwner.getTableSchema(database, "defaults").cols[0].defaultValue == expression.substr(0, dbms::MAX_COL_NAME_LEN));
        // Restore the real physical generation before retiring this table.
        {std::ofstream restored(schema,std::ios::binary|std::ios::trunc);restored.write(original.data(),original.size());assert(restored);}
        std::filesystem::last_write_time(schema,std::filesystem::file_time_type::clock::now()+std::chrono::seconds(++revision));
        assert(parserOwner.dropDatabase(database) == dbms::DBStatus::OK);
    }
    cleanupTestDb("long_default_format");
    std::cout << "[LONG DEFAULT FORMAT] passed" << std::endl;
}
