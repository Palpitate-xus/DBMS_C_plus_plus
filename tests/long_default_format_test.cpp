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
    const size_t extension = original.size() - (sizeof(uint32_t) + sizeof(uint16_t) + entrySize * 2);
    uint32_t magic = 0;
    std::memcpy(&magic, original.data() + extension, sizeof(magic));
    assert(magic == 0x31544644);
    int revision = 0;
    const auto checkRejected = [&](const std::string& image) {
        { std::ofstream output(schema, std::ios::binary | std::ios::trunc); output.write(image.data(), image.size()); assert(output); }
        std::filesystem::last_write_time(schema, std::filesystem::file_time_type::clock::now() + std::chrono::seconds(++revision));
        dbms::StorageEngine engine;
        assert(engine.getTableSchema(database, "defaults").len == 0);
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
    corrupt.back() = '\0';
    checkRejected(corrupt);
    checkRejected(original + "unexpected");
    // Files from before the optional extension remain readable; their lost
    // suffix cannot be reconstructed and is not silently fabricated.
    { std::ofstream output(schema, std::ios::binary | std::ios::trunc); output.write(original.data(), extension); assert(output); }
    std::filesystem::last_write_time(schema, std::filesystem::file_time_type::clock::now() + std::chrono::seconds(++revision));
    {
        dbms::StorageEngine engine;
        assert(engine.getTableSchema(database, "defaults").cols[0].defaultValue == expression.substr(0, dbms::MAX_COL_NAME_LEN));
        assert(engine.dropDatabase(database) == dbms::DBStatus::OK);
    }
    cleanupTestDb("long_default_format");
    std::cout << "[LONG DEFAULT FORMAT] passed" << std::endl;
}
