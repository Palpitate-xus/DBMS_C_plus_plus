#include "catalog/type_registry.h"
#include "catalog/CatalogService.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include "test_utils.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    size_t checks = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++checks;
        std::cout << "TEMPORAL_LAYOUT_CHECK " << checks << ' ' << good << ' ' << label << std::endl;
        if (!good) { ++failures; std::cerr << "TEMPORAL_LAYOUT_FAILURE " << label << '\n'; }
    };
    TypeRegistry::instance().bootstrap();
    const std::string database = testDbPath("temporal_storage_layout");
    check(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "isolated database");
    Session session;
    session.username = "testuser"; session.permission = 1; session.currentDB = database;
    setCurrentSession(&session);
    struct Clear { ~Clear() { setCurrentSession(nullptr); } } clear;
    DdlExecutor ddl;
    TableSchema legacy;
    legacy.tablename = "legacy";
    legacy.append(makeIntColumn("id", false, 4, true));
    legacy.append(makeTimeColumn("t", true));
    legacy.append(makeTimestampColumn("p", true));
    legacy.append(makeTimestamptzColumn("z", true));
    check(g_engine.createTable(database, legacy) == DBStatus::OK, "native legacy schema");
    check(g_engine.insertRow(database, "legacy", {{"id", std::string("1")}, {"t", std::string("12:34:56")},
        {"p", std::string("2000-01-01 00:00:00")}, {"z", std::string("2000-01-01 00:00:00+00")}}) == DBStatus::OK,
        "legacy whole-second row");
    const auto legacyPath = std::filesystem::path(database) / "legacy.stc";
    const auto readBytes = [](const std::filesystem::path& path) {
        std::ifstream input(path, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
    };
    int generation = 0;
    const auto publish = [&](const std::filesystem::path& path, const std::string& bytes) {
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        output.write(bytes.data(), bytes.size()); output.close();
        std::error_code error;
        std::filesystem::last_write_time(path, std::filesystem::file_time_type::clock::now() +
            std::chrono::seconds(++generation), error);
        check(!error && output.good(), "publish owned schema image");
    };
    const auto append = [](std::string& bytes, const auto& value) {
        bytes.append(reinterpret_cast<const char*>(&value), sizeof(value));
    };
    const auto version = [](const std::string& bytes) {
        int32_t value = 0; if (bytes.size() >= 4) std::memcpy(&value, bytes.data(), 4);
        return value;
    };
    const auto oldA = readBytes(legacyPath);
    check(version(oldA) == 0x4442000A, "legacy writer still A");
    auto old9 = oldA;
    if (version(oldA) != 0x4442000A || oldA.size() < 26) return 1;
    old9.resize(old9.size() - 26); // empty DFT1/RLB1 and RID1
    int32_t v9 = 0x44420009; std::memcpy(old9.data(), &v9, 4);
    auto oldB = oldA;
    const uint32_t dso = 0x314F5344;
    append(oldB, dso); const uint16_t four = 4; append(oldB, four);
    oldB.append(4 * 3, '\0'); // each column: origin byte + uint16 domain-name length
    const int32_t vb = 0x4442000B; std::memcpy(oldB.data(), &vb, 4);
    auto oldC = oldB;
    const uint32_t itm = 0x314D5449;
    append(oldC, itm); append(oldC, four);
    for (int i = 0; i < 4; ++i) { const int32_t modifier = -1; append(oldC, modifier); }
    const int32_t vc = 0x4442000C; std::memcpy(oldC.data(), &vc, 4);
    const auto before = g_engine.query(database, "legacy", {}, {"id","t","p","z"}, {});
    std::vector<std::vector<std::string>> rawLegacyRows;
    g_engine.forEachRow(database, "legacy", [&](uint32_t, uint16_t, const char* data, size_t size) {
        const std::string row(data, size);
        std::vector<std::string> cells;
        for (size_t i = 0; i < legacy.len; ++i)
            cells.push_back(StorageEngine::extractColumnValueStatic(row, legacy, i));
        rawLegacyRows.push_back(std::move(cells));
    });
    for (const auto& image : {old9, oldA, oldB, oldC}) {
        publish(legacyPath, image);
        const auto schema = g_engine.getTableSchema(database, "legacy");
        check(schema.len == 4, "legacy format " + std::to_string(version(image)) + " read");
        check(schema.len == 4 && schema.cols[1].dsize == 4 &&
            std::all_of(schema.cols, schema.cols + schema.len, [](const Column& c) {
                return c.storageCodec == Column::StorageCodec::Legacy;
            }), "old units are never inferred from width");
        if (schema.len != 4) continue;
        {
            // These are parser images, not new crash-recovery databases. In
            // particular, 9 has no RID1 while this fixture's modern WAL does.
            // Keep one owner during synthetic schema edits and decode actual
            // legacy payloads; validate a cold owner after restoring real A.
            size_t index = 0;
            g_engine.forEachRow(database, "legacy", [&](uint32_t, uint16_t, const char* data, size_t size) {
                const std::string row(data, size);
                std::vector<std::string> cells;
                for (size_t i = 0; i < schema.len; ++i)
                    cells.push_back(StorageEngine::extractColumnValueStatic(row, schema, i));
                check(index < rawLegacyRows.size() && cells == rawLegacyRows[index++],
                      "legacy 9/A/B/C parser/raw datum codec preserves seconds");
            });
            check(index == rawLegacyRows.size(), "legacy raw row count");
        }
    }
    publish(legacyPath, oldA);
    {
        StorageEngine cold;
        check(cold.query(database, "legacy", {}, {"id","t","p","z"}, {}) == before,
              "real legacy A heap/WAL cold read");
    }
    g_engine.catalogService().evict(database);
    check(!ddl.executeSql("CREATE TABLE tagged(id integer,t time,p timestamp,z timestamptz,note text)", session),
        "SQL scalar tagged table");
    const auto taggedPath = std::filesystem::path(database) / "tagged.stc";
    const auto tagged = readBytes(taggedPath);
    check(version(tagged) == 0x4442000D, "tagged schema D");
    auto schema = g_engine.getTableSchema(database, "tagged");
    check(schema.len == 5 && schema.cols[0].storageCodec == Column::StorageCodec::Legacy &&
        schema.cols[4].storageCodec == Column::StorageCodec::Legacy, "non-temporal codec unchanged");
    for (size_t i = 1; i < 4; ++i)
        check(schema.len == 5 && schema.cols[i].storageCodec == Column::StorageCodec::TemporalMicros &&
            schema.cols[i].dsize == 8, "fixed int64 microsecond slot");
    check(g_engine.insertRow(database, "tagged", {{"id", std::string("1")}, {"t", std::string("00:00:00.000001")},
        {"p", std::string("1999-12-31 23:59:59.999999")},
        {"z", std::string("2000-01-01 05:45:00.000001+05:45")}, {"note", std::string("a b")}}) == DBStatus::OK,
        "mixed varlena microsecond insert");
    check(g_engine.insertRow(database, "tagged", {{"id", std::string("2")}, {"t", std::string("24:00:00")},
        {"p", std::string("2000-01-01 00:00:00")}, {"z", std::string("infinity")}}) == DBStatus::OK,
        "epoch zero and 24h and infinity");
    check(g_engine.insertRow(database, "tagged", {{"id", std::string("3")}}) == DBStatus::OK, "explicit nullable controls");
    const auto taggedRows = g_engine.query(database, "tagged", {}, {"id","t","p","z","note"}, {});
    {
        StorageEngine cold;
        check(cold.query(database, "tagged", {}, {"id","t","p","z","note"}, {}) == taggedRows,
              "cold mixed layout values exact");
    }
    g_engine.catalogService().evict(database);
    StorageEngine parser;
    const size_t tail = tagged.size() - 11; // TCM1 + count + five tags
    for (int corruption = 0; corruption < 8; ++corruption) {
        auto bytes = tagged;
        if (corruption == 0) bytes[tail] = 'X';
        if (corruption == 1) bytes[tail + 4] = 4;
        if (corruption == 2) bytes.back() = 2;
        if (corruption == 3) bytes[tail + 6] = 1; // int column cannot use temporal units
        if (corruption == 4) bytes[tail + 10] = 1; // text column cannot use temporal units
        if (corruption == 5) bytes.resize(bytes.size() - 1);
        if (corruption == 6) bytes.append(1, '\0');
        if (corruption == 7) for (size_t i = tail + 6; i < bytes.size(); ++i) bytes[i] = 0;
        publish(taggedPath, bytes);
        check(parser.getTableSchema(database, "tagged").len == 0, "D corrupt tail fails closed");
    }
    for (const auto* extension : {"CHK2", "IDN1", "DFT1", "RLB1", "RID1", "DSO1", "ITM1", "TCM1"}) {
        const auto position = tagged.find(extension);
        check(position != std::string::npos, std::string("required D extension ") + extension);
        if (position != std::string::npos) {
            publish(taggedPath, tagged.substr(0, position));
            check(parser.getTableSchema(database, "tagged").len == 0,
                  std::string("D prefix truncation before ") + extension + " fails closed");
        }
    }
    publish(taggedPath, tagged);
    check(parser.getTableSchema(database, "tagged").len == 5, "valid D readable after restoration");
    // Same SQL type and same timestamp width still require a unit conversion.
    for (const auto& nameType : std::vector<std::pair<std::string,std::string>>{
            {"t","time"},{"p","timestamp"},{"z","timestamptz"}}) {
        if (nameType.first == "z") {
            session.timeZone = "America/New_York";
            session.timezoneOffsetMinutes = -300;
        }
        const auto beforeAlter = g_engine.query(database, "legacy", {}, {"id","t","p","z"}, {});
        check(!ddl.executeSql("ALTER TABLE legacy ALTER COLUMN " + nameType.first + " TYPE " + nameType.second,
                              session), "legacy to micro ALTER");
        const auto altered = g_engine.getTableSchema(database, "legacy");
        const size_t index = nameType.first == "t" ? 1 : nameType.first == "p" ? 2 : 3;
        check(altered.cols[index].storageCodec == Column::StorageCodec::TemporalMicros, "ALTER persists codec");
        check(g_engine.query(database, "legacy", {}, {"id","t","p","z"}, {}) == beforeAlter,
              "ALTER preserves decoded instants, including legacy UTC in non-UTC session");
    }
    session.timeZone = "UTC";
    session.timezoneOffsetMinutes = 0;
    check(g_engine.insertRow(database, "legacy", {{"id", std::string("2")}, {"t", std::string("12:34:56.000001")},
        {"p", std::string("2000-01-01 00:00:00.000001")}, {"z", std::string("2000-01-01 00:00:00.000001+00")}}) ==
        DBStatus::OK, "converted legacy table accepts fractional values");
    TableSchema parent; parent.tablename = "legacy_parent"; parent.append(makeTimeColumn("t", true));
    check(g_engine.createTable(database, parent) == DBStatus::OK, "legacy TIME inheritance parent");
    check(!ddl.executeSql("CREATE TABLE micro_inherited(t time) INHERITS(legacy_parent)", session),
        "new child accepts same SQL type with old parent units");
    const auto child = g_engine.getTableSchema(database, "micro_inherited");
    check(child.len == 1 && child.cols[0].storageCodec == Column::StorageCodec::TemporalMicros,
        "new child uses its own microsecond heap");
    check(g_engine.insertRow(database, "micro_inherited", {{"t", std::string("00:00:00.000001")}}) == DBStatus::OK,
          "inherited fractional insert");
    std::cout << "TEMPORAL_STORAGE_LAYOUT_COMPLETE " << checks << " FAILURES " << failures << '\n';
    return failures == 0 ? 0 : 1;
}
