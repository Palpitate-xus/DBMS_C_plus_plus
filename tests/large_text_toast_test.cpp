#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
using Cell = std::optional<std::string>;
using Rows = std::map<std::string, StorageEngine::SqlRow>;
size_t checks = 0, failures = 0;
void check(bool good, const char* label) {
    ++checks;
    if (!good) { ++failures; std::cerr << "FAIL " << label << '\n'; }
}
void require(bool good, const char* label) { if (!good) throw std::runtime_error(label); }
std::string payload(size_t bytes, uint32_t seed) {
    static constexpr char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::string value(bytes, '\0');
    for (char& c : value) {
        seed ^= seed << 13; seed ^= seed >> 17; seed ^= seed << 5;
        c = alphabet[seed % (sizeof(alphabet) - 1)];
    }
    return value + " \n\t";
}
Rows snapshot(const std::string& database, const std::string& table) {
    const auto schema = g_engine.getTableSchema(database, table);
    TableScanOp scan(&g_engine, database, table);
    require(scan.open(), "actual full scan opens");
    Rows rows; std::string raw;
    while (scan.next(raw)) {
        StorageEngine::SqlRow row;
        for (size_t i = 0; i < schema.len; ++i)
            row[schema.cols[i].dataName] = scan.lastColumnIsNull(i) ? Cell{} :
                Cell{g_engine.extractColumnValue(raw, schema, i, database)};
        require(row.at("id").has_value() && rows.emplace(*row.at("id"), std::move(row)).second,
                "actual complete unique IDs");
    }
    require(!scan.hasError(), "actual full scan finishes without an error");
    scan.close();
    return rows;
}
}
int main() {
    using namespace dbms;
    try {
        TypeRegistry::instance().bootstrap(); g_engine.setBackgroundIntervals(60000, 60000);
        const std::string database = testDbPath("large_text_toast");
        require(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "create actual database");
        TableSchema schema; schema.tablename = "documents";
        schema.append(makeIntColumn("id", false, 4, true));
        schema.append(makeTextColumn("body", true));
        schema.append(makeTextColumn("other", true));
        schema.append(makeVarCharColumn("bounded", false, 12));
        require(g_engine.createTable(database, schema) == DBStatus::OK, "create actual TEXT table");
        check(g_engine.getTableSchema(database, "documents").cols[1].dsize == 65535,
              "legacy persisted nominal TEXT width remains readable");
        std::string unicode;
        for (size_t i = 0; i < 10001; ++i) unicode += "é中🙂";
        unicode += " \n\t";
        const std::vector<Cell> inputs = {
            payload(65532, 1), payload(65533, 2), payload(70000, 3), unicode,
            std::string(131073, 'z') + " \n\t", payload(1048573, 4),
            Cell{}, std::string{}, std::string("NULL"), std::string("__TOAST__1")};
        Rows expected;
        for (size_t i = 0; i < inputs.size(); ++i) {
            const auto id = std::to_string(i);
            StorageEngine::SqlRow row{{"id", id}, {"body", inputs[i]},
                {"other", i == 2 ? Cell{payload(100000, 5)} : Cell{payload(40000, 6)}},
                {"bounded", "ok"}};
            const auto status = g_engine.insertRow(database, "documents", row);
            check(status == DBStatus::OK, "all boundary/full-value/NULL TEXT inserts accepted");
            expected[id] = row;
            std::cout << "LARGE_TEXT insert=" << id << " bytes=" << (inputs[i] ? inputs[i]->size() : 0)
                      << " null=" << !inputs[i] << " actual_status=" << int(status) << '\n';
        }
        check(snapshot(database, "documents") == expected,
              "all original bytes including trailing whitespace and NULLs round-trip");
        require(g_engine.insert(database, "documents", {{"id", "20"}, {"body", "seed"},
            {"other", "seed"}, {"bounded", "ok"}}) == DBStatus::OK, "legacy mutation seed");
        expected["20"] = {{"id", "20"}, {"body", payload(70000, 7)},
                          {"other", "seed"}, {"bounded", "ok"}};
        std::vector<std::map<std::string, std::string>> legacyImages;
        check(g_engine.update(database, "documents", {{"body", *expected["20"]["body"]}},
              {"=id 20"}, &legacyImages) == DBStatus::OK, "legacy update accepts full TEXT");
        check(legacyImages.size() == 1 && legacyImages[0].at("body") == *expected["20"]["body"],
              "legacy update returns the whole logical value");
        check(snapshot(database, "documents") == expected, "legacy update stores whole value");
        expected["20"]["body"] = payload(131074, 8); expected["20"]["other"] = Cell{};
        std::vector<StorageEngine::SqlRow> images; size_t affected = 0;
        check(g_engine.updateRows(database, "documents", {{"body", expected["20"]["body"]},
              {"other", Cell{}}}, {"=id 20"}, &images, {}, {}, &affected) == DBStatus::OK,
              "optional update accepts full TEXT and SQL NULL");
        check(affected == 1 && images.size() == 1 && images[0] == expected["20"],
              "optional update returns full values and exact affected count");
        check(snapshot(database, "documents") == expected, "optional update stores every byte");
        const auto committed = expected;
        require(g_engine.beginTransaction(database) == DBStatus::OK, "begin actual rollback control");
        check(g_engine.updateRows(database, "documents", {{"body", payload(90000, 9)}},
              {"=id 20"}) == DBStatus::OK, "transaction update accepts long TEXT");
        check(g_engine.insertRow(database, "documents", {{"id", "30"}, {"body", payload(70000, 10)},
              {"other", "pending"}, {"bounded", "ok"}}) == DBStatus::OK,
              "transaction insert accepts long TEXT");
        require(g_engine.rollbackTransaction() == DBStatus::OK, "actual rollback succeeds");
        check(snapshot(database, "documents") == committed, "rollback restores whole original values");
        require(g_engine.beginTransaction(database) == DBStatus::OK, "begin actual commit control");
        expected["20"]["body"] = std::string(200003, 'q') + " \n\t";
        check(g_engine.updateRows(database, "documents", {{"body", expected["20"]["body"]}},
              {"=id 20"}) == DBStatus::OK, "committed large TEXT update succeeds");
        require(g_engine.commitTransaction() == DBStatus::OK, "actual commit succeeds");
        check(snapshot(database, "documents") == expected, "commit retains complete large TEXT");
        (void)g_engine.vacuum(database, "documents");
        check(snapshot(database, "documents") == expected, "ordinary VACUUM retains full values");
        (void)g_engine.vacuumFull(database, "documents");
        check(snapshot(database, "documents") == expected, "VACUUM FULL retains full values");
        TableSchema bounded; bounded.tablename = "bounds";
        bounded.append(makeIntColumn("id", false, 4, true));
        bounded.append(makeVarCharColumn("body", false, 12));
        require(g_engine.createTable(database, bounded) == DBStatus::OK, "bounded declaration control");
        check(g_engine.insertRow(database, "bounds", {{"id", "1"}, {"body", std::string(13, 'a')}})
              == DBStatus::STRING_DATA_RIGHT_TRUNCATION, "bounded ASCII width still rejects excess");
        const std::string twelve = "é中🙂é中🙂é中🙂é中🙂";
        check(g_engine.insertRow(database, "bounds", {{"id", "2"}, {"body", twelve}}) == DBStatus::OK,
              "bounded UTF8 width counts characters");
        check(g_engine.insertRow(database, "bounds", {{"id", "3"}, {"body", twelve + "a"}})
              == DBStatus::STRING_DATA_RIGHT_TRUNCATION, "bounded UTF8 width still rejects excess");
        check(g_engine.insertRow(database, "bounds", {{"id", "4"}, {"body", twelve + "   "}})
              == DBStatus::OK, "bounded excess spaces retain existing semantics");
        check(snapshot(database, "bounds") == Rows{{"2", {{"id", "2"}, {"body", twelve}}},
              {"4", {{"id", "4"}, {"body", twelve}}}}, "bounded full values remain exact");
        check(g_engine.truncateTable(database, "documents") == DBStatus::OK, "truncate large TEXT table");
        expected = {{"40", {{"id", "40"}, {"body", payload(1048573, 11)},
                              {"other", "fresh"}, {"bounded", "ok"}}}};
        check(g_engine.insert(database, "documents", {{"id", "40"}, {"body", *expected["40"]["body"]},
              {"other", "fresh"}, {"bounded", "ok"}}) == DBStatus::OK,
              "legacy insert accepts 1MiB TEXT after TRUNCATE");
        check(snapshot(database, "documents") == expected, "rewritten generation retains full 1MiB TEXT");
        std::cout << "LARGE_TEXT_COMPLETE checks=" << checks << " failures=" << failures << '\n';
        return failures ? 1 : 0;
    } catch (const DbError& e) { std::cerr << "FATAL " << e.sqlState() << ' ' << e.what() << '\n'; return 1; }
      catch (const std::exception& e) { std::cerr << "FATAL " << e.what() << '\n'; return 1; }
}
