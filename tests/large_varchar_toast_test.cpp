#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include <cstdint>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>

extern dbms::StorageEngine g_engine;
extern char** environ;
namespace {
using namespace dbms;
using Cell = std::optional<std::string>;
using Rows = std::map<std::string, StorageEngine::SqlRow>;
const std::string database = "__t_large_varchar";
size_t checks = 0, failures = 0;
void need(bool value, const char* message) { if (!value) throw std::runtime_error(message); }
void check(bool value, const char* message) {
    ++checks;
    if (!value) { ++failures; std::cout << "VARCHAR_CHECK_FAIL " << message << '\n'; }
}
std::string payload(size_t bytes, uint32_t seed) {
    const std::string alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    need(bytes >= 3, "known complete payload size");
    std::string result(bytes - 3, '\0');
    for (auto& value : result) {
        seed ^= seed << 13; seed ^= seed >> 17; seed ^= seed << 5;
        value = alphabet[seed % alphabet.size()];
    }
    return result + " \n\t";
}
std::string repeat(const std::string& value, size_t count) {
    std::string result; result.reserve(value.size() * count);
    for (size_t i = 0; i < count; ++i) result += value;
    return result;
}
Column bodyColumn(const std::string& mode) {
    if (mode == "bounded-wide") return makeVarCharColumn("body", true, 70000);
    if (mode == "legacy-UTF8") return makeVarCharColumn("body", true, 30000);
    need(mode == "unbounded", "known native VARCHAR mode");
    auto column = makeVarCharColumn("body", true, 65535);
    column.typeMod = -2; // Explicit new unbounded provenance, not an old dsize guess.
    return column;
}
Cell initialBody(const std::string& mode, unsigned id) {
    if (id == 3) return Cell{};
    if (id == 4) return std::string{};
    if (id == 5) return std::string("NULL");
    if (id == 6) return std::string("__TOAST__1");
    if (mode == "unbounded") return id == 1 ? payload(1048576, 61) : repeat("é中🙂", 10000);
    if (mode == "bounded-wide") return id == 1 ? payload(70000, 62) : repeat("中🙂", 35000);
    need(mode == "legacy-UTF8", "known UTF8 mode");
    return id == 1 ? repeat("é中🙂", 10000) : repeat("🙂", 30000);
}
std::string changedBody(const std::string& mode, bool final) {
    if (mode == "unbounded") return payload(final ? 1048576 : 200003, final ? 64 : 63);
    if (mode == "bounded-wide") return payload(70000, final ? 66 : 65);
    return repeat("é中🙂", 9999) + (final ? "xyz" : "abc");
}
Rows initial(const std::string& mode) {
    Rows result;
    for (unsigned id = 1; id <= 6; ++id)
        result[std::to_string(id)] = {{"id", std::to_string(id)}, {"body", initialBody(mode, id)},
            {"note", id % 2 ? Cell{"note_é中🙂_" + std::to_string(id)} : Cell{}}};
    return result;
}
Rows finalRows(const std::string& mode) {
    auto result = initial(mode); result.erase("6");
    result["1"]["body"] = changedBody(mode, true); result["1"]["note"] = Cell{};
    return result;
}
Rows afterTruncate(const std::string& mode) {
    return {{"40", {{"id", "40"}, {"body", changedBody(mode, true)}, {"note", "fresh"}}}};
}
Rows scan(const std::string& phase) {
    const auto schema = g_engine.getTableSchema(database, "items");
    need(schema.len == 3, "actual complete three-column schema");
    TableScanOp cursor(&g_engine, database, "items"); need(cursor.open(), "actual scan opens");
    Rows rows; std::string raw;
    while (cursor.next(raw)) {
        StorageEngine::SqlRow row;
        for (size_t i = 0; i < schema.len; ++i)
            row[schema.cols[i].dataName] = cursor.lastColumnIsNull(i) ? Cell{} :
                Cell{g_engine.extractColumnValue(raw, schema, i, database)};
        need(row["id"].has_value(), "actual nonNULL ID"); const auto id = *row["id"];
        need(rows.emplace(id, std::move(row)).second, "actual unique complete IDs");
    }
    need(!cursor.hasError(), "actual complete scan ends without errors"); cursor.close();
    // Preserve complete observed values, independently decodable outside C++.
    std::ofstream out("observed-" + phase + ".rows", std::ios::binary);
    const auto number = [&](uint32_t value) { out.write(reinterpret_cast<const char*>(&value), sizeof(value)); };
    const auto bytes = [&](const std::string& value) { number(static_cast<uint32_t>(value.size())); out.write(value.data(), value.size()); };
    number(static_cast<uint32_t>(rows.size()));
    for (const auto& [id, row] : rows) {
        bytes(id); number(static_cast<uint32_t>(row.size()));
        for (const auto& [name, value] : row) {
            bytes(name);
            if (!value) number(UINT32_MAX); else bytes(*value);
        }
    }
    out.flush(); need(bool(out), "complete observed values written to the stream");
    return rows;
}
void verifyIndexes(const Rows& expected) {
    auto* index = g_engine.getPKIndex(database, "items"); need(index != nullptr, "actual primary index");
    for (const auto& [key, row] : expected) {
        int64_t rid = -1; need(index->search(key, rid), "actual surviving primary key");
        check(g_engine.isColumnNullByRid(database, "items", rid, 1) == !row.at("body"),
              "actual body NULL bitmap follows its primary RID");
        check(g_engine.isColumnNullByRid(database, "items", rid, 2) == !row.at("note"),
              "actual note NULL bitmap follows its primary RID");
    }
}
int setup(const std::string& mode) {
    Column resolved;
    const auto error = TypeRegistry::instance().resolveColumnType(resolved, "varchar", {"70000"}, false);
    check(error.empty() && resolved.typeMod == 70000 && resolved.dsize == 65535,
          "declaration character bound is independent of nominal physical width");
    Column invalid = bodyColumn(mode); invalid.typeMod = 10485761;
    check(!TypeRegistry::instance().validateColumn(invalid).empty(), "declaration above PG maximum rejected");
    need(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "actual own native database");
    TableSchema schema; schema.tablename = "items";
    schema.append(makeIntColumn("id", false, 4, true)); schema.append(bodyColumn(mode));
    schema.append(makeTextColumn("note", true));
    const auto created = g_engine.createTable(database, schema);
    check(created == DBStatus::OK, "actual complete VARCHAR schema accepted");
    std::cout << "VARCHAR_CREATE mode=" << mode << " actual_status=" << int(created) << '\n';
    if (created != DBStatus::OK) return 1;
    const auto actual = g_engine.getTableSchema(database, "items").cols[1];
    check(actual.typeMod == schema.cols[1].typeMod && actual.dsize == schema.cols[1].dsize,
          "actual declaration survives schema serialization");
    Rows admitted;
    for (const auto& [key, row] : initial(mode)) {
        const auto status = g_engine.insertRow(database, "items", row);
        check(status == DBStatus::OK, "all complete VARCHAR boundary values admitted");
        if (status == DBStatus::OK) admitted[key] = row;
        std::cout << "VARCHAR_INSERT mode=" << mode << " id=" << key
                  << " bytes=" << (row.at("body") ? row.at("body")->size() : 0)
                  << " null=" << !row.at("body") << " actual_status=" << int(status) << '\n';
    }
    check(scan("admission") == admitted, "every actually admitted warm value and NULL is exact");
    check(admitted == initial(mode), "complete required six-row population");
    if (failures) {
        std::cout << "VARCHAR_ADMISSION_INCOMPLETE mode=" << mode << " rows=" << admitted.size()
                  << " checks=" << checks << " failures=" << failures << '\n';
        return 1; // Unsupported admission is not a cold corruption reproduction.
    }
    need(g_engine.createIndex(database, "items", "note") == DBStatus::OK &&
         g_engine.createHashIndex(database, "items", "id") == DBStatus::OK,
         "actual RID-bearing secondary and hash indexes");
    auto expected = initial(mode);
    expected["1"]["body"] = changedBody(mode, false); expected["1"]["note"] = Cell{};
    std::vector<StorageEngine::SqlRow> images; size_t affected = 0;
    const auto updateStatus = g_engine.updateRows(database, "items", {{"body", expected["1"]["body"]}, {"note", Cell{}}},
          {"=id 1"}, &images, {}, {}, &affected);
    std::cout << "VARCHAR_UPDATE mode=" << mode << " actual_status=" << int(updateStatus)
              << " affected=" << affected << " images=" << images.size() << '\n';
    check(updateStatus == DBStatus::OK && affected == 1 &&
          images.size() == 1 && images[0] == expected["1"], "optional update returns all actual bytes and NULLs");
    check(scan("after-update") == expected, "update stores all complete VARCHAR bytes");
    need(g_engine.beginTransaction(database) == DBStatus::OK, "actual native rollback transaction");
    const auto pendingStatus = g_engine.updateRows(database, "items", {{"body", changedBody(mode, true)}}, {"=id 1"});
    std::cout << "VARCHAR_PENDING_UPDATE mode=" << mode << " actual_status=" << int(pendingStatus) << '\n';
    check(pendingStatus == DBStatus::OK, "actual long pending update");
    need(g_engine.rollbackTransaction() == DBStatus::OK, "actual rollback completes");
    check(scan("after-rollback") == expected, "rollback restores every byte and NULL");
    if (mode != "unbounded") {
        const size_t limit = mode == "bounded-wide" ? 70000 : 30000;
        check(g_engine.insertRow(database, "items", {{"id", "90"}, {"body", std::string(limit + 1, 'x')},
              {"note", Cell{}}}) == DBStatus::STRING_DATA_RIGHT_TRUNCATION,
              "actual bounded VARCHAR still rejects excess nonspace characters");
        check(scan("after-rejection") == expected, "failed width check leaves actual values unchanged");
    }
    need(g_engine.remove(database, "items", {"=id 6"}) == DBStatus::OK, "actual native delete");
    expected.erase("6"); (void)g_engine.vacuum(database, "items");
    check(scan("after-vacuum") == expected, "ordinary VACUUM preserves whole VARCHAR values");
    bool full = false; const auto count = g_engine.vacuumFull(database, "items", &full);
    check(full && count == expected.size(), "actual complete VARCHAR FULL succeeds");
    check(scan("after-full") == expected, "FULL preserves all original values and NULLs");
    need(g_engine.beginTransaction(database) == DBStatus::OK, "actual post-FULL transaction");
    const auto postFullStatus = g_engine.updateRows(database, "items", {{"body", changedBody(mode, true)}}, {"=id 1"});
    std::cout << "VARCHAR_POST_FULL_UPDATE mode=" << mode << " actual_status=" << int(postFullStatus) << '\n';
    check(postFullStatus == DBStatus::OK, "actual committed long post-FULL update");
    need(g_engine.commitTransaction() == DBStatus::OK, "actual post-FULL commit");
    check(scan("after-post-full") == finalRows(mode), "post-FULL commit keeps full new bytes");
    verifyIndexes(finalRows(mode));
    std::cout << "VARCHAR_READY mode=" << mode << " checks=" << checks << " failures=" << failures
              << " full_values_NULLs=" << (failures ? "failed" : "exact") << '\n' << std::flush;
    return failures ? 1 : 0;
}
int cold(const std::string& mode, bool second) {
    const auto expected = second ? afterTruncate(mode) : finalRows(mode);
    check(scan(second ? "cold2" : "cold1") == expected, "fresh process full values and NULLs exact");
    verifyIndexes(expected);
    const auto actual = g_engine.getTableSchema(database, "items").cols[1];
    const auto declaration = bodyColumn(mode);
    check(actual.typeMod == declaration.typeMod && actual.dsize == declaration.dsize,
          "fresh process retains its actual declaration semantics");
    if (!second) {
        need(g_engine.truncateTable(database, "items") == DBStatus::OK, "actual native TRUNCATE");
        const auto row = afterTruncate(mode).at("40");
        need(g_engine.insert(database, "items", {{"id", "40"}, {"body", *row.at("body")},
             {"note", "fresh"}}) == DBStatus::OK, "legacy insert stores full value in new generation");
        check(scan("after-truncate") == afterTruncate(mode), "complete new generation value");
    }
    std::cout << "VARCHAR_COLD mode=" << mode << " second=" << second
              << " checks=" << checks << " failures=" << failures << '\n';
    return failures ? 1 : 0;
}
int child(const std::string& executable, const std::string& mode, const std::string& phase) {
    std::vector<std::string> values;
    for (char** value = environ; *value; ++value)
        if (std::string(*value).rfind("DBMS_DATA_DIR=", 0) != 0) values.emplace_back(*value);
    values.push_back("DBMS_DATA_DIR=" + std::filesystem::absolute(mode).string());
    std::vector<char*> env;
    for (auto& value : values) env.push_back(value.data());
    env.push_back(nullptr);
    char* args[] = {const_cast<char*>(executable.c_str()), const_cast<char*>("--phase"),
        const_cast<char*>(phase.c_str()), const_cast<char*>(mode.c_str()), nullptr};
    std::cout.flush(); std::cerr.flush(); const auto pid = ::fork(); need(pid >= 0, "fork own fresh native child");
    if (pid == 0) {
        if (::chdir(mode.c_str()) != 0) ::_exit(126);
        ::execve(executable.c_str(), args, env.data()); ::_exit(127);
    }
    int status = 0; pid_t waited;
    do { waited = ::waitpid(pid, &status, 0); } while (waited == -1 && errno == EINTR);
    need(waited == pid, "reap exact native child");
    const int result = WIFEXITED(status) ? WEXITSTATUS(status) : WIFSIGNALED(status) ? -WTERMSIG(status) : 255;
    std::cout << "VARCHAR_CHILD mode=" << mode << " phase=" << phase << " pid=" << pid
              << " actual_exit=" << result << " raw_wait=" << status << '\n' << std::flush;
    return result;
}
}
int main(int argc, char** argv) {
    try {
        dbms::TypeRegistry::instance().bootstrap(); g_engine.setBackgroundIntervals(60000, 60000);
        if (argc == 4 && std::string(argv[1]) == "--phase") {
            const std::string phase = argv[2], mode = argv[3];
            if (phase == "setup") return setup(mode);
            need(phase == "cold1" || phase == "cold2", "known native cold phase");
            return cold(mode, phase == "cold2");
        }
        need(argc == 1, "known parent native arguments");
        const auto executable = std::filesystem::read_symlink("/proc/self/exe").string();
        size_t stages = 0, failed = 0, notrun = 0;
        for (const std::string mode : {"bounded-wide", "unbounded", "legacy-UTF8"}) {
            need(std::filesystem::create_directory(mode), "fresh own native mode directory");
            if (child(executable, mode, "setup") != 0) { ++stages; ++failed; notrun += 2; continue; }
            ++stages;
            if (child(executable, mode, "cold1") != 0) { ++stages; ++failed; ++notrun; continue; }
            ++stages;
            if (child(executable, mode, "cold2") != 0) ++failed;
            ++stages;
        }
        std::cout << "VARCHAR_COMPLETE stages=" << stages << " failed=" << failed
                  << " notrun=" << notrun << '\n';
        return failed || notrun ? 1 : 0;
    } catch (const dbms::DbError& error) {
        std::cerr << "VARCHAR_FATAL " << error.sqlState() << ' ' << error.what() << '\n'; return 2;
    } catch (const std::exception& error) {
        std::cerr << "VARCHAR_FATAL " << error.what() << '\n'; return 2;
    }
}
