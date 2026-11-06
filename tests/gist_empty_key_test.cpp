// Existing APIs, complete real physical cells and actual RIDs.
// Independent strict V2/V3 framing; no new header API is needed for before ABI.
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "commands/StoredRowNull.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "catalog/type_registry.h"
#include "common/Config.h"
#include "utils/Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
using namespace dbms;
using Cell = std::optional<std::string>;
using Row = std::vector<Cell>;
using Model = std::map<int, Row>;
const std::string database = testDbPath("gist_empty_existing_api");
const std::string table = "empty_rows";
size_t checks = 0, failures = 0;
uint32_t keyOid = 25;

std::string hex(const std::string& value) {
    constexpr char digits[] = "0123456789abcdef";
    std::string result;
    for (unsigned char byte : value) { result += digits[byte >> 4]; result += digits[byte & 15]; }
    return result;
}
void check(bool good, const std::string& label) {
    ++checks;
    if (!good) { ++failures; std::cerr << "GIST_EMPTY_FAILED " << std::quoted(label) << '\n'; }
}
void need(bool good, const std::string& label) {
    if (!good) throw std::runtime_error(label);
}
std::string quote(const std::string& value) {
    std::string result = "'";
    for (const char ch : value) { result += ch; if (ch == '\'') result += ch; }
    return result + "'";
}
std::string bytes(const std::filesystem::path& path) {
    std::ifstream stream(path, std::ios::binary);
    need(bool(stream), "open actual image: " + path.string());
    const std::string result{std::istreambuf_iterator<char>(stream), {}};
    need(!stream.bad(), "read complete actual image"); return result;
}
uint32_t crc(const std::string& value, size_t size) {
    uint32_t state = 0xffffffffu;
    for (size_t i = 0; i < size; ++i) {
        state ^= static_cast<unsigned char>(value[i]);
        for (unsigned bit = 0; bit < 8; ++bit) state = (state >> 1) ^ ((state & 1) ? 0x82f63b78u : 0u);
    }
    return state ^ 0xffffffffu;
}
struct Encoded { uint64_t rid; std::string low, high; };
// Independently decode frozen20Bheader +24Bentryfixed +payload +CRC32C.
bool existingImage(const std::string& image, uint32_t& version, std::vector<Encoded>& rows) {
    if (image.size() < 24) return false;
    size_t position = 0;
    const auto number = [&](size_t width, uint64_t& out) {
        if (position + width > image.size() - 4) return false;
        out = 0; for (size_t i = 0; i < width; ++i) out |= uint64_t(static_cast<unsigned char>(image[position++])) << (8 * i);
        return true;
    };
    uint32_t trailer = 0; for (size_t i = 0; i < 4; ++i) trailer |= uint32_t(static_cast<unsigned char>(image[image.size() - 4 + i])) << (8 * i);
    if (crc(image, image.size() - 4) != trailer) return false;
    const bool magic2 = image.substr(0, 8) == std::string("\0DBMSS\0002", 8);
    const bool magic3 = image.substr(0, 8) == std::string("\0DBMSS\0003", 8);
    if (!magic2 && !magic3) return false;
    position = 8; uint64_t v = 0, count = 0;
    if (!number(4, v) || !number(8, count)) return false;
    version = uint32_t(v);
    if ((v != 2 && v != 3) || (magic2 && v != 2) || (magic3 && v != 3) || count > (image.size() - 24) / 24) return false;
    std::set<uint64_t> seen;
    for (uint64_t i = 0; i < count; ++i) {
        uint64_t rid = 0, a = 0, b = 0;
        if (!number(8, rid) || !number(8, a) || !number(8, b) || !rid || !seen.insert(rid).second ||
            a > image.size() - 4 - position || b > image.size() - 4 - position - a) return false;
        Encoded entry{rid, image.substr(position, size_t(a)), image.substr(position + size_t(a), size_t(b))};
        position += size_t(a + b); rows.push_back(std::move(entry));
    }
    return position == image.size() - 4;
}
Model seed() {
    Model model;
    for (int id = 1; id <= 128; ++id) {
        Cell key;
        if (id != 2 && id % 17) key = id == 1 || id % 13 == 0 ? "" : id == 3 ? "NULL" :
            id == 4 ? " " : id == 5 ? "界\n|;," : id == 6 ? std::string(1, '\x7f') : "key" + std::to_string(id % 11);
        Cell pad = id % 11 ? Cell("payload 界\n|;," + std::to_string(id) + std::string(id % 47 ? 64 : 12000, 'x')) : Cell{};
        model.emplace(id, Row{std::to_string(id), key, pad});
    }
    return model;
}
Model finalModel() {
    auto model = seed();
    model.emplace(193, Row{"193", "", "new empty"});
    model.emplace(194, Row{"194", Cell{}, "new NULL"});
    model.emplace(195, Row{"195", "omega", Cell{}});
    model.at(3)[1] = ""; model.at(1)[1].reset(); model.at(2)[1] = "beta"; model.at(4)[1] = "a";
    model.erase(13); model.erase(17); model.erase(10); return model;
}
bool match(const Row& row, const std::string& op, const std::string& value) {
    if (op == "IS NULL") return !row[1];
    if (op == "IS NOT NULL") return bool(row[1]);
    if (!row[1]) return false;
    const auto& key = *row[1];
    if (op == "=") return key == value;
    if (op == "<") return key < value;
    if (op == "<=") return key <= value;
    if (op == ">") return key > value;
    if (op == ">=") return key >= value;
    throw std::runtime_error("unknown independent oracle operation");
}
Model selected(const Model& model, const std::string& op, const std::string& value) {
    Model result; for (const auto& entry : model) if (match(entry.second, op, value)) result.insert(entry); return result;
}
void dump(const std::string& label, const Model& model) {
    std::cout << "GIST_EMPTY_ROWS " << std::quoted(label) << " count=" << model.size() << '\n';
    for (const auto& entry : model) {
        std::cout << "ROW " << entry.first;
        for (const auto& cell : entry.second) std::cout << ' ' << bool(cell) << ':' << (cell ? hex(*cell) : "-");
        std::cout << '\n';
    }
}
std::map<int, int64_t> physical(const Model& wanted, const std::string& phase) {
    const auto schema = g_engine.getTableSchema(database, table);
    Model actual; std::map<int, int64_t> rids;
    need(g_engine.forEachRow(database, table, [&](uint32_t page, uint16_t slot, const char* data, size_t length) {
        const auto rid = StorageEngine::encodeRid(page, slot);
        const auto nulls = detail::storedRowNullBitmap(g_engine, database, table, schema, rid);
        need(nulls && nulls->size() == schema.len, "real tuple/RID bitmap");
        Row row; const std::string value(data, length);
        for (size_t i = 0; i < 3; ++i) row.push_back((*nulls)[i] ? Cell{} : Cell(g_engine.extractColumnValue(value, schema, i, database)));
        if (schema.len == 4 && schema.cols[3].dataName == "extra") {
            check(!(*nulls)[3] && g_engine.extractColumnValue(value, schema, 3, database).empty(), phase + ":real added-column empty DEFAULT distinct from NULL");
        }
        need(bool(row[0]), "actual nonNULL primary identity"); const int id = std::stoi(*row[0]);
        need(actual.emplace(id, row).second && rids.emplace(id, rid).second, "unique physical row/RID");
        std::cout << "GIST_EMPTY_PHYSICAL " << std::quoted(phase) << ' ' << id << " rid=" << rid << " keyNULL=" << (*nulls)[1] << '\n';
    }), "actual visible heap scan");
    dump(phase + ":physical", actual); check(actual == wanted, phase + ":full physical values/NULL"); return rids;
}
void image(const Model& wanted, const std::map<int, int64_t>& rids, const std::string& phase) {
    const auto raw = bytes(g_engine.dbPath(database) / (table + "_k.gist"));
    uint32_t version = 0; std::vector<Encoded> entries;
    const bool decoded = existingImage(raw, version, entries);
    std::cout << "GIST_EMPTY_IMAGE " << std::quoted(phase) << " version=" << version << " bytes=" << raw.size() << " entries=" << entries.size() << " fullhex=" << hex(raw) << '\n';
    check(decoded, phase + ":complete independently decoded current image");
    check(version == 3, phase + ":positive complete-empty storage version3 required");
    if (!decoded) return;
    for (const auto& entry : wanted) {
        const auto rid = rids.at(entry.first); const auto& key = entry.second[1];
        const bool found = std::any_of(entries.begin(), entries.end(), [&](const auto& e) { return e.rid == uint64_t(rid) && key && e.low == *key && e.high == *key; });
        check(key ? found : std::none_of(entries.begin(), entries.end(), [&](const auto& e) { return e.rid == uint64_t(rid); }), phase + ":encoded trueempty/nonempty vs realNULL id=" + std::to_string(entry.first));
    }
}
void legacy(const Model& wanted, const std::string& op, const std::string& value, const std::string& phase) {
    const auto condition = op == "IS NULL" ? "isnullk" : op == "IS NOT NULL" ? "isnotnullk" : op + "k " + quote(value);
    std::vector<std::vector<std::string>> rows; std::vector<std::vector<bool>> nulls; std::vector<int64_t> rids;
    (void)g_engine.query(database, table, {condition}, {"id", "k", "pad"}, {}, false, false, false, 0, {}, &rows, &nulls, &rids);
    need(rows.size() == nulls.size() && rows.size() == rids.size(), "legacy actual parallel outputs"); Model actual;
    for (size_t i = 0; i < rows.size(); ++i) {
        need(rows[i].size() == 3 && nulls[i].size() == 3, "legacy complete3fields"); Row row;
        for (size_t j = 0; j < 3; ++j) row.push_back(nulls[i][j] ? Cell{} : Cell(rows[i][j]));
        need(row[0] && actual.emplace(std::stoi(*row[0]), row).second, "legacy unique actual identity");
        std::cout << "GIST_EMPTY_LEGACY_RID " << rids[i] << '\n';
    }
    dump(phase + ":legacy:" + op + ":" + hex(value), actual);
    check(actual == selected(wanted, op, value), phase + ":legacy full rows:" + op + ":" + hex(value));
}
void prepared(const Model& wanted, const std::string& op, const std::string& value, const std::string& phase) {
    const auto predicate = op == "IS NULL" || op == "IS NOT NULL" ? "k " + op : "k" + op + quote(value);
    const std::string sql = "SELECT id,k,pad FROM " + table + " WHERE " + predicate;
    auto query = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database, sql));
    auto plan = QueryPlanner::buildPreparedQueryPlan(&g_engine, database, query, query->ast.get());
    auto cursor = QueryPlanner::makePreparedCursor(std::move(plan), query->output);
    const auto& descriptor = cursor->descriptor();
    need(descriptor.size() == 3, "actual bound descriptor width");
    check(descriptor[0].name == "id" && descriptor[0].typeOid == 23 && descriptor[1].name == "k" && descriptor[1].typeOid == keyOid && descriptor[2].name == "pad" && descriptor[2].typeOid == 25, phase + ":actual bound complete descriptor");
    for (const auto& column : descriptor) std::cout << "GIST_EMPTY_DESCRIPTOR " << hex(column.name) << ' ' << column.typeOid << ' ' << column.typeMod << ' ' << hex(column.type) << '\n';
    Model actual; std::vector<ExprValue> cells;
    try { while (cursor->next(cells)) {
        need(cells.size() == 3, "actual cursor row width"); Row row;
        for (const auto& cell : cells) row.push_back(cell.isNull ? Cell{} : Cell(cell.value));
        need(row[0] && actual.emplace(std::stoi(*row[0]), row).second, "prepared unique actual identity");
    } cursor->close(); } catch (...) { cursor->close(); throw; }
    dump(phase + ":prepared:" + op + ":" + hex(value), actual);
    check(actual == selected(wanted, op, value), phase + ":prepared full rows:" + op + ":" + hex(value));
}
void verify(const Model& model, const std::string& phase) {
    const auto rids = physical(model, phase); image(model, rids, phase);
    for (const auto& op : {"=", "<", "<=", ">", ">="}) {
        for (const auto& value : {std::string(), std::string("key5"), std::string(1, '\x7f')}) {
            legacy(model, op, value, phase); prepared(model, op, value, phase);
        }
    }
    for (const auto& op : {"IS NULL", "IS NOT NULL"}) { legacy(model, op, "", phase); prepared(model, op, "", phase); }
    // Backward-compatible original string API: both empty bounds mean open.
    const auto actual = g_engine.giSTSearchOverlap(database, table, "k", "", "");
    std::set<int64_t> expected; for (const auto& e : model) if (e.second[1]) expected.insert(rids.at(e.first));
    check(std::set<int64_t>(actual.begin(), actual.end()) == expected, phase + ":legacy string empty bounds stay unbounded and excludeNULL");
}
void insert(const Row& row) {
    need(g_engine.insertRow(database, table, {{"id", row[0]}, {"k", row[1]}, {"pad", row[2]}}) == DBStatus::OK, "actual insertRow");
}
void lifecycle(Session& session, DdlExecutor& ddl, Model& model) {
    for (const auto& row : {Row{"193", "", "new empty"}, Row{"194", Cell{}, "new NULL"}, Row{"195", "omega", Cell{}}}) { insert(row); model.emplace(std::stoi(*row[0]), row); }
    verify(model, "insert");
    for (const auto& change : std::vector<std::pair<int, Cell>>{{3, ""}, {1, Cell{}}, {2, "beta"}, {4, "a"}}) {
        need(g_engine.updateRows(database, table, {{"k", change.second}}, {"=id " + std::to_string(change.first)}) == DBStatus::OK, "actual updateRows PK predicate"); model.at(change.first)[1] = change.second;
    }
    verify(model, "update");
    for (const int id : {13, 17, 10}) { need(g_engine.removeRows(database, table, {"=id " + std::to_string(id)}) == DBStatus::OK, "actual removeRows"); model.erase(id); }
    verify(model, "delete");
    const auto saved = model;
    need(g_engine.beginTransaction(database) == DBStatus::OK, "actual transaction");
    need(g_engine.updateRows(database, table, {{"k", ""}}, {"=id 27"}) == DBStatus::OK, "transaction actual empty key"); model.at(27)[1] = "";
    insert(Row{"300", "", "uncommitted empty"}); model.emplace(300, Row{"300", "", "uncommitted empty"});
    need(g_engine.removeRows(database, table, {"=id 39"}) == DBStatus::OK, "transaction remove empty"); model.erase(39);
    need(g_engine.reindex(database, table) == DBStatus::OK, "actual transaction REINDEX"); verify(model, "transaction-reindex");
    need(g_engine.rollbackTransaction() == DBStatus::OK, "actual rollback"); model = saved; verify(model, "rollback");
    (void)g_engine.vacuum(database, table); verify(model, "vacuum");
    bool full = false; (void)g_engine.vacuumFull(database, table, &full); need(full, "actual VACUUM FULL"); verify(model, "vacuum-full");
    need(!ddl.executeSql("ALTER TABLE empty_rows ADD COLUMN extra text DEFAULT ''", session), "actual ADD COLUMN"); verify(model, "add-column");
    need(!ddl.executeSql("ALTER TABLE empty_rows DROP COLUMN extra", session), "actual DROP COLUMN"); verify(model, "drop-column");
    need(!ddl.executeSql("ALTER TABLE empty_rows ALTER COLUMN k TYPE varchar(128)", session), "actual ALTER type"); keyOid = 1043; verify(model, "varchar");
    need(!ddl.executeSql("ALTER TABLE empty_rows ALTER COLUMN k TYPE text", session), "actual ALTER back"); keyOid = 25; verify(model, "text");
    need(g_engine.reindex(database, table) == DBStatus::OK, "actual final REINDEX"); verify(model, "reindex");
    need(!ddl.executeSql("DROP INDEX empty_rows_gist", session), "actual DROP INDEX");
    need(!ddl.executeSql("CREATE INDEX empty_rows_gist ON empty_rows USING gist(k)", session), "actual recreate named index"); verify(model, "recreate-index");
    need(g_engine.checkpoint(database), "actual final checkpoint");
    const auto backup = testDbPath("gist_empty_existing_backup");
    need(g_engine.physicalBackup(database, backup), "actual ordinary public backup");
    insert(Row{"999", "", "post-backup new row"});
    need(g_engine.physicalRestore(database, backup), "actual ordinary restore same existing target"); verify(model, "backup-restore");
    need(model == finalModel(), "independent final model"); need(g_engine.checkpoint(database), "normal terminal checkpoint");
}
} // namespace
int main(int argc, char** argv) try {
    using namespace dbms; TypeRegistry::instance().bootstrap();
    g_config.autoAnalyzeEnabled = false; g_config.autoVacuumEnabled = false; g_engine.setBackgroundIntervals(60000, 60000);
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = database;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* p; ~Restore() { if (g_engine.inTransaction()) (void)g_engine.rollbackTransaction(); setCurrentSession(p); } } restore{previous};
    const std::string mode = argc > 1 ? argv[1] : "all";
    if (mode == "cold") { need(g_engine.recoverAllDatabases(), "actual fresh-process cold recovery"); verify(finalModel(), "cold"); }
    else {
        need(mode == "all" || mode == "diagnostic", "fixed mode"); need(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "actual database");
        DdlExecutor ddl; need(!ddl.executeSql("CREATE TABLE empty_rows(id integer PRIMARY KEY,k text COLLATE \"C\",pad text)", session), "actual physical declaration");
        auto model = seed(); for (const auto& e : model) insert(e.second);
        need(!ddl.executeSql("CREATE INDEX empty_rows_gist ON empty_rows USING gist(k)", session), "actual named CREATE INDEX");
        verify(model, "create-index");
        if (mode == "all") lifecycle(session, ddl, model);
    }
    std::cout << "GIST_EMPTY_EXISTING_API checks=" << checks << " failures=" << failures << " mode=" << mode << " V3_adapter_pending=0\n";
    return failures ? 1 : 0;
} catch (const std::exception& error) { std::cerr << "GIST_EMPTY_DRAFT_FATAL " << error.what() << '\n'; return 2; }
