// test_link_flags: -Wl,--wrap=_ZNK4dbms10BufferPool20refersToCurrentFilesEv
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "storage/BufferPool.h"
#include "test_utils.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <vector>

extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
using Cell = std::optional<std::string>;
using Row = std::vector<Cell>;
size_t checks = 0, failures = 0, fileChecks = 0;
void check(bool good, const std::string& label) {
    ++checks;
    if (!good) { ++failures; std::cerr << "FAIL " << label << '\n'; }
}
void require(bool good, const char* label) { if (!good) throw std::runtime_error(label); }
bool same(const StorageEngine::NullRowBindingState& a, const StorageEngine::NullRowBindingState& b) {
    return a.engine == b.engine && a.database == b.database && a.table == b.table && a.rid == b.rid && a.natts == b.natts;
}
void install(const StorageEngine::NullRowBindingState& binding) {
    auto copy = binding; StorageEngine::restoreNullRowBinding(std::move(copy));
}
std::string payload(size_t n, uint32_t state) {
    std::string result;
    while (result.size() < n) {
        state ^= state << 13; state ^= state >> 17; state ^= state << 5;
        result += char('!' + state % 90);
    }
    return result;
}
std::string binary(size_t n) {
    static constexpr char hex[] = "0123456789abcdef";
    std::string result = "\\x";
    for (size_t i = 0; i < n; ++i) { auto v = (i * 73 + i / 13) & 255; result += hex[v >> 4]; result += hex[v & 15]; }
    return result;
}
struct Fixture {
    std::string database;
    TableSchema schema;
    std::map<std::string, Row> expected;
    std::map<uint64_t, std::string> objects;
    std::map<std::string, StorageEngine::NullRowBindingState> bindings;
    uint64_t orphan = 0;
};
std::map<std::string, Row> rows(Fixture& f, bool capture) {
    std::map<std::string, Row> result;
    std::vector<std::filesystem::path> paths;
    if (f.schema.partitionType == TableSchema::PartitionType::None)
        paths.push_back(std::filesystem::path(f.database) / (f.schema.tablename + ".dt"));
    else for (const auto& part : f.schema.rangePartitions)
        paths.push_back(std::filesystem::path(f.database) / (f.schema.tablename + "#" + part.first + ".dt"));
    // Decode the actual committed physical tuples through the existing
    // full-value codec. This oracle is independent of the maintenance
    // marker collector and of the legacy logical partition scan provider.
    for (const auto& path : paths) {
        DataFileHeader fileHeader{};
        std::ifstream input(path, std::ios::binary);
        require(bool(input.read(reinterpret_cast<char*>(&fileHeader), sizeof(fileHeader))) &&
            fileHeader.magic == DATA_FILE_MAGIC && fileHeader.formatVersion == DATA_FILE_FORMAT_VERSION &&
            fileHeader.headerChecksum == computeDataFileHeaderChecksum(fileHeader), "actual durable heap header");
        PageAllocator pages(path.string(), fileHeader.rowSize, 8192, fileHeader.formatVersion);
        require(pages.open(), "actual physical heap open");
        for (uint32_t pageId = 1; pageId < pages.numPages(); ++pageId) {
            char* buffer = pages.fetchPage(pageId); require(buffer != nullptr, "actual heap page");
            PageWrapper page(buffer, pages.pageSize(), fileHeader.formatVersion);
            require(page.isValid(), "valid actual heap page");
            page.forEachLive([&](uint16_t, const char* tuple, size_t length) {
            require(length >= sizeof(HeapTupleHeaderData), "actual tuple header");
            const auto* header = castHeapHeader(tuple);
            require(header->t_hoff >= sizeof(HeapTupleHeaderData) + (f.schema.len + 7) / 8 && header->t_hoff <= length, "actual tuple body span");
            std::string raw(tuple + header->t_hoff, length - header->t_hoff);
            std::vector<bool> storedNulls(f.schema.len, false), nulls;
            for (size_t i = 0; i < f.schema.len; ++i) storedNulls[i] = dbms::isNull(header, int(i));
            std::vector<std::string> cells;
            detail::decodeStoredRowValues(g_engine, f.database, raw, f.schema, storedNulls, cells, nulls);
            auto id = cells[0];
            Row row;
            for (size_t index : {size_t(10), size_t(11), size_t(12)}) {
                row.push_back(nulls[index] ? Cell{} : Cell{cells[index]});
                if (capture && !nulls[index]) {
                    uint64_t toast = 0;
                    if (StorageEngine::parseToastMarker(StorageEngine::extractColumnValueStatic(raw, f.schema, index), toast))
                        f.objects[toast] = g_engine.readToast(f.database, f.schema.tablename, toast);
                }
            }
            require(result.emplace(id, std::move(row)).second, "actual unique IDs");
            });
            pages.unpinPage(pageId);
        }
        pages.close();
    }
    return result;
}
Fixture fixture(const std::string& name, bool partitioned, const TableSchema* partitionDeclaration = nullptr) {
    Fixture f; f.database = testDbPath(name);
    require(g_engine.createDatabase(f.database, "utf8") == DBStatus::OK, "create database");
    if (partitioned) {
        require(partitionDeclaration != nullptr, "actual SQL descriptor for native partition control");
        auto table = *partitionDeclaration;
        table.partitionType = TableSchema::PartitionType::Range; table.partitionKey = "id";
        table.rangePartitions = {{"early", "4"}, {"later", "20"}};
        require(g_engine.createTable(f.database, table) == DBStatus::OK, "create supported native partition storage");
    } else {
        Session session; session.username = "admin"; session.permission = 1; session.currentDB = f.database;
        struct RestoreSession { Session* previous; ~RestoreSession() { setCurrentSession(previous); } } restore{currentSession()};
        setCurrentSession(&session); DdlExecutor ddl;
        std::string sql = "CREATE TABLE documents(id INTEGER PRIMARY KEY";
        for (size_t i = 1; i <= 9; ++i) sql += ",f" + std::to_string(i) + " INTEGER";
        sql += ",inline_value TEXT,payload TEXT,bytes BYTEA)";
        require(!ddl.executeSql(sql, session), "create actual SQL declaration");
    }
    f.schema = g_engine.getTableSchema(f.database, "documents");
    f.expected = {{"0", {"caller NULLs", Cell{}, Cell{}}},
        {"1", {"é中", payload(11003, 0x13579), binary(70003)}},
        {"2", {"", "NULL", "\\x"}}, {"3", {Cell{}, "", Cell{}}},
        {"4", {"two words", "__TOAST__1", binary(11009)}},
        {"5", {"NULL", payload(40003, 0x2468), Cell{}}},
        {"6", {"last row", Cell{}, "\\x004e554c4cff"}}};
    for (const auto& [id, row] : f.expected) {
        std::map<std::string, Cell> input{{"id", id}, {"inline_value", row[0]}, {"payload", row[1]}, {"bytes", row[2]}};
        for (size_t i = 1; i <= 9; ++i) input["f" + std::to_string(i)] = Cell{};
        const auto status = g_engine.insertRow(f.database, f.schema.tablename, input);
        if (status != DBStatus::OK)
            throw std::runtime_error("insert fixture " + f.database + " id=" + id + " status=" + std::to_string(static_cast<int>(status)));
    }
    require(rows(f, true) == f.expected && f.objects.size() >= 5, "all original cells and actual live TOAST objects");
    if (!partitioned) {
        TableScanOp scan(&g_engine, f.database, f.schema.tablename);
        require(scan.open(), "actual scan for caller provenance");
        std::string raw;
        while (scan.next(raw)) {
            auto id = g_engine.extractColumnValue(raw, f.schema, 0, f.database);
            auto binding = StorageEngine::captureNullRowBinding();
            require(binding.engine == &g_engine && binding.database == f.database && binding.table == f.schema.tablename && binding.rid > 0, "genuine positive caller provenance");
            f.bindings[id] = std::move(binding);
        }
        require(!scan.hasError() && f.bindings.size() == f.expected.size(), "complete actual caller bindings");
        scan.close();
    }
    f.orphan = g_engine.allocToastId(f.database, f.schema.tablename);
    require(f.orphan && !f.objects.count(f.orphan) && g_engine.writeToast(f.database, f.schema.tablename, f.orphan, "genuine unreferenced object"), "create actual unreferenced TOAST object");
    return f;
}
}
extern "C" bool realCurrentFiles(const dbms::BufferPool*) asm("__real__ZNK4dbms10BufferPool20refersToCurrentFilesEv");
extern "C" bool wrapCurrentFiles(const dbms::BufferPool*) asm("__wrap__ZNK4dbms10BufferPool20refersToCurrentFilesEv");
extern "C" bool wrapCurrentFiles(const dbms::BufferPool* pool) { ++fileChecks; return realCurrentFiles(pool); }

int main() {
    using namespace dbms;
    try {
        TypeRegistry::instance().bootstrap(); g_engine.setBackgroundIntervals(60000, 60000);
        StorageEngine::unbindNullRow(); auto caller = fixture("vacuum_reference_caller", false);
        size_t cases = 0;
        for (bool partitioned : {false, true}) for (bool wholeVacuum : {false, true}) for (int binding = 0; binding < 4; ++binding) {
            StorageEngine::unbindNullRow();
            auto f = fixture("vacuum_reference_" + std::to_string(++cases), partitioned, &caller.schema);
            // Every bound RID and NULL state comes from a real committed row.
            // Cross-database callers deliberately use the same relation name.
            if (binding == 1) install(partitioned ? caller.bindings.at("6") : f.bindings.at("0"));
            if (binding == 2) install(partitioned ? caller.bindings.at("1") : f.bindings.at("1"));
            if (binding == 3) install(caller.bindings.at("0"));
            auto before = StorageEngine::captureNullRowBinding();
            size_t initialChecks = fileChecks;
            size_t removed = wholeVacuum ? g_engine.vacuum(f.database, f.schema.tablename)
                                        : g_engine.vacuumToast(f.database, f.schema.tablename);
            size_t validations = fileChecks - initialChecks;
            check(wholeVacuum || removed == 1, "only one orphan reclaimed");
            check(same(before, StorageEngine::captureNullRowBinding()), "caller's genuine binding preserved");
            for (const auto& [id, expected] : f.objects)
                check(g_engine.readToast(f.database, f.schema.tablename, id) == expected, "every live object retains all original bytes");
            check(g_engine.readToast(f.database, f.schema.tablename, f.orphan).empty(), "actual orphan reclaimed");
            StorageEngine::unbindNullRow();
            check(rows(f, false) == f.expected, "all NULL/empty/literalNULL/marker/binary/large values retained");
            check(g_engine.vacuumToast(f.database, f.schema.tablename) == 0, "second ordinary vacuum is idempotent");
            std::cout << "VACUUM_REFERENCE case=" << cases << " partitioned=" << partitioned << " whole=" << wholeVacuum << " binding=" << binding << " objects=" << f.objects.size() << " removed=" << removed << " file_validations=" << validations << '\n';
        }
        StorageEngine::unbindNullRow();
        std::cout << "VACUUM_REFERENCE cases=" << cases << " checks=" << checks << " failures=" << failures << '\n';
        return failures ? 1 : 0;
    } catch (const DbError& e) { std::cerr << "FATAL " << e.sqlState() << ' ' << e.what() << '\n'; return 1; }
      catch (const std::exception& e) { std::cerr << "FATAL " << e.what() << '\n'; return 1; }
}
