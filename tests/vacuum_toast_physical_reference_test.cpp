// test_link_flags: -Wl,--wrap=_ZNK4dbms10BufferPool20refersToCurrentFilesEv -Wl,--wrap=_ZN4dbms13PageAllocator9fetchPageEj
#include "access/BPTree.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "storage/BufferPool.h"
#include "storage/PageAllocator.h"
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
thread_local PageAllocator* watchedHeap = nullptr;
thread_local size_t heapFetches = 0;
void check(bool good, const std::string& label) {
    ++checks;
    if (!good) { ++failures; std::cerr << "FAIL " << label << '\n'; }
}
void require(bool good, const char* label) { if (!good) throw std::runtime_error(label); }
DataFileHeader readHeapHeader(const std::filesystem::path& path) {
    DataFileHeader header{};
    std::ifstream input(path, std::ios::binary);
    require(bool(input.read(reinterpret_cast<char*>(&header), sizeof(header))) &&
            header.magic == DATA_FILE_MAGIC && header.formatVersion == DATA_FILE_FORMAT_VERSION &&
            header.headerChecksum == computeDataFileHeaderChecksum(header), "actual durable heap header");
    return header;
}
void checkNoToastCandidates() {
    const auto database = testDbPath("vacuum_reference_no_toast_candidates");
    require(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "create no-candidate database");
    for (bool inlineText : {false, true}) {
        TableSchema schema;
        schema.tablename = inlineText ? "inline_text" : "fixed_width";
        schema.formatVersion = DATA_FILE_FORMAT_VERSION;
        schema.append(makeIntColumn("id", false, 4, true));
        if (inlineText) schema.append(makeTextColumn("value", true));
        else schema.append(makeIntColumn("value", true, 4));
        const auto& table = schema.tablename;
        require(g_engine.createTable(database, schema) == DBStatus::OK, "create no-candidate table");

        const std::map<std::string, Cell> expected{
            {"1", inlineText ? "small text" : "-42"}, {"2", std::nullopt}, {"3", inlineText ? "" : "0"}};
        for (const auto& [id, value] : expected) {
            require(g_engine.insertRow(database, table, {{"id", id}, {"value", value}}) == DBStatus::OK,
                    "insert no-candidate row");
        }

        const auto toastHeap = g_engine.toastDataPath(database, table);
        const auto toastIndex = g_engine.toastIndexPath(database, table);
        if (inlineText) {
            const auto header = readHeapHeader(toastHeap);
            require(header.rowSize == 0 && header.numPages == 1 && header.freeListHead == 0 &&
                    std::filesystem::file_size(toastHeap) == StorageEngine::pageSizeForFormatVersion(header.formatVersion),
                    "inline table has a header-only TOAST heap");
            BPTree index(toastIndex);
            std::vector<int64_t> entries;
            require(index.openExisting() && index.allValuesChecked(entries) && entries.empty(),
                    "inline table has a valid empty TOAST index");
        } else {
            require(!std::filesystem::exists(toastHeap) && !std::filesystem::exists(toastIndex),
                    "fixed-width table has no TOAST heap or index");
        }

        auto readRows = [&] {
            std::map<std::string, Cell> result;
            require(g_engine.forEachRow(database, table, [&](uint32_t, uint16_t, const char* data, size_t size) {
                const std::string raw(data, size);
                const auto id = g_engine.extractColumnValue(raw, schema, 0, database);
                bool isNull = false;
                auto value = g_engine.extractColumnValue(raw, schema, 1, database, true, &isNull);
                check(result.emplace(id, isNull ? Cell{} : Cell{std::move(value)}).second, table + " unique row ID");
            }), "read no-candidate table");
            return result;
        };

        watchedHeap = g_engine.getPageAllocator(database, table);
        require(watchedHeap && watchedHeap->numPages() > 1, "no-candidate table has real main heap data pages");
        heapFetches = 0;
        check(readRows() == expected, table + " rows before TOAST vacuum");
        require(heapFetches > 0, "real table scan exercises the main heap fetch counter");
        heapFetches = 0;
        const size_t removed = g_engine.vacuumToast(database, table);
        watchedHeap = nullptr;
        std::cout << "VACUUM_REFERENCE table=" << table << " main_heap_fetches=" << heapFetches << '\n';
        check(removed == 0, table + " TOAST vacuum removes no objects");
        check(heapFetches == 0, table + " TOAST vacuum performs no main heap fetches");
        check(readRows() == expected, table + " rows after TOAST vacuum");
    }
}
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
        const auto fileHeader = readHeapHeader(path);
        PageAllocator pages(path.string(), fileHeader.rowSize, 8192, fileHeader.formatVersion);
        require(pages.open(), "actual physical heap open");
        for (uint32_t pageId = 1; pageId < pages.numPages(); ++pageId) {
            char* buffer = pages.fetchPage(pageId); require(buffer != nullptr, "actual heap page");
            PageWrapper page(buffer, pages.pageSize(), fileHeader.formatVersion);
            require(page.isValid(), "valid actual heap page");
            page.forEachLive([&](uint16_t, const char* tuple, size_t length) {
            require(length >= sizeof(HeapTupleHeaderData), "actual tuple header");
            const auto* header = castHeapHeader(tuple);
            require(header->t_hoff >= sizeof(HeapTupleHeaderData) + (f.schema.len() + 7) / 8 && header->t_hoff <= length, "actual tuple body span");
            std::string raw(tuple + header->t_hoff, length - header->t_hoff);
            std::vector<bool> storedNulls(f.schema.len(), false), nulls;
            for (size_t i = 0; i < f.schema.len(); ++i) storedNulls[i] = dbms::isNull(header, int(i));
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
extern "C" char* realFetch(dbms::PageAllocator*, uint32_t) asm("__real__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator*, uint32_t) asm("__wrap__ZN4dbms13PageAllocator9fetchPageEj");
extern "C" char* wrapFetch(dbms::PageAllocator* allocator, uint32_t pageId) {
    if (allocator == watchedHeap) ++heapFetches;
    return realFetch(allocator, pageId);
}

int main() {
    using namespace dbms;
    try {
        TypeRegistry::instance().bootstrap(); g_engine.setBackgroundIntervals(60000, 60000);
        StorageEngine::unbindNullRow();
        checkNoToastCandidates();
        auto caller = fixture("vacuum_reference_caller", false);
        g_engine.vacuum(caller.database, caller.schema.tablename, false, 1, false);
        check(g_engine.readToast(caller.database, caller.schema.tablename, caller.orphan) ==
              "genuine unreferenced object", "main-only ordinary vacuum preserves a real TOAST orphan");
        check(g_engine.vacuumToast(caller.database, caller.schema.tablename) == 1,
              "explicit TOAST vacuum reclaims the preserved orphan");
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
