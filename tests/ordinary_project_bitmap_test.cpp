// Real physical TableScan -> Project NULL bitmap ownership.
// Parent generation: -DOPB_HAS_NEW_SCOPE=0; current production defaults to 1.
// Only scope-specific controls are omitted by the before build. All projection,
// fallback, demand and raw mechanism observations run in both generations.
// test_link_flags: -Wl,--wrap=_ZNK4dbms10BufferPool20refersToCurrentFilesEv
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/StoredRowNull.h"
#include "commands/TableManage.h"
#include "common/Config.h"
#include "executor/ExecutionPlan.h"
#include "storage/BufferPool.h"
#include "test_utils.h"

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#ifndef OPB_HAS_NEW_SCOPE
#define OPB_HAS_NEW_SCOPE 1
#endif

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

namespace {
using Engine = dbms::StorageEngine;
using Cell = std::optional<std::string>;
using Model = std::map<int, std::vector<Cell>>;
thread_local uint64_t* observedLookups = nullptr;

struct CountLookups {
    uint64_t* old;
    explicit CountLookups(uint64_t& output) : old(observedLookups) {
        observedLookups = &output;
    }
    ~CountLookups() { observedLookups = old; }
};

void need(bool good, const std::string& message) {
    if (!good) throw std::runtime_error(message);
}

std::string hex(const std::string& bytes) {
    static const char digits[] = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (unsigned char value : bytes) {
        out.push_back(digits[value >> 4]); out.push_back(digits[value & 15]);
    }
    return out;
}

struct RestoreNullBinding {
    Engine::NullRowBindingState old = Engine::captureNullRowBinding();
    ~RestoreNullBinding() { Engine::restoreNullRowBinding(std::move(old)); }
};

// An actual-child capability decorator: no synthetic row, RID, origin/schema,
// provider or parameter frame. It intentionally exposes the ORIGINAL absent
// bitmap capability, while forwarding the real source and NULL predicate.
class BitmapOpaqueRealScan final : public dbms::Operator {
public:
    explicit BitmapOpaqueRealScan(dbms::OpPtr actual) : actual_(std::move(actual)) {}
    bool open() override { clearError(); return actual_->open(); }
    bool next(std::string& out) override {
        const bool good = actual_->next(out);
        if (!good && actual_->hasError())
            return propagateChildError(actual_.get(), "actual scan failed");
        return good;
    }
    void close() override { actual_->close(); }
    bool lastColumnIsNull(size_t i) const override { return actual_->lastColumnIsNull(i); }
    ScanOrigin scanOrigin() const override { return actual_->scanOrigin(); }
private:
    dbms::OpPtr actual_;
};

struct ProjectResult {
    Model cells;
    std::map<int, int64_t> rids;
    uint64_t lookupCalls = 0;
    std::vector<uint64_t> nextLookupCalls; // Includes final false next().
};

std::vector<size_t> ordinals(const dbms::TableSchema& schema,
                             const std::set<std::string>& selected) {
    std::vector<size_t> out;
    for (size_t i = 0; i < schema.len; ++i)
        if (selected.empty() || selected.count(schema.cols[i].dataName)) out.push_back(i);
    return out;
}

Model selectModel(const Model& model, const std::vector<size_t>& columns) {
    Model result;
    for (const auto& [key, row] : model) {
        std::vector<Cell> cells;
        for (size_t i : columns) cells.push_back(row.at(i));
        result.emplace(key, std::move(cells));
    }
    return result;
}

ProjectResult project(const std::string& db, const dbms::TableSchema& schema,
                      const std::set<std::string>& selected, const std::string& label,
                      bool opaque = false, bool filter = false) {
    RestoreNullBinding binding;
    auto physical = std::make_unique<dbms::TableScanOp>(&g_engine, db, schema.tablename);
    auto* actualScan = physical.get();
    dbms::OpPtr child = std::move(physical);
    if (opaque) child = std::make_unique<BitmapOpaqueRealScan>(std::move(child));
    if (filter) {
        Engine::Condition condition;
        condition.colName = "id"; condition.op = ">="; condition.value = "1";
        child = std::make_unique<dbms::FilterOp>(std::move(child), schema,
            std::vector<Engine::Condition>{condition});
    }
    dbms::ProjectOp output(std::move(child), schema, selected);
    need(output.open(), label + " actual project open");
    struct Close {
        dbms::ProjectOp& node;
        bool armed = true;
        ~Close() noexcept { if (armed) { try { node.close(); } catch (...) {} } }
    } close{output};
    ProjectResult result;
    std::string rendered;
    while (true) {
        uint64_t calls = 0;
        bool good;
        { CountLookups count(calls); good = output.next(rendered); }
        result.lookupCalls += calls;
        result.nextLookupCalls.push_back(calls);
        if (!good) break;
        std::vector<std::string> cells;
        std::vector<bool> nulls;
        need(output.lastStructuredRow(cells, nulls), label + " structured row available");
        need(cells.size() == nulls.size() && !cells.empty() && !nulls.front(), label + " actual id present");
        const int key = std::stoi(cells.front());
        const auto origin = actualScan->scanOrigin();
        need(origin.engine == &g_engine && origin.dbname == db && origin.tablename == schema.tablename && origin.rid > 0,
             label + " actual heap origin");
        std::vector<bool> bitmap;
        need(actualScan->lastStoredNullBitmap(bitmap) && bitmap.size() == schema.len,
             label + " actual owned bitmap exported");
        std::vector<Cell> row;
        for (size_t i = 0; i < cells.size(); ++i) row.push_back(nulls[i] ? Cell{} : Cell{cells[i]});
        need(result.cells.emplace(key, row).second && result.rids.emplace(key, origin.rid).second,
             label + " no duplicate actual primary key");
        std::cout << "PROJECT_ROW label=" << label << " id=" << key << " rid=" << origin.rid << " cells=";
        for (const auto& cell : row) std::cout << (cell ? "H" + hex(*cell) : "N") << ',';
        std::cout << " bitmap=";
        for (bool null : bitmap) std::cout << (null ? '1' : '0');
        std::cout << '\n';
    }
    need(!output.hasError(), label + " completed without child error");
    close.armed = false;
    output.close();
    std::cout << "LOOKUP_OBSERVATION label=" << label << " calls=" << result.lookupCalls << " next_calls=";
    for (auto calls : result.nextLookupCalls) std::cout << calls << ',';
    std::cout << " rows=" << result.cells.size() << " has_new_scope=" << OPB_HAS_NEW_SCOPE << '\n';
    return result;
}

struct OwnedRow {
    int key = 0;
    int64_t rid = 0;
    std::string raw;
    std::vector<bool> nulls;
};

std::vector<OwnedRow> ownRows(const std::string& db, const dbms::TableSchema& schema,
                              uint64_t* scanOnlyCalls = nullptr) {
    RestoreNullBinding binding;
    dbms::TableScanOp source(&g_engine, db, schema.tablename);
    need(source.open(), "actual owner scan open");
    struct Close {
        dbms::TableScanOp& node;
        bool armed = true;
        ~Close() noexcept { if (armed) { try { node.close(); } catch (...) {} } }
    } close{source};
    std::vector<OwnedRow> rows;
    std::string raw;
    while (true) {
        bool good;
        if (scanOnlyCalls) { CountLookups count(*scanOnlyCalls); good = source.next(raw); }
        else good = source.next(raw);
        if (!good) break;
        OwnedRow row;
        row.key = std::stoi(Engine::extractColumnValueStatic(raw, schema, 0));
        row.rid = source.lastRid(); row.raw = raw;
        need(source.lastStoredNullBitmap(row.nulls) && row.nulls.size() == schema.len && row.rid > 0,
             "physical row/bitmap/RID captured from same true next");
        rows.push_back(std::move(row));
    }
    need(!source.hasError(), "owner scan completed");
    close.armed = false;
    source.close();
    return rows;
}

void bind(const std::string& db, const dbms::TableSchema& schema, const OwnedRow& row) {
    Engine::bindNullRow(&g_engine, db, schema.tablename, row.rid, schema.len);
}

void checkCell(const std::string& db, const dbms::TableSchema& schema,
               const OwnedRow& row, size_t ordinal, const Cell& expected,
               bool computeVirtual = false) {
    bool isNull = false;
    const auto value = g_engine.extractColumnValue(row.raw, schema, ordinal, db, computeVirtual, &isNull);
    need(isNull == !expected && (!expected || value == *expected), "actual extraction bytes/NULL mismatch");
}

std::string sqlstate(const std::function<void()>& action) {
    try { action(); }
    catch (const dbms::DbError& error) { return error.sqlState(); }
    return {};
}
} // namespace

extern "C" bool __real__ZNK4dbms10BufferPool20refersToCurrentFilesEv(const dbms::BufferPool*);
extern "C" bool __wrap__ZNK4dbms10BufferPool20refersToCurrentFilesEv(const dbms::BufferPool* pool) {
    if (observedLookups) ++*observedLookups;
    return __real__ZNK4dbms10BufferPool20refersToCurrentFilesEv(pool);
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name = "ordinary_project_bitmap", db = testDbPath(name);
    cleanupTestDb(name);
    auto* oldSession = currentSession();
    const bool oldAnalyze = g_config.autoAnalyzeEnabled;
    struct Restore {
        Session* old; bool analyze;
        ~Restore() { Engine::unbindNullRow(); setCurrentSession(old); g_config.autoAnalyzeEnabled = analyze; }
    } restore{oldSession, oldAnalyze};
    g_config.autoAnalyzeEnabled = false;
    Session session; session.username = "admin"; session.permission = 1; session.currentDB = db;
    setCurrentSession(&session);
    size_t checked = 0, failed = 0, scopeSkipped = 0;
    auto run = [&](const char* label, const std::function<void()>& action) {
        ++checked;
        try { action(); std::cout << "OPB_CASE " << checked << ' ' << label << " PASS\n"; }
        catch (const std::exception& error) { ++failed; std::cerr << "OPB_CASE " << checked << ' ' << label << " FAIL " << error.what() << '\n'; }
    };
    try {
        need(g_engine.createDatabase(db, "utf8") == DBStatus::OK, "actual private database create");
        DdlExecutor ddl;
        auto create = [&](const std::string& sql) { need(!ddl.executeSql(sql, session), "actual DDL failed: " + sql); };
        create("CREATE TABLE opb_native_fixed(id integer PRIMARY KEY,nn integer NOT NULL,n integer,m integer)");
        create("CREATE TABLE opb_native_values(id integer PRIMARY KEY,nn integer NOT NULL,n integer,tag varchar(64),pad varchar(16384))");
        create("CREATE TABLE opb_native_empty(id integer PRIMARY KEY,nn integer NOT NULL,n integer,m integer)");
        create("CREATE TABLE opb_native_demand(id integer PRIMARY KEY,n integer,good integer GENERATED ALWAYS AS(n+1) VIRTUAL,bad integer GENERATED ALWAYS AS(1/(id-id)) VIRTUAL)");
        Model fixed, values;
        const std::string longA = std::string(8192, 'A') + std::string(1024, 'B');
        const std::string longB = std::string(10240, 'z');
        for (int id = 1; id <= 8; ++id) {
            Cell n = id % 2 ? Cell{} : Cell{id == 2 ? "0" : id == 4 ? "2147483647" : id == 6 ? "-2147483648" : "7"};
            Cell m = id % 2 ? Cell{"0"} : Cell{};
            Cell tag = id % 4 == 1 ? Cell{""} : id % 4 == 2 ? Cell{} : id % 4 == 3 ? Cell{"NULL"} : Cell{"é中"};
            Cell pad = id == 1 ? Cell{longA} : id == 2 ? Cell{} : id == 3 ? Cell{"__TOAST__1"} : id == 4 ? Cell{""} : id == 5 ? Cell{longB} : id == 6 ? Cell{"NULL"} : id == 7 ? Cell{} : Cell{"é\nline\t"};
            fixed[id] = {Cell{std::to_string(id)}, Cell{std::to_string(100 + id)}, n, m};
            values[id] = {Cell{std::to_string(id)}, Cell{std::to_string(100 + id)}, n, tag, pad};
            need(g_engine.insertRow(db, "opb_native_fixed", {{"id", std::to_string(id)}, {"nn", std::to_string(100 + id)}, {"n", n}, {"m", m}}) == DBStatus::OK, "fixed insert");
            need(g_engine.insertRow(db, "opb_native_values", {{"id", std::to_string(id)}, {"nn", std::to_string(100 + id)}, {"n", n}, {"tag", tag}, {"pad", pad}}) == DBStatus::OK, "values insert");
        }
        need(g_engine.insertRow(db, "opb_native_demand", {{"id", "1"}, {"n", std::nullopt}}) == DBStatus::OK, "bad virtual remains unselected during insert1");
        need(g_engine.insertRow(db, "opb_native_demand", {{"id", "2"}, {"n", "2"}}) == DBStatus::OK, "bad virtual remains unselected during insert2");
        const auto fs = g_engine.getTableSchema(db, "opb_native_fixed");
        const auto vs = g_engine.getTableSchema(db, "opb_native_values");
        const auto es = g_engine.getTableSchema(db, "opb_native_empty");
        const auto ds = g_engine.getTableSchema(db, "opb_native_demand");
        const auto actualFixed = ownRows(db, fs);
        const auto actualValues = ownRows(db, vs);
        const auto actualDemand = ownRows(db, ds);
        need(actualFixed.size() == 8 && actualValues.size() == 8 && actualDemand.size() == 2, "all real seed rows present");
        const auto& a = actualFixed.at(0);
        const auto& b = actualFixed.at(1);
        need(a.key == 1 && b.key == 2 && a.nulls.at(2) && !b.nulls.at(2), "two actual opposite bitmap rows");
        run("wrap-positive-real-cache-hit", [&] {
            need(g_engine.getPageAllocator(db, fs.tablename) != nullptr, "warm actual allocator");
            uint64_t calls = 0;
            { CountLookups count(calls); need(g_engine.getPageAllocator(db, fs.tablename) != nullptr, "actual cache hit"); }
            need(calls > 0, "external BufferPool observer actually hit");
            std::cout << "LOOKUP_CANARY calls=" << calls << '\n';
        });
        run("direct-full-alternating-NULLs", [&] {
            auto result = project(db, fs, {}, "direct_full"); need(result.cells == fixed, "full structured cells exact");
            std::set<int64_t> rids;
            for (const auto& row : actualFixed) { need(result.rids.at(row.key) == row.rid, "same true actual RID"); rids.insert(row.rid); }
            need(rids.size() == 8, "unique real heap RIDs");
        });
        run("direct-selected-nullable", [&] { const std::set<std::string> cols{"id", "n"}; need(project(db, fs, cols, "direct_nullable").cells == selectModel(fixed, ordinals(fs, cols)), "nullable subset exact"); });
        run("direct-nonnullable-only", [&] { const std::set<std::string> cols{"id", "nn"}; need(project(db, fs, cols, "direct_nonnullable").cells == selectModel(fixed, ordinals(fs, cols)), "nonnull subset exact"); });
        run("TOAST-empty-literalNULL-marker-UTF8", [&] { need(project(db, vs, {}, "direct_toast_values").cells == values, "full real long/NULL/empty/marker bytes"); });
        run("zero-row-no-projection-demand", [&] { need(project(db, es, {}, "empty").cells.empty(), "empty actual table"); });
        run("raw-mechanism-not-a-clock-or-budget-pass", [&] {
            uint64_t baseline = 0;
            need(ownRows(db, fs, &baseline).size() == 8, "scan-only positive population");
            const auto full = project(db, fs, {}, "mechanism_nullable");
            const auto nonnull = project(db, fs, {"id", "nn"}, "mechanism_nonnullable");
            need(full.cells == fixed && nonnull.cells == selectModel(fixed, ordinals(fs, {"id", "nn"})), "mechanism full semantic equality");
            std::cout << "MECHANISM_RAW scan_next_lookups=" << baseline << " nullable_project_lookups=" << full.lookupCalls
                      << " nonnullable_project_lookups=" << nonnull.lookupCalls << " actual_rows=8 has_new_scope=" << OPB_HAS_NEW_SCOPE
                      << " comparison_status=REQUIRES_BOTH_ACTUAL_GENERATION_RECEIPTS no_speed_claim=1\n";
        });
        run("actual-child-no-bitmap-fallback", [&] { need(project(db, fs, {}, "opaque_fallback", true).cells == fixed, "original fallback NULLs/values"); });
        run("real-Filter-child-fallback", [&] { need(project(db, fs, {}, "filter_fallback", false, true).cells == fixed, "real Filter forwarding unchanged"); });
        run("open-close-reopen-no-prior-row-bitmap", [&] {
            const auto first = project(db, fs, {}, "reopen_first");
            const auto second = project(db, fs, {}, "reopen_second");
            need(first.cells == fixed && second.cells == fixed && first.rids == second.rids, "same actual reopened population/RIDs");
        });
        run("unselected-invalid-virtual", [&] {
            const Model expected{{1,{Cell{"1"},Cell{}}},{2,{Cell{"2"},Cell{"2"}}}};
            need(project(db, ds, {"id","n"}, "virtual_unselected").cells == expected, "no bad virtual demand");
        });
        run("selected-good-virtual-preserves-actualNULL", [&] {
            const Model expected{{1,{Cell{"1"},Cell{}}},{2,{Cell{"2"},Cell{"3"}}}};
            need(project(db, ds, {"id","good"}, "virtual_good").cells == expected, "good virtual exact/NULL");
        });
        run("selected-error-unwinds-then-opposite-row", [&] {
            need(sqlstate([&] { (void)project(db, ds, {"id","bad"}, "virtual_bad"); }) == "22012", "selected virtual original division error");
            need(project(db, fs, {}, "after_actual_error").cells == fixed, "error did not leak buffered NULL ownership");
        });
#if OPB_HAS_NEW_SCOPE
        run("scope-nested-same-real-schema", [&] {
            RestoreNullBinding binding; bind(db, fs, b);
            detail::ScopedStoredRowNullBitmap outer(fs, a.nulls);
            checkCell(db, fs, a, 2, Cell{});
            std::vector<std::string> decoded; std::vector<bool> nulls;
            detail::decodeStoredRowValues(g_engine, db, b.raw, fs, b.nulls, decoded, nulls);
            need(nulls == b.nulls && decoded.at(2) == "0", "existing private decoder used true other row");
            checkCell(db, fs, a, 2, Cell{});
        });
        run("scope-nested-different-real-schema", [&] {
            RestoreNullBinding binding; bind(db, fs, b);
            detail::ScopedStoredRowNullBitmap outer(fs, a.nulls);
            const auto& other = actualValues.at(1);
            std::vector<std::string> decoded; std::vector<bool> nulls;
            detail::decodeStoredRowValues(g_engine, db, other.raw, vs, other.nulls, decoded, nulls);
            need(nulls == other.nulls && nulls.at(3) && nulls.at(4), "actual other schema true NULLs");
            checkCell(db, fs, a, 2, Cell{});
        });
        run("scope-inner-actual-generated-error-restores-outer", [&] {
            RestoreNullBinding binding; bind(db, fs, b);
            detail::ScopedStoredRowNullBitmap outer(fs, a.nulls);
            const auto& bad = actualDemand.at(0);
            need(sqlstate([&] {
                RestoreNullBinding nested; bind(db, ds, bad);
                detail::ScopedStoredRowNullBitmap inner(ds, bad.nulls);
                checkCell(db, ds, bad, 3, Cell{"unreachable"}, true);
            }) == "22012", "real evaluation threw inside inner scope");
            checkCell(db, fs, a, 2, Cell{});
        });
        run("scope-invalid-width-is-inactive", [&] {
            RestoreNullBinding binding; bind(db, fs, b);
            detail::ScopedStoredRowNullBitmap outer(fs, a.nulls);
            auto invalid = b.nulls; invalid.pop_back();
            { detail::ScopedStoredRowNullBitmap inactive(fs, invalid); checkCell(db, fs, a, 2, Cell{}); }
            checkCell(db, fs, a, 2, Cell{});
        });
        run("scope-noncurrent-format-admission-only", [&] {
            RestoreNullBinding binding; bind(db, fs, b);
            detail::ScopedStoredRowNullBitmap outer(fs, a.nulls);
            auto rejectedDeclaration = fs; rejectedDeclaration.formatVersion = 1;
            // Guard component only: no format1 file/data is created or decoded.
            { detail::ScopedStoredRowNullBitmap inactive(rejectedDeclaration, b.nulls); checkCell(db, fs, a, 2, Cell{}); }
            checkCell(db, fs, a, 2, Cell{});
        });
        run("scope-destruction-restores-realRID-fallback", [&] {
            RestoreNullBinding binding; bind(db, fs, b);
            { detail::ScopedStoredRowNullBitmap outer(fs, a.nulls); checkCell(db, fs, a, 2, Cell{}); }
            checkCell(db, fs, b, 2, Cell{"0"});
        });
#else
        scopeSkipped = 6;
        std::cout << "SCOPE_CONTROLS capability_absent_before=6 NOTRUN_not_PASS=1\n";
#endif
        need(g_engine.dropDatabase(db) == DBStatus::OK, "owned database normal cleanup");
        cleanupTestDb(name);
    } catch (const std::exception& error) {
        ++failed; std::cerr << "OPB_FATAL setup_or_cleanup " << error.what() << '\n';
    }
    std::cout << "OPB_RESULT cases=" << checked << " failures=" << failed << " scope_capability_absent=" << scopeSkipped
              << " mechanism_is_raw_lookup_count_not_clock=1 has_new_scope=" << OPB_HAS_NEW_SCOPE << '\n';
    return failed ? 1 : 0;
}
