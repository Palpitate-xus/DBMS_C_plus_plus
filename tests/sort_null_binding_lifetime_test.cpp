#include "executor/ExecutionPlan.h"
#include "catalog/type_registry.h"
#include "Config.h"
#include "test_utils.h"

#include <cassert>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;

namespace {
using Binding = dbms::StorageEngine::NullRowBindingState;

size_t sortDescriptors() {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
        std::error_code error;
        const auto name = std::filesystem::read_symlink(entry.path(), error).string();
        count += !error && name.find("/.dbms-sort-") != std::string::npos;
    }
    return count;
}

bool noSortPaths() {
    for (const auto& entry : std::filesystem::directory_iterator("."))
        if (entry.path().filename().string().rfind(".dbms-sort-", 0) == 0) return false;
    return true;
}

bool sameBinding(const Binding& left, const Binding& right) {
    return left.engine == right.engine && left.database == right.database &&
        left.table == right.table && left.rid == right.rid && left.natts == right.natts;
}

void installBinding(const Binding& binding) {
    auto copy = binding;
    dbms::StorageEngine::restoreNullRowBinding(std::move(copy));
}

bool locksEmpty() {
    return g_engine.getLockManager().captureCheckpoint().tableCounts.empty();
}

constexpr size_t rowCount = 64;

std::optional<std::string> key(size_t id) {
    if (id % 7 == 0) return std::nullopt;
    return std::to_string((rowCount - 1 - id) % 13);
}

std::optional<std::string> payload(size_t id) {
    if (id % 9 == 0) return std::nullopt;
    if (id % 9 == 1) return std::string{};
    if (id % 9 == 2) return std::string("NULL");
    return "heap row " + std::to_string(id) + " " + std::string(256, 'a' + id % 23);
}

struct StoredRow {
    std::string raw;
    Binding binding;
};

// Every saved provenance value comes from an actual committed heap scan.
std::vector<StoredRow> readRows(const std::string& database, const dbms::TableSchema& schema) {
    dbms::TableScanOp scan(&g_engine, database, schema.tablename);
    assert(scan.open());
    std::vector<StoredRow> rows;
    std::string raw;
    while (scan.next(raw)) {
        const auto id = std::stoul(g_engine.extractColumnValue(raw, schema, 0, database));
        if (rows.size() <= id) rows.resize(id + 1);
        rows[id] = {raw, dbms::StorageEngine::captureNullRowBinding()};
    }
    assert(!scan.hasError());
    scan.close();
    return rows;
}

enum class Fault { None, OpenFalse, OpenThrow, NextFalse, NextThrow, CloseThrow, NextAndCloseThrow };

struct HeapEvents {
    Fault fault = Fault::None;
    size_t opens = 0, emitted = 0, closes = 0;
    std::exception_ptr primary = std::make_exception_ptr(dbms::DbError("22012", "heap source primary failure"));
};

// Faults wrap a real scan: locking, origin and NULL metadata are never invented.
class ObservedHeapSource final : public dbms::Operator {
    dbms::TableScanOp scan_;
    HeapEvents& events_;
public:
    ObservedHeapSource(const std::string& database, const std::string& table, HeapEvents& events)
        : scan_(&g_engine, database, table), events_(events) {}
    bool open() override {
        clearError(); ++events_.opens; events_.emitted = 0;
        if (!scan_.open()) return propagateChildError(&scan_, "heap scan could not open");
        if (events_.fault == Fault::OpenThrow) std::rethrow_exception(events_.primary);
        if (events_.fault == Fault::OpenFalse) { setError("heap source open failure"); return false; }
        return true;
    }
    bool next(std::string& raw) override {
        if (events_.emitted == 16) {
            if (events_.fault == Fault::NextThrow || events_.fault == Fault::NextAndCloseThrow)
                std::rethrow_exception(events_.primary);
            if (events_.fault == Fault::NextFalse) { setError("heap source next failure"); return false; }
        }
        if (!scan_.next(raw)) {
            if (scan_.hasError()) return propagateChildError(&scan_, "heap scan failed");
            return false;
        }
        ++events_.emitted;
        return true;
    }
    bool lastColumnIsNull(size_t ordinal) const override { return scan_.lastColumnIsNull(ordinal); }
    bool lastStoredNullBitmap(std::vector<bool>& nulls) const override { return scan_.lastStoredNullBitmap(nulls); }
    ScanOrigin scanOrigin() const override { return scan_.scanOrigin(); }
    void close() override {
        ++events_.closes;
        scan_.close();
        if (events_.fault == Fault::CloseThrow || events_.fault == Fault::NextAndCloseThrow)
            throw dbms::DbError("22023", "heap source close failure");
    }
};
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto originalBudget = g_config.workMemKb;
    auto originalBinding = StorageEngine::captureNullRowBinding();
    StorageEngine::unbindNullRow();
    const auto database = testDbPath("sort_null_binding_lifetime");
    assert(g_engine.createDatabase(database, "utf8") == DBStatus::OK);
    TableSchema schema; schema.tablename = "sort_rows"; schema.formatVersion = DATA_FILE_FORMAT_VERSION;
    schema.append(makeIntColumn("id", false, 2));
    schema.append(makeIntColumn("k", true, 2));
    schema.append(makeTextColumn("payload", true));
    TableSchema callerSchema; callerSchema.tablename = "caller_rows";
    callerSchema.formatVersion = DATA_FILE_FORMAT_VERSION;
    callerSchema.append(makeIntColumn("id", false, 2));
    callerSchema.append(makeTextColumn("payload", true));
    assert(g_engine.createTable(database, schema) == DBStatus::OK);
    assert(g_engine.createTable(database, callerSchema) == DBStatus::OK);
    assert(g_engine.beginTransaction(database) == DBStatus::OK);
    for (size_t id = 0; id < rowCount; ++id)
        assert(g_engine.insertRow(database, schema.tablename,
            {{"id", std::to_string(id)}, {"k", key(id)}, {"payload", payload(id)}}) == DBStatus::OK);
    assert(g_engine.insertRow(database, callerSchema.tablename,
        {{"id", "0"}, {"payload", std::nullopt}}) == DBStatus::OK);
    assert(g_engine.insertRow(database, callerSchema.tablename,
        {{"id", "1"}, {"payload", std::string("NULL")}}) == DBStatus::OK);
    assert(g_engine.commitTransaction() == DBStatus::OK);
    schema = g_engine.getTableSchema(database, schema.tablename);
    callerSchema = g_engine.getTableSchema(database, callerSchema.tablename);
    const auto rows = readRows(database, schema);
    const auto callers = readRows(database, callerSchema);
    assert(rows.size() == rowCount && callers.size() == 2 && locksEmpty());
    assert(callers[0].binding.table != schema.tablename && callers[0].binding.rid > 0 &&
        callers[1].binding.rid > 0 && callers[0].binding.rid != callers[1].binding.rid);
    std::vector<size_t> ordered;
    for (size_t id = 0; id < rowCount; ++id) ordered.push_back(id);

    const auto callerValid = [&](size_t id) {
        bool isNull = false;
        const auto value = g_engine.extractColumnValue(callers[id].raw, callerSchema, 1, database, true, &isNull);
        return sameBinding(StorageEngine::captureNullRowBinding(), callers[id].binding) &&
            isNull == (id == 0) && (id == 0 ? value.empty() : value == "NULL");
    };
    const auto rowValid = [&](SortOp& sort, const std::string& raw, size_t id) {
        const auto origin = sort.scanOrigin();
        std::vector<bool> nulls;
        const auto expectedKey = key(id), expectedPayload = payload(id);
        bool keyNull = false, payloadNull = false;
        const auto actualKey = g_engine.extractColumnValue(raw, schema, 1, database, true, &keyNull);
        const auto actualPayload = g_engine.extractColumnValue(raw, schema, 2, database, true, &payloadNull);
        return g_engine.extractColumnValue(raw, schema, 0, database) == std::to_string(id) &&
            origin.engine == &g_engine && origin.dbname == database && origin.tablename == schema.tablename &&
            origin.rid == rows[id].binding.rid &&
            sameBinding(StorageEngine::captureNullRowBinding(), rows[id].binding) &&
            sort.lastStoredNullBitmap(nulls) && nulls == std::vector<bool>({false, !expectedKey, !expectedPayload}) &&
            !sort.lastColumnIsNull(0) && sort.lastColumnIsNull(1) == !expectedKey &&
            sort.lastColumnIsNull(2) == !expectedPayload && keyNull == !expectedKey &&
            payloadNull == !expectedPayload && actualKey == expectedKey.value_or("") &&
            actualPayload == expectedPayload.value_or("");
    };
    const auto makeSort = [&](HeapEvents& events) {
        // A unique key makes row identity assertions independent of peer order;
        // nullable k/payload still exercise the buffered physical NULL bitmap.
        return std::make_unique<SortOp>(std::make_unique<ObservedHeapSource>(database, schema.tablename, events),
            schema, "id", true);
    };
    size_t checked = 0, failures = 0;
    const auto check = [&](const char* label, bool passed) {
        ++checked; failures += !passed;
        std::cout << "SORT_NULL_BINDING budget_kb=" << g_config.workMemKb << " " << label
                  << " passed=" << passed << '\n';
    };

    for (const auto budget : {size_t(1), size_t(1024)}) {
        g_config.workMemKb = budget;
        const auto expectedFiles = budget == 1 ? 1U : 0U;
        const auto clean = [&] { return sortDescriptors() == 0 && noSortPaths() && locksEmpty(); };
        {
            HeapEvents events;
            installBinding(callers[0].binding);
            {
                auto sort = makeSort(events);
                installBinding(callers[1].binding);
                sort->close(); sort->close();
            }
            check("unopened close and destructor preserve later caller", callerValid(1) &&
                events.opens == 0 && events.closes == 0 && clean());
        }
        {
            HeapEvents events; installBinding(callers[0].binding);
            auto sort = makeSort(events); std::string raw;
            bool good = sort->open() && sortDescriptors() == expectedFiles && locksEmpty() &&
                sort->next(raw) && rowValid(*sort, raw, ordered[0]);
            sort->close();
            check("early close restores complete caller and metadata", good && callerValid(0) &&
                events.opens == 1 && events.closes == 1 && clean());
            installBinding(callers[1].binding); sort->close(); sort->close();
            check("duplicate close preserves newly installed caller", callerValid(1) && events.closes == 1 && clean());
            good = sort->open() && sortDescriptors() == expectedFiles;
            size_t count = 0;
            while (sort->next(raw)) {
                good = count < ordered.size() && rowValid(*sort, raw, ordered[count]) && good;
                ++count;
            }
            good = good && count == rowCount && !sort->hasError(); sort->close();
            check("full reopen restores its new caller", good && callerValid(1) &&
                events.opens == 2 && events.closes == 2 && clean());
        }
        {
            HeapEvents events; installBinding(callers[0].binding); bool good = false;
            {
                auto sort = makeSort(events); std::string raw;
                good = sort->open() && sort->next(raw) && rowValid(*sort, raw, ordered[0]) &&
                    sortDescriptors() == expectedFiles;
            }
            check("active destructor restores caller and releases files", good && callerValid(0) &&
                events.closes == 1 && clean());
        }
        {
            HeapEvents events; installBinding(callers[0].binding); bool good = false;
            {
                auto sort = makeSort(events); std::string raw;
                good = sort->open() && sort->next(raw) && rowValid(*sort, raw, ordered[0]);
                sort->close(); good = callerValid(0) && good;
                installBinding(callers[1].binding);
            }
            check("closed destructor preserves later caller", good && callerValid(1) &&
                events.closes == 1 && clean());
        }
        {
            HeapEvents outerEvents, innerEvents; installBinding(callers[0].binding);
            auto outer = makeSort(outerEvents); std::string outerRaw, innerRaw;
            bool good = outer->open() && outer->next(outerRaw) && rowValid(*outer, outerRaw, ordered[0]);
            const auto outerBinding = StorageEngine::captureNullRowBinding();
            const auto outerFiles = sortDescriptors();
            auto inner = makeSort(innerEvents);
            good = inner->open() && inner->next(innerRaw) && rowValid(*inner, innerRaw, ordered[0]) && good;
            inner->close();
            check("nested close restores active outer row", good && sameBinding(outerBinding,
                StorageEngine::captureNullRowBinding()) && rowValid(*outer, outerRaw, ordered[0]) &&
                sortDescriptors() == outerFiles && innerEvents.closes == 1 && locksEmpty());
            good = outer->next(outerRaw) && rowValid(*outer, outerRaw, ordered[1]);
            outer->close();
            check("outer resumes then restores original caller", good && callerValid(0) &&
                outerEvents.closes == 1 && clean());
        }
        {
            HeapEvents events; StorageEngine::unbindNullRow();
            const auto unbound = StorageEngine::captureNullRowBinding();
            auto sort = makeSort(events); std::string raw; bool good = sort->open(); size_t count = 0;
            while (sort->next(raw)) {
                good = count < ordered.size() && rowValid(*sort, raw, ordered[count]) && good;
                ++count;
            }
            sort->close();
            check("full close restores absent caller", good && count == rowCount &&
                sameBinding(StorageEngine::captureNullRowBinding(), unbound) && clean());
        }
        {
            HeapEvents events; installBinding(callers[0].binding);
            auto sort = makeSort(events); std::string raw;
            bool good = sort->open() && sort->next(raw) && rowValid(*sort, raw, ordered[0]);
            good = sort->open() && sort->next(raw) && rowValid(*sort, raw, ordered[0]) && good;
            sort->close();
            check("active reopen retires prior ownership once", good && callerValid(0) &&
                events.opens == 2 && events.closes == 2 && clean());
        }
        for (const auto fault : {Fault::OpenFalse, Fault::OpenThrow, Fault::NextFalse,
                                Fault::NextThrow, Fault::CloseThrow, Fault::NextAndCloseThrow}) {
            HeapEvents events; events.fault = fault; installBinding(callers[0].binding);
            auto sort = makeSort(events); bool opened = false, primaryIdentity = true;
            std::string state, message;
            try { opened = sort->open(); }
            catch (const DbError& error) {
                state = error.sqlState(); message = error.message();
                if (fault != Fault::CloseThrow) primaryIdentity = std::current_exception() == events.primary;
            }
            catch (...) { state = "UNEXPECTED"; }
            std::string unused;
            bool good = !opened && callerValid(0) && clean() && !sort->next(unused);
            try { sort->close(); sort->close(); } catch (...) { good = false; }
            if (fault == Fault::OpenFalse || fault == Fault::NextFalse) {
                const auto expected = fault == Fault::OpenFalse ? "heap source open failure" : "heap source next failure";
                good = good && state.empty() && sort->hasError() && sort->errorMessage() == expected;
            } else if (fault == Fault::CloseThrow) {
                good = good && state == "22023" && message == "heap source close failure";
            } else {
                good = good && state == "22012" && message == "heap source primary failure" && primaryIdentity;
            }
            good = good && callerValid(0) && events.opens == 1 && events.closes == 1 && clean();
            installBinding(callers[1].binding); sort.reset();
            const auto label = fault == Fault::OpenFalse ? "failed child open restores caller" :
                fault == Fault::OpenThrow ? "throwing child open restores caller" :
                fault == Fault::NextFalse ? "failed child next restores caller" :
                fault == Fault::NextThrow ? "throwing child next preserves primary and caller" :
                fault == Fault::CloseThrow ? "throwing child close restores caller" :
                "secondary close failure preserves primary and caller";
            check(label, good && callerValid(1) && events.closes == 1 && clean());
        }
    }
    StorageEngine::unbindNullRow();
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    g_config.workMemKb = originalBudget;
    StorageEngine::restoreNullRowBinding(std::move(originalBinding));
    assert(checked == 32);
    std::cout << "SORT_NULL_BINDING_COMPLETE " << checked << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
