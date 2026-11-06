// test_link_flags: -Wl,--wrap=_ZNK4dbms10BufferPool20refersToCurrentFilesEv
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "common/Config.h"
#include "executor/ExecutionPlan.h"
#include "storage/BufferPool.h"
#include "test_utils.h"
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <optional>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
extern dbms::Config g_config;
namespace {
size_t validations = 0, checks = 0, failures = 0;
using Cell = std::optional<std::string>;
using Row = std::vector<Cell>;
using Rows = std::vector<Row>;
void check(bool passed, const std::string& label) {
    ++checks;
    if (!passed) { ++failures; std::cerr << "FAIL " << label << '\n'; }
}
bool sameBinding(const dbms::StorageEngine::NullRowBindingState& a,
                 const dbms::StorageEngine::NullRowBindingState& b) {
    return a.engine == b.engine && a.database == b.database && a.table == b.table &&
           a.rid == b.rid && a.natts == b.natts;
}
size_t sortFiles() {
    size_t n = 0;
    for (const auto& e : std::filesystem::directory_iterator("/proc/self/fd")) {
        std::error_code error;
        const auto target = std::filesystem::read_symlink(e.path(), error).string();
        n += !error && target.find("/.dbms-sort-") != std::string::npos;
    }
    return n;
}
// A real Sort subclass must retain the ordinary virtual consumer path.
class ObservedSort final : public dbms::SortOp {
public:
    using SortOp::SortOp;
    size_t calls = 0;
    bool next(std::string& raw) override { ++calls; return SortOp::next(raw); }
};
}

extern "C" bool realCurrentFiles(const dbms::BufferPool*)
    asm("__real__ZNK4dbms10BufferPool20refersToCurrentFilesEv");
extern "C" bool wrapCurrentFiles(const dbms::BufferPool*)
    asm("__wrap__ZNK4dbms10BufferPool20refersToCurrentFilesEv");
extern "C" bool wrapCurrentFiles(const dbms::BufferPool* pool) {
    ++validations;
    return realCurrentFiles(pool);
}

int main() {
    using namespace dbms;
    try {
        TypeRegistry::instance().bootstrap();
        const auto db = testDbPath("sorted_projection_null_bitmap_cost");
        Session session; session.username = "admin"; session.permission = 1; session.currentDB = db;
        struct Restore {
            Session* session; size_t budget; bool analyze;
            ~Restore() { setCurrentSession(session); g_config.workMemKb = budget; g_config.autoAnalyzeEnabled = analyze; }
        } restore{currentSession(), g_config.workMemKb, g_config.autoAnalyzeEnabled};
        setCurrentSession(&session); g_config.autoAnalyzeEnabled = false;
        if (g_engine.createDatabase(db, "utf8") != DBStatus::OK) throw std::runtime_error("create database");
        DdlExecutor ddl;
        for (const auto* sql : {
             "CREATE TABLE sp_values(id integer PRIMARY KEY,k integer,payload text,bytes bytea)",
             "CREATE TABLE sp_virtual(id integer PRIMARY KEY,k integer,good integer GENERATED ALWAYS AS(k+1) VIRTUAL,bad integer GENERATED ALWAYS AS(1/(id-id)) VIRTUAL)"})
            if (ddl.executeSql(sql, session)) throw std::runtime_error(sql);
        Rows model;
        for (size_t i = 0; i < 64; ++i) {
            Cell key = i % 7 ? Cell{std::to_string(int(i % 11) - 5)} : Cell{};
            Cell payload = i % 8 == 0 ? Cell{} : i % 8 == 1 ? Cell{""} :
                i % 8 == 2 ? Cell{"NULL"} : i % 8 == 3 ? Cell{"é中 two words"} :
                i % 8 == 4 ? Cell{"__TOAST__1"} : Cell{std::string(9216 + i, char('a' + i % 23))};
            Cell bytes = i % 5 == 0 ? Cell{} : i % 5 == 1 ? Cell{"\\x"} : Cell{"\\x004e554c4cff0a20"};
            Row row{std::to_string(i), key, payload, bytes};
            if (g_engine.insertRow(db, "sp_values", {{"id", row[0]}, {"k", key}, {"payload", payload}, {"bytes", bytes}}) != DBStatus::OK)
                throw std::runtime_error("insert real values");
            model.push_back(std::move(row));
        }
        for (const auto* id : {"1", "2"})
            if (g_engine.insertRow(db, "sp_virtual", {{"id", std::string(id)}, {"k", *id == '1' ? Cell{} : Cell{"2"}}}) != DBStatus::OK)
                throw std::runtime_error("insert virtual inputs");
        const auto schema = g_engine.getTableSchema(db, "sp_values");
        const auto virt = g_engine.getTableSchema(db, "sp_virtual");
        StorageEngine::unbindNullRow();
        const auto caller = StorageEngine::captureNullRowBinding();
        for (const size_t budget : {size_t(1), size_t(4096)}) {
            g_config.workMemKb = budget;
            for (const bool descending : {false, true}) for (int mode = 0; mode != 4; ++mode) {
                // Mode 0/1 prove the captured bitmap works across both ordinary
                // physical and filtered sort inputs; 2/3 are fallback controls.
                OpPtr source = std::make_unique<TableScanOp>(&g_engine, db, schema.tablename);
                if (mode == 1) source = std::make_unique<FilterOp>(std::move(source), schema, std::vector<StorageEngine::Condition>{});
                ObservedSort* observed = nullptr;
                OpPtr sorted;
                if (mode == 3) {
                    auto op = std::make_unique<ObservedSort>(std::move(source), schema, "k", !descending, true, true);
                    observed = op.get(); sorted = std::move(op);
                } else sorted = std::make_unique<SortOp>(std::move(source), schema, "k", !descending, true, true);
                auto consumer = schema;
                if (mode == 2) consumer.cols[2].isNull = !consumer.cols[2].isNull;
                check(mode != 2 || schema.cols[2].isNull != consumer.cols[2].isNull, "real schema mismatch control");
                ProjectOp project(std::move(sorted), consumer, {});
                check(project.open(), "sort/projection open");
                check((sortFiles() > 0) == (budget == 1), "actual in-memory or external sort selected");
                const size_t before = validations;
                Rows actual;
                std::string raw;
                while (project.next(raw)) {
                    std::vector<std::string> cells; std::vector<bool> nulls;
                    check(project.lastStructuredRow(cells, nulls) && cells.size() == 4 && nulls.size() == 4, "full structured row");
                    Row row;
                    for (size_t i = 0; i < cells.size(); ++i) row.push_back(nulls[i] ? Cell{} : Cell{cells[i]});
                    actual.push_back(std::move(row));
                }
                const size_t repeated = validations - before;
                auto expected = model;
                std::stable_sort(expected.begin(), expected.end(), [&](const Row& a, const Row& b) {
                    if (!a[1] || !b[1]) return !a[1] && bool(b[1]);
                    return descending ? std::stoi(*a[1]) > std::stoi(*b[1]) : std::stoi(*a[1]) < std::stoi(*b[1]);
                });
                check(actual == expected && !project.hasError(), "all sorted values, peers, NULL/empty/literalNULL/binary/LOB");
                check(mode < 2 ? repeated == 0 : repeated > 0, "bitmap reuse or guarded fallback has actual file validation count");
                check(!observed || observed->calls == 65, "subclass virtual next called for every row and EOF");
                project.close(); project.close();
                check(sortFiles() == 0 && sameBinding(caller, StorageEngine::captureNullRowBinding()), "close restores caller and releases sort files");
                std::cout << "SORTED_PROJECT_NULL budget=" << budget << " desc=" << descending << " mode=" << mode << " rows=" << actual.size() << " repeated_file_validations=" << repeated << '\n';
            }
            {
                auto sorted = std::make_unique<SortOp>(std::make_unique<TableScanOp>(&g_engine, db, schema.tablename), schema, "id", true);
                LimitOp limit(std::make_unique<ProjectOp>(std::move(sorted), schema, std::set<std::string>{"id", "payload"}), 3);
                check(limit.open(), "early LIMIT open");
                size_t count = 0; std::string raw;
                while (limit.next(raw)) ++count;
                check(count == 3 && !limit.hasError(), "early LIMIT row demand");
                limit.close();
                check(sortFiles() == 0 && sameBinding(caller, StorageEngine::captureNullRowBinding()), "early LIMIT ordinary cleanup");
            }
            {
                auto runVirtual = [&](const char* name) {
                    ProjectOp project(std::make_unique<SortOp>(std::make_unique<TableScanOp>(&g_engine, db, virt.tablename), virt, "id", true), virt, {"id", name});
                    Rows actual;
                    try {
                        check(project.open(), "virtual source open"); std::string raw;
                        while (project.next(raw)) {
                            std::vector<std::string> cells; std::vector<bool> nulls;
                            check(project.lastStructuredRow(cells, nulls), "virtual structured row");
                            Row row; for (size_t i = 0; i < cells.size(); ++i) row.push_back(nulls[i] ? Cell{} : Cell{cells[i]});
                            actual.push_back(std::move(row));
                        }
                        project.close();
                    } catch (...) { project.close(); throw; }
                    return actual;
                };
                check(runVirtual("good") == Rows{{"1", Cell{}}, {"2", "3"}}, "selected virtual nullable input remains correct");
                bool error = false;
                try { (void)runVirtual("bad"); } catch (const DbError& e) { error = e.sqlState() == "22012"; }
                check(error, "selected virtual division still errors22012");
                check(sortFiles() == 0 && sameBinding(caller, StorageEngine::captureNullRowBinding()), "virtual ordinary close restores scope");
            }
        }
        std::cout << "SORTED_PROJECT_NULL checks=" << checks << " failures=" << failures << '\n';
        return failures ? 1 : 0;
    } catch (const DbError& e) { std::cerr << "FATAL " << e.sqlState() << ' ' << e.what() << '\n'; return 1; }
      catch (const std::exception& e) { std::cerr << "FATAL " << e.what() << '\n'; return 1; }
}
