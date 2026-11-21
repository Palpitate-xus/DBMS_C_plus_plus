// Run in a fresh private CWD: test_stubs constructs its engine before main.
#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/StoredReferenceOutput.h"
#include "test_utils.h"
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
using Role = StoredReferenceOutputRole;
using Kind = Role::Kind;
using Cell = std::optional<std::string>;
using Row = std::vector<Cell>;
using Rows = std::vector<Row>;
unsigned checked = 0;

void need(bool good, const std::string& reason) {
    ++checked;
    if (!good) throw std::runtime_error(reason);
}
Role raw(Kind kind, const std::string& source) { return {kind, source}; }
bool sameBinding(const StorageEngine::NullRowBindingState& a,
                 const StorageEngine::NullRowBindingState& b) {
    return a.engine == b.engine && a.database == b.database && a.table == b.table &&
        a.rid == b.rid && a.natts == b.natts;
}
Row nullable(const std::vector<std::string>& cells, const std::vector<bool>& nulls) {
    need(cells.size() == nulls.size(), "actual cell/NULL widths agree");
    Row result;
    for (size_t i = 0; i < cells.size(); ++i)
        result.push_back(nulls[i] ? Cell{} : Cell{cells[i]});
    return result;
}
Rows unordered(Rows rows) { std::sort(rows.begin(), rows.end()); return rows; }

struct Capture {
    std::vector<std::string> display, keys;
    std::vector<std::vector<std::string>> cells;
    std::vector<std::vector<bool>> nulls;
    std::vector<Role> roles;
    bool rolesAvailable = false;
    Rows rows() const {
        Rows result;
        for (size_t i = 0; i < cells.size(); ++i) result.push_back(nullable(cells[i], nulls[i]));
        return result;
    }
};

// Observe the actual executing producer before and after reading its metadata.
// A getter-only fixture would miss eager output conversion inside next().
Capture consume(OpPtr plan, ProjectOp* physicalProjection = nullptr) {
    Capture result;
    result.roles = {raw(Kind::Regclass, "stale caller role")};
    const auto prior = StorageEngine::captureNullRowBinding();
    result.rolesAvailable = plan->storedOutputRoles(result.roles);
    need(sameBinding(prior, StorageEngine::captureNullRowBinding()),
         "metadata lookup before open does not rebind a stored row");
    need(result.rolesAvailable || result.roles.empty(), "unavailable metadata clears stale caller roles");
    try {
        need(plan->open(), "actual storage plan opens");
        std::vector<Role> afterOpen;
        need(plan->storedOutputRoles(afterOpen) == result.rolesAvailable && afterOpen == result.roles,
             "opening the source does not change its physical output declaration");
        std::string display;
        while (plan->next(display)) {
            std::vector<std::string> cells;
            std::vector<bool> nulls;
            need(plan->lastStructuredRow(cells, nulls), "actual projected structured row");
            std::string key;
            if (physicalProjection)
                need(physicalProjection->lastTypedRowKey(key), "actual physical projection typed key");
            const auto binding = StorageEngine::captureNullRowBinding();
            std::vector<Role> roles;
            need(plan->storedOutputRoles(roles) == result.rolesAvailable && roles == result.roles,
                 "per-row roles retain source order and do not depend on values/NULLs");
            std::vector<std::string> again;
            std::vector<bool> againNulls;
            need(plan->lastStructuredRow(again, againNulls) && again == cells && againNulls == nulls,
                 "reading roles does not alter cells or NULL metadata");
            need(sameBinding(binding, StorageEngine::captureNullRowBinding()),
                 "reading roles does not change current stored-row ownership");
            if (physicalProjection) {
                std::string againKey;
                need(physicalProjection->lastTypedRowKey(againKey) && againKey == key,
                     "producer metadata cannot change the typed equality key");
                result.keys.push_back(key);
            }
            result.display.push_back(display);
            result.cells.push_back(std::move(cells));
            result.nulls.push_back(std::move(nulls));
        }
        need(!plan->hasError(), "actual storage plan reaches normal EOF");
        plan->close();
    } catch (...) { plan->close(); throw; }
    return result;
}

OpPtr project(const std::string& database, const TableSchema& schema,
              const std::set<std::string>& selected, ProjectOp** observer = nullptr,
              const std::string& order = "id") {
    OpPtr input = std::make_unique<TableScanOp>(&g_engine, database, schema.tablename);
    if (!order.empty()) input = std::make_unique<SortOp>(std::move(input), schema, order, true);
    auto result = std::make_unique<ProjectOp>(std::move(input), schema, selected);
    if (observer) *observer = result.get();
    return result;
}

Capture query(const std::string& database, const std::string& table,
              const std::set<std::string>& selected, bool captureRoles) {
    Capture result;
    StorageEngine::OrderBySpec order;
    order.colName = "id";
    result.roles = {raw(Kind::Regclass, "stale caller role")};
    result.display = g_engine.query(database, table, {}, selected, {order},
        false, false, false, 0, {}, &result.cells, &result.nulls,
        nullptr, {}, nullptr, captureRoles ? &result.roles : nullptr);
    if (!captureRoles) result.roles.clear();
    result.rolesAvailable = !result.roles.empty();
    return result;
}

void expect(const Capture& actual, const Rows& rows, const std::vector<Role>& roles,
            const std::string& label) {
    need(actual.rows() == rows, label + ": exact raw/NULL cells");
    need(actual.rolesAvailable == !roles.empty() && actual.roles == roles,
         label + ": actual producer roles in output schema order");
}

void physicalProjection(const std::string& database, const TableSchema& schema,
                        const Session& session) {
    const std::set<std::string> selected{"z", "r", "ra", "c"};
    const std::vector<Role> roles{{}, raw(Kind::Regtype, "r"),
        raw(Kind::RegtypeArray, "ra"), raw(Kind::Regclass, "c")};
    const Rows rows{{"23", "23", "[0:2]={23,NULL,0}", "0"},
                    {"integer", "25", "{}", "0"},
                    {"-", "0", {}, {}}, {"NULL", {}, "{25}", "0"}};
    ProjectOp* actualProjection = nullptr;
    auto plan = project(database, schema, selected, &actualProjection);
    const auto actual = consume(std::move(plan), actualProjection);
    expect(actual, rows, roles, "physical Project selects columns in schema order, not set order");
    need(actual.cells[0][1] == "23" && actual.cells[1][1] == "25" && actual.cells[2][1] == "0",
         "Project never changes stored 23/25/0 to integer/text/-");
    const auto withRoles = query(database, schema.tablename, selected, true);
    const auto withoutRoles = query(database, schema.tablename, selected, false);
    expect(withRoles, rows, roles, "StorageEngine query has the same physical producer contract");
    need(withRoles.display == withoutRoles.display && withRoles.cells == withoutRoles.cells &&
         withRoles.nulls == withoutRoles.nulls && withRoles.rows() == actual.rows(),
         "requesting query provenance does not change storage extraction or legacy display bytes");

    const auto catalog = regtype_detail::metadata(g_engine, database);
    StoredReferenceOutputRenderer renderer(catalog, &session);
    auto presented = actual.cells;
    for (size_t i = 0; i < presented.size(); ++i)
        renderer.render(presented[i], actual.nulls[i], actual.roles);
    need(presented == std::vector<std::vector<std::string>>{
             {"23", "integer", "[0:2]={integer,NULL,-}", "-"},
             {"integer", "text", "{}", "-"}, {"-", "-", "", ""},
             {"NULL", "", "{text}", "-"}},
         "only the explicit outgoing copy renders genuine references; text and SQL NULLs survive");
    need(actual.rows() == rows, "final presentation leaves the captured physical inputs intact");

    const Rows mixed{{"23", "23", "integer"}, {"integer", "25", "text"},
                     {"-", "0", "-"}, {"NULL", {}, {}}};
    const std::vector<Role> mixedRoles{{}, raw(Kind::Regtype, "r"), {}};
    const auto virtualMixed = consume(project(database, schema, {"gr", "r", "z"}));
    expect(virtualMixed, mixed, mixedRoles, "raw stored and already evaluated virtual references coexist");
    auto mixedPresentation = virtualMixed.cells;
    for (size_t i = 0; i < mixedPresentation.size(); ++i)
        renderer.render(mixedPresentation[i], virtualMixed.nulls[i], virtualMixed.roles);
    need(mixedPresentation[0] == std::vector<std::string>{"23", "integer", "integer"} &&
         mixedPresentation[2] == std::vector<std::string>{"-", "-", "-"},
         "virtual reference names are never decoded a second time");
    expect(consume(project(database, schema, {"gr"})),
           {{"integer"}, {"text"}, {"-"}, {{}}}, {},
           "virtual-only projection advertises no stored reference role");
    expect(consume(project(database, schema, {"z"})),
           {{"23"}, {"integer"}, {"-"}, {"NULL"}}, {},
           "ordinary numeric-looking and literal-NULL text advertises no reference role");
    expect(query(database, schema.tablename, {"gr"}, true),
           {{"integer"}, {"text"}, {"-"}, {{}}}, {},
           "StorageEngine virtual-only output clears a stale raw role");
}

void sortedDistinct(const std::string& database, const TableSchema& schema) {
    const Rows sorted{{"0"}, {"2"}, {"16"}, {"20"}, {"23"}, {"23"},
                      {"25"}, {"4294967295"}, {{}}, {{}}};
    ProjectOp* physical = nullptr;
    auto plan = project(database, schema, {"r"}, &physical, "r");
    const auto actual = consume(std::move(plan), physical);
    expect(actual, sorted, {raw(Kind::Regtype, "r")}, "Sort compares unsigned OID datums before output");
    need(actual.keys == std::vector<std::string>{"V8:00000000", "V8:00000002", "V8:00000010",
             "V8:00000014", "V8:00000017", "V8:00000017", "V8:00000019",
             "V8:ffffffff", "N;", "N;"},
         "typed keys preserve unsigned reference identity, duplicate equality, zero and NULL distinction");
    auto chain = [&]() {
        OpPtr result = project(database, schema, {"r"}, &physical, "r");
        result = std::make_unique<DistinctOp>(std::move(result));
        result = std::make_unique<OffsetOp>(std::move(result), 1);
        return OpPtr(std::make_unique<LimitOp>(std::move(result), 5));
    };
    auto limited = chain();
    const auto retained = consume(std::move(limited), physical);
    const Rows expected{{"2"}, {"16"}, {"20"}, {"23"}, {"25"}};
    expect(retained, expected, {raw(Kind::Regtype, "r")},
           "Distinct/Offset/Limit preserve producer roles while selecting real sorted rows");
    need(retained.keys == std::vector<std::string>{"V8:00000002", "V8:00000010", "V8:00000014",
                                                   "V8:00000017", "V8:00000019"},
         "wrapper consumption retains the originating Project equality keys");
    const auto receipt = QueryPlanner::executePlanChecked(chain());
    receipt.throwIfFailed();
    need(receipt.ok && receipt.structuredRowsAvailable && receipt.storedOutputRolesAvailable &&
         receipt.storedOutputRoles == std::vector<Role>{raw(Kind::Regtype, "r")} &&
         receipt.structuredRows == retained.cells && receipt.structuredNulls == retained.nulls &&
         receipt.rows == retained.display,
         "checked real-plan execution transports roles without changing raw result frames");
}

void actualJoins(const std::string& database, const TableSchema& left, const TableSchema& right) {
    const Rows lhs{{"1", "23", "23", "[0:2]={23,NULL,0}", "0", "integer"},
                   {"2", "integer", "25", "{}", "0", "text"},
                   {"3", "-", "0", {}, {}, "-"}, {"4", "NULL", {}, "{25}", "0", {}}};
    const Rows rhs{{"1", "25", "{25,NULL}"}, {"2", "23", "{}"}, {"99", "0", {}}};
    const std::vector<Role> roles{{}, {}, raw(Kind::Regtype, "r"), raw(Kind::RegtypeArray, "ra"),
        raw(Kind::Regclass, "c"), {}, {}, raw(Kind::Regtype, "r"), raw(Kind::RegtypeArray, "ra")};
    const StorageEngine::JoinRangeNames ranges{"left occurrence", "right occurrence", true};
    for (int mode = 0; mode < 5; ++mode) {
        auto run = [&](bool metadata) {
            Capture result;
            result.roles = {raw(Kind::Regtype, "stale caller role")};
            auto* outputRoles = metadata ? &result.roles : nullptr;
            if (mode == 0) result.display = g_engine.join(database, left.tablename, right.tablename,
                "id", "id", {}, {}, &result.cells, &result.nulls, {}, ranges, outputRoles);
            if (mode == 1) result.display = g_engine.leftJoin(database, left.tablename, right.tablename,
                "id", "id", {}, {}, &result.cells, &result.nulls, {}, ranges, outputRoles);
            if (mode == 2) result.display = g_engine.rightJoin(database, left.tablename, right.tablename,
                "id", "id", {}, {}, &result.cells, &result.nulls, {}, ranges, outputRoles);
            if (mode == 3) result.display = g_engine.fullOuterJoin(database, left.tablename, right.tablename,
                "id", "id", {}, {}, &result.cells, &result.nulls, {}, ranges, outputRoles);
            if (mode == 4) result.display = g_engine.crossJoin(database, left.tablename, right.tablename,
                {}, {}, &result.cells, &result.nulls, ranges, outputRoles);
            if (!metadata) result.roles.clear();
            result.rolesAvailable = !result.roles.empty();
            return result;
        };
        Rows expected;
        const auto append = [&](const Row& a, const Row& b) {
            Row row = a; row.insert(row.end(), b.begin(), b.end()); expected.push_back(std::move(row));
        };
        for (const auto& a : lhs) {
            bool matched = false;
            for (const auto& b : rhs) if (mode == 4 || a[0] == b[0]) { append(a, b); matched = true; }
            if (!matched && (mode == 1 || mode == 3)) append(a, Row(right.len()));
        }
        if (mode == 2 || mode == 3) for (const auto& b : rhs) {
            const bool matched = std::any_of(lhs.begin(), lhs.end(), [&](const Row& a) { return a[0] == b[0]; });
            if (!matched) append(Row(left.len()), b);
        }
        const auto actual = run(true), legacy = run(false);
        need(unordered(actual.rows()) == unordered(expected), "actual JOIN mode " + std::to_string(mode) + " retains raw cells and outer NULLs");
        need(actual.rolesAvailable && actual.roles == roles, "actual JOIN roles follow left/right occurrence layout including virtual None");
        need(actual.display == legacy.display && actual.cells == legacy.cells && actual.nulls == legacy.nulls,
             "JOIN metadata collection cannot alter output bytes, match multiplicity or NULLs");
    }
    const std::set<std::string> selected{
        StorageEngine::joinRangeColumnKey(ranges.first, "z", true),
        StorageEngine::joinRangeColumnKey(ranges.first, "ra", true),
        StorageEngine::joinRangeColumnKey(ranges.first, "gr", true),
        StorageEngine::joinRangeColumnKey(ranges.second, "r", true)};
    Capture subset;
    subset.display = g_engine.join(database, left.tablename, right.tablename, "id", "id", {}, selected,
        &subset.cells, &subset.nulls, {}, ranges, &subset.roles);
    subset.rolesAvailable = !subset.roles.empty();
    need(unordered(subset.rows()) == unordered({{"23", "[0:2]={23,NULL,0}", "integer", "25"},
                                               {"integer", "{}", "text", "23"}}),
         "encoded JOIN selection binds each actual occurrence without parsing range names");
    need(subset.roles == std::vector<Role>{{}, raw(Kind::RegtypeArray, "ra"), {}, raw(Kind::Regtype, "r")},
         "JOIN selected-column roles remain in actual output schema order");
    Capture presented;
    presented.roles = {raw(Kind::Regtype, "stale caller role")};
    const std::set<std::string> virtualOnly{
        StorageEngine::joinRangeColumnKey(ranges.first, "gr", true)};
    presented.display = g_engine.join(database, left.tablename, right.tablename, "id", "id", {}, virtualOnly,
        &presented.cells, &presented.nulls, {}, ranges, &presented.roles);
    need(presented.roles.empty() && unordered(presented.rows()) == unordered({{"integer"}, {"text"}}),
         "actual JOIN virtual-only reference output clears a stale raw role and stays presented");
}
} // namespace

int main() {
    try {
        using namespace dbms;
        TypeRegistry::instance().bootstrap();
        const auto database = testDbPath("stored_reference_provenance_" + std::to_string(::getpid()));
        need(!std::filesystem::exists(database), "fresh private test database");
        need(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "actual database creation");
        Session session;
        session.username = "admin"; session.permission = 1; session.currentDB = database; session.pid = ::getpid();
        struct Restore { Session* prior = currentSession(); ~Restore() { setCurrentSession(prior); } } restore;
        setCurrentSession(&session);
        for (const auto* sql : {
            "CREATE TABLE provenance_source(id integer PRIMARY KEY,z text,r regtype,ra regtype[],c regclass,gr regtype GENERATED ALWAYS AS(r) VIRTUAL)",
            "CREATE TABLE provenance_keys(id integer PRIMARY KEY,r regtype)",
            "CREATE TABLE provenance_right(id integer PRIMARY KEY,r regtype,ra regtype[])",
            "CREATE TABLE provenance_empty(id integer,r regtype)"})
            need(!DdlExecutor{}.executeSql(sql, session), "actual source DDL: " + std::string(sql));
        const auto insert = [&](const std::string& table, const StorageEngine::SqlRow& row) {
            need(g_engine.insertRow(database, table, row) == DBStatus::OK, "actual fixture storage insert");
        };
        insert("provenance_source", {{"id", "1"}, {"z", "23"}, {"r", "integer"}, {"ra", "[0:2]={integer,NULL,-}"}, {"c", "-"}});
        insert("provenance_source", {{"id", "2"}, {"z", "integer"}, {"r", "text"}, {"ra", "{}"}, {"c", "-"}});
        insert("provenance_source", {{"id", "3"}, {"z", "-"}, {"r", "-"}, {"ra", std::nullopt}, {"c", std::nullopt}});
        insert("provenance_source", {{"id", "4"}, {"z", "NULL"}, {"r", std::nullopt}, {"ra", "{text}"}, {"c", "-"}});
        const std::vector<Cell> keys{"25", "23", "0", "20", "16", "23", {}, {}, "4294967295", "2"};
        for (size_t i = 0; i < keys.size(); ++i)
            insert("provenance_keys", {{"id", std::to_string(i + 1)}, {"r", keys[i]}});
        insert("provenance_right", {{"id", "1"}, {"r", "text"}, {"ra", "{text,NULL}"}});
        insert("provenance_right", {{"id", "2"}, {"r", "integer"}, {"ra", "{}"}});
        insert("provenance_right", {{"id", "99"}, {"r", "-"}, {"ra", std::nullopt}});
        const auto source = g_engine.getTableSchema(database, "provenance_source");
        const auto keySchema = g_engine.getTableSchema(database, "provenance_keys");
        const auto right = g_engine.getTableSchema(database, "provenance_right");
        physicalProjection(database, source, session);
        sortedDistinct(database, keySchema);
        actualJoins(database, source, right);
        const auto empty = g_engine.getTableSchema(database, "provenance_empty");
        expect(consume(project(database, empty, {"r"})), {}, {raw(Kind::Regtype, "r")},
               "zero-row physical Project retains its actual output declaration");
        expect(query(database, empty.tablename, {"r"}, true), {}, {raw(Kind::Regtype, "r")},
               "zero-row StorageEngine query retains its actual output declaration");
        std::cout << "STORED_REFERENCE_PROVENANCE_CHECKED=" << checked << " FAILED=0\n";
        return 0;
    } catch (const DbError& error) {
        std::cerr << "STORED_REFERENCE_PROVENANCE_CHECKED=" << checked << " FAIL=" << error.sqlState() << ' ' << error.what() << '\n';
    } catch (const std::exception& error) {
        std::cerr << "STORED_REFERENCE_PROVENANCE_CHECKED=" << checked << " FAIL=" << error.what() << '\n';
    }
    return 1;
}
