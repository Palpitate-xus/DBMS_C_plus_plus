// ============================================================================
// parallel_exec_test — parallel JOIN / aggregate / GatherMerge:
//   ParallelGroupAggregateOp: worker-thread partitioned aggregation matches
//     the serial GroupAggregateOp output (plain + GROUP BY, incl. NULLs,
//     avg/min/max, HAVING)
//   ParallelHashJoinOp: page-range parallel build side produces the same
//     join result as HashJoinOp (and usedParallelWorkers reports the mode)
//   GatherMergeOp: k-way merge of per-worker sorted runs yields a globally
//     ordered stream; planner builds it under parallelism + ORDER BY
// ============================================================================

#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "catalog/type_registry.h"

#include <algorithm>
#include <optional>
#include <memory>
#include <array>
#include <cassert>
#include <filesystem>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "test_utils.h"

extern dbms::StorageEngine g_engine;
using dbms::DBStatus;

static void cleanupDb(const std::string& db) {
    if (g_engine.databaseExists(db)) {
        const auto status = g_engine.dropDatabase(db);
        assert(status == DBStatus::OK || status == DBStatus::DATABASE_NOT_FOUND);
    }
    std::error_code ec;
    std::filesystem::remove_all(db, ec);
}

static void setupSession(Session& s, const std::string& db) {
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
}

static std::vector<std::string> runPlan(dbms::Operator* op) {
    std::vector<std::string> rows;
    std::string row;
    assert(op->open());
    while (op->next(row)) rows.push_back(row);
    assert(!op->hasError());
    op->close();
    return rows;
}

static std::vector<std::string> sorted(std::vector<std::string> v) {
    std::sort(v.begin(), v.end());
    return v;
}

class MinMaxReplayRows final : public dbms::Operator {
public:
    struct Row { std::string raw; ScanOrigin origin; };
    explicit MinMaxReplayRows(std::vector<Row> rows) : rows_(std::move(rows)) {}
    bool open() override { position_ = 0; return true; }
    bool next(std::string& raw) override {
        if (position_ == rows_.size()) return false;
        raw = rows_[position_++].raw;
        return true;
    }
    void close() override { position_ = 0; }
    ScanOrigin scanOrigin() const override {
        return position_ ? rows_[position_ - 1].origin : ScanOrigin{};
    }
    bool lastColumnIsNull(size_t ordinal) const override {
        const auto origin = scanOrigin();
        return origin.engine && origin.engine->isColumnNullByRid(
            origin.dbname, origin.tablename, origin.rid, ordinal);
    }
private:
    std::vector<Row> rows_;
    size_t position_ = 0;
};

static bool testDeclaredMinMaxComparison() {
    using namespace dbms;
    using Value = std::optional<std::string>;
    using Bounds = std::pair<Value, Value>;
    struct Case {
        Column column;
        std::array<Value, 4> inputs;
        // Explicit global, group0, group1 MIN/MAX expectations. Do not derive
        // the oracle using the implementation's numeric/string comparator.
        std::array<Bounds, 3> expected;
    };
    TypeRegistry::instance().bootstrap();
    Column integerArray;
    integerArray.dataName = "integer_array";
    assert(TypeRegistry::instance().resolveColumnType(integerArray, "integer", {}, true).empty());
    integerArray.isNull = true;
    const std::string database = testDbPath("minmax_declared_comparison_probe_v4");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    // Use an actual SQL enum declaration, not the internal generic ENUM
    // placeholder, whose fixed index carrier is not a named label type.
    Session declarationSession;
    declarationSession.username = "testuser";
    declarationSession.permission = 1;
    declarationSession.currentDB = database;
    DdlExecutor declarations;
    assert(!declarations.executeSql("CREATE TYPE r192_enum AS ENUM ('z','a','')", declarationSession));
    assert(!declarations.executeSql("CREATE TABLE enum_declaration_probe(declared_value r192_enum)", declarationSession));
    const auto declared = g_engine.getTableSchema(database, "enum_declaration_probe");
    const std::vector<std::string> enumLabels{"z", "a", ""};
    assert(declared.len == 1 && declared.cols[0].enumValues == enumLabels);
    Column enumColumn = declared.cols[0];
    enumColumn.dataName = "enum_order";
    assert(enumColumn.isNull && !enumColumn.isArray);
    const Bounds numericText{"10", "2"};
    const Bounds nanText{"Infinity", "NaN"};
    const Bounds positive{"9007199254740992", "9007199254740993"};
    const Bounds negative{"-9007199254740993", "-9007199254740992"};
    const Bounds decimal{"9007199254740993.0000", "9007199254740993.0001"};
    const Bounds real{"-Infinity", "NaN"};
    const Bounds dbl{"1.5", "NaN"};
    const Bounds ordinary{"-10", "2"};
    const Bounds onlyNull{std::nullopt, std::nullopt};
    const Bounds onlyNan{"NaN", "NaN"};
    std::vector<Case> cases = {
        {makeTextColumn("numeric_text", true), {"2", "10", "10", "2"}, {numericText, numericText, numericText}},
        {makeTextColumn("special_text", true), {"NaN", "Infinity", "Infinity", "NaN"}, {nanText, nanText, nanText}},
        {makeTextColumn("empty_text", true), {"", "0", "NULL", std::nullopt},
            {Bounds{"", "NULL"}, Bounds{"", "NULL"}, Bounds{"0", "0"}}},
        {makeIntColumn("positive_bigint", true, 4),
            {"9007199254740993", "9007199254740992", "9007199254740992", "9007199254740993"},
            {positive, positive, positive}},
        {makeIntColumn("negative_bigint", true, 4),
            {"-9007199254740992", "-9007199254740993", "-9007199254740993", "-9007199254740992"},
            {negative, negative, negative}},
        {makeDecimalColumn("exact_decimal", true, 20, 4),
            {"9007199254740993.0001", "9007199254740993.0000", "9007199254740993.0000", "9007199254740993.0001"},
            {decimal, decimal, decimal}},
        {makeFloatColumn("real_value", true), {"-Infinity", "Infinity", "NaN", "1.5"},
            {real, real, Bounds{"1.5", "Infinity"}}},
        {makeDoubleColumn("double_value", true), {"1.5", "NaN", "NaN", "1.5"}, {dbl, dbl, dbl}},
        {makeIntColumn("ordinary_integer", true, 2), {"2", "-10", "-10", "2"}, {ordinary, ordinary, ordinary}},
        {makeTextColumn("only_null", true), {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
            {onlyNull, onlyNull, onlyNull}},
        {makeDoubleColumn("only_nan", true), {"NaN", "NaN", "NaN", "NaN"}, {onlyNan, onlyNan, onlyNan}},
        {makeDecimalColumn("collapsed_decimal", true, 20, 4),
            {"9007199254740992.0001", "9007199254740992.0000", "9007199254740992.0000", "9007199254740992.0001"},
            {Bounds{"9007199254740992.0000", "9007199254740992.0001"},
             Bounds{"9007199254740992.0000", "9007199254740992.0001"},
             Bounds{"9007199254740992.0000", "9007199254740992.0001"}}},
        {integerArray, {"{2}", "{10}", "{10}", "{2}"},
            {Bounds{"{2}", "{10}"}, Bounds{"{2}", "{10}"}, Bounds{"{2}", "{10}"}}},
        {enumColumn, {"z", "a", "", "z"},
            {Bounds{"z", ""}, Bounds{"z", ""}, Bounds{"z", "a"}}},
    };
    TableSchema schema;
    schema.tablename = "inputs";
    schema.formatVersion = DATA_FILE_FORMAT_VERSION;
    schema.append(makeIntColumn("id", false, 2));
    schema.append(makeIntColumn("grp", false, 2));
    for (auto& item : cases) {
        if (item.column.dataType == "text") item.column.collation = "C";
        schema.append(item.column);
    }
    assert(cases[3].column.dsize == 8 && cases[5].column.dataType == "numeric");
    assert(g_engine.createTable(database, schema) == DBStatus::OK);
    schema = g_engine.getTableSchema(database, schema.tablename);
    assert(schema.len == 16 && schema.cols[schema.len - 1].enumValues == enumLabels);
    assert(schema.cols[schema.len - 2].isArray);
    for (size_t id = 0; id < 4; ++id) {
        std::map<std::string, Value> row{{"id", std::to_string(id)}, {"grp", std::to_string(id % 2)}};
        for (const auto& item : cases) row[item.column.dataName] = item.inputs[id];
        const auto inserted = g_engine.insertRow(database, schema.tablename, row);
        if (inserted != DBStatus::OK)
            std::cerr << "MINMAX_SETUP_INSERT_FAILED id=" << id
                      << " status=" << static_cast<int>(inserted) << std::endl;
        assert(inserted == DBStatus::OK);
    }
    TableScanOp scan(&g_engine, database, schema.tablename);
    assert(scan.open());
    std::vector<MinMaxReplayRows::Row> originals;
    std::string raw;
    while (scan.next(raw)) {
        assert(scan.scanOrigin().engine && scan.scanOrigin().rid);
        originals.push_back({raw, scan.scanOrigin()});
    }
    assert(!scan.hasError() && originals.size() == 4);
    scan.close();
    std::vector<StorageEngine::AggItem> aggregates;
    for (const auto& item : cases)
        for (const std::string function : {"min", "max"})
            aggregates.push_back({function, item.column.dataName, {}});
    size_t controls = 0, failures = 0;
    const auto check = [&](const std::string& label, bool valid,
                           const std::string& actual, const std::string& expected) {
        ++controls;
        if (!valid) {
            ++failures;
            std::cout << "MINMAX_DECLARED_FAILURE " << label << " actual=" << actual
                      << " expected=" << expected << '\n';
        }
    };
    for (size_t permutation = 0; permutation < 4; ++permutation) {
        std::vector<MinMaxReplayRows::Row> inputs;
        for (size_t row = 0; row < 512; ++row) inputs.push_back(originals[(row + permutation) % 4]);
        for (const bool parallel : {false, true}) {
            for (const bool grouped : {false, true}) {
                const std::vector<std::string> keys = grouped ? std::vector<std::string>{"grp"} : std::vector<std::string>{};
                const std::string label = "p=" + std::to_string(permutation) + " parallel=" + std::to_string(parallel) +
                                          " grouped=" + std::to_string(grouped);
                OpPtr child = std::make_unique<MinMaxReplayRows>(inputs);
                OpPtr aggregate;
                if (parallel) aggregate = std::make_unique<ParallelGroupAggregateOp>(
                    std::move(child), schema, keys, aggregates, std::vector<std::string>{}, 4);
                else aggregate = std::make_unique<GroupAggregateOp>(std::move(child), schema, keys,
                    std::vector<std::vector<std::string>>{}, aggregates, std::vector<std::string>{});
                assert(aggregate->open());
                if (parallel) check(label + " actual worker activation",
                    dynamic_cast<ParallelGroupAggregateOp*>(aggregate.get())->usedParallelWorkers(), "used", "used");
                size_t rows = 0;
                while (aggregate->next(raw)) {
                    std::vector<std::string> values;
                    std::vector<bool> nulls;
                    assert(aggregate->lastStructuredRow(values, nulls));
                    assert(values.size() == keys.size() + cases.size() * 2 && nulls.size() == values.size());
                    const size_t expectation = grouped ? size_t(std::stoul(values[0])) + 1 : 0;
                    assert(expectation < 3);
                    for (size_t item = 0; item < cases.size(); ++item) {
                        for (size_t bound = 0; bound < 2; ++bound) {
                            const auto& expected = bound ? cases[item].expected[expectation].second : cases[item].expected[expectation].first;
                            const size_t column = keys.size() + item * 2 + bound;
                            check(label + " group=" + std::to_string(expectation) + " " + cases[item].column.dataName +
                                  (bound ? " MAX" : " MIN"), nulls[column] == !expected &&
                                  (expected ? values[column] == *expected : values[column].empty()),
                                  (nulls[column] ? "SQL_NULL:" : "VALUE:") + values[column],
                                  expected ? "VALUE:" + *expected : "SQL_NULL:");
                        }
                    }
                    ++rows;
                }
                assert(!aggregate->hasError());
                aggregate->close();
                check(label + " output row count", rows == (grouped ? 2U : 1U), std::to_string(rows), grouped ? "2" : "1");
            }
        }
    }
    assert(controls == 696);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "MINMAX_DECLARED_NATIVE_COMPLETE " << controls << " FAILURES " << failures << std::endl;
    return failures == 0;
}

int main() {
    dbms::TypeRegistry::instance().bootstrap();

    const std::string db = testDbPath("parallel_exec");
    cleanupDb(db);
    assert(g_engine.createDatabase(db, "utf8") == DBStatus::OK);
    Session s;
    setupSession(s, db);
    dbms::DdlExecutor ddl;

    // 600 rows -> >256 to trigger worker-thread aggregation; enough pages
    // for the parallel build side and GatherMerge partitions.
    assert(!ddl.executeSql("CREATE TABLE big (id INT, grp INT, val INT)", s));
    for (int i = 0; i < 600; ++i) {
        assert(g_engine.insert(db, "big",
                               {{"id", std::to_string(i)},
                                {"grp", std::to_string(i % 3)},
                                {"val", std::to_string(i % 7)}})
                   == DBStatus::OK);
    }

    // ------------------------------------------------------------------
    // 1. ParallelGroupAggregateOp vs GroupAggregateOp: plain aggregate.
    // ------------------------------------------------------------------
    std::vector<dbms::StorageEngine::AggItem> items = {
        {"count", "*", {}}, {"sum", "val", {}}, {"avg", "val", {}},
        {"min", "val", {}}, {"max", "val", {}}};
    dbms::TableSchema tbl = g_engine.getTableSchema(db, "big");

    auto scan1 = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    dbms::GroupAggregateOp serial(std::move(scan1), tbl, {}, {}, items, {});
    auto serialRows = runPlan(&serial);

    auto scan2 = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    dbms::ParallelGroupAggregateOp par(std::move(scan2), tbl, {}, items, {}, 4);
    auto parRows = runPlan(&par);

    assert(serialRows.size() == 1 && parRows.size() == 1);
    assert(serialRows[0] == parRows[0]);
    assert(parRows[0].find("600") != std::string::npos);  // count(*) = 600
    std::cout << "[PAR] parallel plain aggregate OK (" << parRows[0] << ")"
              << std::endl;

    // ------------------------------------------------------------------
    // 2. GROUP BY parity: same groups, same aggregates.
    // ------------------------------------------------------------------
    std::vector<std::string> groupCols = {"grp"};
    auto scan3 = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    dbms::GroupAggregateOp serialG(std::move(scan3), tbl, groupCols, {}, items, {});
    auto serialGRows = sorted(runPlan(&serialG));

    auto scan4 = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    dbms::ParallelGroupAggregateOp parG(std::move(scan4), tbl, groupCols, items, {}, 4);
    auto parGRows = sorted(runPlan(&parG));

    assert(serialGRows.size() == 3 && parGRows.size() == 3);
    assert(serialGRows == parGRows);
    assert(par.usedParallelWorkers());
    std::cout << "[PAR] parallel GROUP BY parity OK" << std::endl;

    // ------------------------------------------------------------------
    // 3. ParallelHashJoinOp: big JOIN small on key; result parity with
    //    HashJoinOp and parallel build engaged.
    // ------------------------------------------------------------------
    assert(!ddl.executeSql("CREATE TABLE dim (k INT, label VARCHAR(10))", s));
    for (int k = 0; k < 7; ++k) {
        assert(g_engine.insert(db, "dim",
                               {{"k", std::to_string(k)},
                                {"label", "L" + std::to_string(k)}})
                   == DBStatus::OK);
    }
    // build side must span >2 pages: reuse big as build via a self-join
    // pattern big(a) JOIN big(b) ON a.id = b.id - expect 600 rows.
    auto ls = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    auto rs = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    dbms::HashJoinOp serialJ(&g_engine, db, std::move(ls), std::move(rs),
                             "big", "big", "id", "id");
    auto serialJRows = runPlan(&serialJ);
    assert(serialJRows.size() == 600);

    auto ls2 = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    auto rs2 = std::make_unique<dbms::TableScanOp>(&g_engine, db, "big");
    dbms::ParallelHashJoinOp parJ(&g_engine, db, std::move(ls2), std::move(rs2),
                                  "big", "big", "id", "id", 4);
    auto parJRows = runPlan(&parJ);
    // Result parity with the serial operator (same contract, same rows).
    assert(parJRows.size() == 600);
    assert(parJRows.size() == serialJRows.size());
    assert(sorted(parJRows) == sorted(serialJRows));
    assert(!parJRows.empty());
    assert(parJ.usedParallelWorkers());
    std::cout << "[PAR] parallel hash join parity OK (" << parJRows.size()
              << " rows)" << std::endl;

    // EXPLAIN shows the parallel node: join on a non-indexed column so the
    // hash path is selected (id is the PK and would pick merge join).
    dbms::QueryPlanner::setParallelWorkers(4);
    auto joinPlan = dbms::QueryPlanner::buildJoinPlan(
        &g_engine, db, "big", "big", "grp", "grp", {}, {});
    auto* joinRoot = joinPlan.get();
    assert(dynamic_cast<dbms::ParallelHashJoinOp*>(joinRoot) != nullptr);
    dbms::QueryPlanner::ExplainOptions opts;
    std::string jx = dbms::QueryPlanner::explain(joinPlan, &g_engine, db, opts);
    assert(jx.find("ParallelHashJoin") != std::string::npos);
    std::cout << "[PAR] planner chooses ParallelHashJoin OK" << std::endl;

    // ------------------------------------------------------------------
    // 3b. MergeJoinOp must emit the Cartesian product of equal-key groups.
    // ------------------------------------------------------------------
    assert(!ddl.executeSql("CREATE TABLE merge_l (id INT, k INT)", s));
    assert(!ddl.executeSql("CREATE TABLE merge_r (id INT, k INT)", s));
    for (const auto& values : std::vector<std::pair<int, int>>{
             {1, 7}, {2, 7}, {3, 8}}) {
        assert(g_engine.insert(db, "merge_l",
                               {{"id", std::to_string(values.first)},
                                {"k", std::to_string(values.second)}})
                   == DBStatus::OK);
    }
    for (const auto& values : std::vector<std::pair<int, int>>{
             {10, 7}, {20, 7}, {30, 7}, {40, 8}}) {
        assert(g_engine.insert(db, "merge_r",
                               {{"id", std::to_string(values.first)},
                                {"k", std::to_string(values.second)}})
                   == DBStatus::OK);
    }
    assert(g_engine.insert(db, "merge_l", {{"id", "4"}, {"k", "NULL"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(db, "merge_r", {{"id", "50"}, {"k", "NULL"}}) ==
           DBStatus::OK);
    auto hashLeft = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "merge_l");
    auto hashRight = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "merge_r");
    dbms::HashJoinOp duplicateHash(
        &g_engine, db, std::move(hashLeft), std::move(hashRight),
        "merge_l", "merge_r", "k", "k");
    auto hashDuplicateRows = sorted(runPlan(&duplicateHash));

    auto mergeLeft = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "merge_l");
    auto mergeRight = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "merge_r");
    dbms::MergeJoinOp duplicateMerge(
        &g_engine, db, std::move(mergeLeft), std::move(mergeRight),
        "merge_l", "merge_r", "k", "k");
    auto mergeDuplicateRows = sorted(runPlan(&duplicateMerge));
    assert(hashDuplicateRows.size() == 7);
    assert(mergeDuplicateRows == hashDuplicateRows);
    std::cout << "[PAR] merge join duplicate-key product OK" << std::endl;

    auto nestedLeft = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "merge_l");
    auto nestedRight = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "merge_r");
    dbms::NestedLoopJoinOp nullNested(
        &g_engine, db, std::move(nestedLeft), std::move(nestedRight),
        "merge_l", "merge_r", "k", "k");
    assert(sorted(runPlan(&nullNested)) == hashDuplicateRows);

    auto parallelLeft = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "merge_l");
    auto parallelRight = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "merge_r");
    dbms::ParallelHashJoinOp nullParallelFallback(
        &g_engine, db, std::move(parallelLeft), std::move(parallelRight),
        "merge_l", "merge_r", "k", "k", 4);
    assert(sorted(runPlan(&nullParallelFallback)) == hashDuplicateRows);

    assert(!ddl.executeSql(
        "CREATE TABLE nested_l (id INT, k VARCHAR(20))", s));
    assert(!ddl.executeSql(
        "CREATE TABLE nested_r (id INT, k VARCHAR(20))", s));
    assert(g_engine.insert(db, "nested_l", {{"id", "1"}, {"k", "alpha"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(db, "nested_l", {{"id", "2"}, {"k", "beta"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(db, "nested_r", {{"id", "10"}, {"k", "alpha"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(db, "nested_r", {{"id", "20"}, {"k", "beta"}}) ==
           DBStatus::OK);
    assert(g_engine.insert(db, "nested_r", {{"id", "30"}, {"k", "other"}}) ==
           DBStatus::OK);
    auto textHashLeft = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "nested_l");
    auto textHashRight = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "nested_r");
    dbms::HashJoinOp textHash(
        &g_engine, db, std::move(textHashLeft), std::move(textHashRight),
        "nested_l", "nested_r", "k", "k");
    auto textHashRows = sorted(runPlan(&textHash));
    assert(textHashRows.size() == 2);

    auto textNestedLeft = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "nested_l");
    auto textNestedRight = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "nested_r");
    dbms::NestedLoopJoinOp textNested(
        &g_engine, db, std::move(textNestedLeft), std::move(textNestedRight),
        "nested_l", "nested_r", "k", "k");
    assert(sorted(runPlan(&textNested)) == textHashRows);
    std::cout << "[PAR] nested-loop variable-length keys OK" << std::endl;

    // ------------------------------------------------------------------
    // 4. GatherMerge: per-worker sorted runs merge globally.
    // ------------------------------------------------------------------
    dbms::TableSchema dimTbl = g_engine.getTableSchema(db, "dim");
    // Raw rows laid out for dim: "k L<k>" text, split into 3 interleaved
    // pre-sorted runs (run w holds k = w, w+3, w+6, ...).
    std::vector<std::string> all;
    for (int k = 0; k < 7; ++k) {
        std::string row;
        for (size_t c = 0; c < dimTbl.len; ++c) {
            if (c > 0) row.push_back(' ');
            row += (c == 0) ? std::to_string(k) : ("L" + std::to_string(k));
        }
        all.push_back(row);
    }
    std::vector<dbms::OpPtr> runs;
    for (int w = 0; w < 3; ++w) {
        std::vector<std::string> rows;
        for (size_t i = static_cast<size_t>(w); i < all.size(); i += 3) {
            rows.push_back(all[i]);
        }
        runs.push_back(std::make_unique<dbms::MaterializedRowsOp>(std::move(rows)));
    }
    dbms::GatherMergeOp gm(std::move(runs), dimTbl, "k", true);
    auto gmRows = runPlan(&gm);
    assert(gmRows.size() == 7);
    for (size_t i = 1; i < gmRows.size(); ++i) {
        int prev = std::stoi(gmRows[i - 1].substr(0, gmRows[i - 1].find(' ')));
        int cur = std::stoi(gmRows[i].substr(0, gmRows[i].find(' ')));
        assert(prev < cur);
    }
    std::cout << "[PAR] GatherMerge k-way merge OK" << std::endl;

    // ------------------------------------------------------------------
    // 5. Planner: parallel aggregate + GatherMerge show in EXPLAIN.
    // ------------------------------------------------------------------
    dbms::QueryPlanner::setParallelWorkers(4);
    dbms::PlanContext ctx;
    ctx.dbname = db;
    ctx.tablename = "big";
    ctx.groupByCols = {"grp"};
    ctx.aggregateItems = items;
    auto aggPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, ctx);
    std::string ax = dbms::QueryPlanner::explain(aggPlan, &g_engine, db, opts);
    assert(ax.find("ParallelAggregate") != std::string::npos);
    std::cout << "[PAR] planner ParallelAggregate OK" << std::endl;

    dbms::PlanContext sctx;
    sctx.dbname = db;
    sctx.tablename = "big";
    sctx.orderByCol = "id";
    auto sortPlan = dbms::QueryPlanner::buildSelectPlan(&g_engine, sctx);
    std::string sx = dbms::QueryPlanner::explain(sortPlan, &g_engine, db, opts);
    if (sx.find("GatherMerge") != std::string::npos) {
        // Executing the whole plan (Project over GatherMerge) must return
        // every row exactly once.
        auto gmPlanned = runPlan(sortPlan.get());
        assert(gmPlanned.size() == 600);
        std::cout << "[PAR] planner GatherMerge OK" << std::endl;
    } else {
        // fell back to SortOp (e.g. fewer than 3 pages) — acceptable.
        assert(sx.find("Sort") != std::string::npos);
        std::cout << "[PAR] GatherMerge fallback to Sort (acceptable)"
                  << std::endl;
    }

    // Exercise the direct page-range build path with one NULL on each side.
    // The non-NULL key domains do not overlap, so NULL = NULL was the only
    // way this join could incorrectly emit a row.
    assert(g_engine.insert(db, "big",
                           {{"id", "600"}, {"grp", "NULL"}, {"val", "0"}}) ==
           DBStatus::OK);
    auto nullProbe = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "merge_l");
    auto parallelBuild = std::make_unique<dbms::TableScanOp>(
        &g_engine, db, "big");
    dbms::ParallelHashJoinOp nullParallelBuild(
        &g_engine, db, std::move(nullProbe), std::move(parallelBuild),
        "merge_l", "big", "k", "grp", 4);
    assert(runPlan(&nullParallelBuild).empty());
    assert(nullParallelBuild.usedParallelWorkers());
    std::cout << "[PAR] NULL join keys remain unmatched" << std::endl;
    dbms::QueryPlanner::setParallelWorkers(0);

    cleanupDb(db);
    assert(testDeclaredMinMaxComparison());
    std::cout << "[PAR] all tests passed" << std::endl;
    return 0;
}
