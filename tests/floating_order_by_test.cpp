#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <optional>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::vector<std::string> ids(std::initializer_list<const char*> values) {
    std::vector<std::string> result;
    for (const char* value : values) result.emplace_back(std::string(value) + " ");
    return result;
}

dbms::StorageEngine::OrderBySpec ascending(const std::string& column,
                                            bool nullsFirst = false) {
    dbms::StorageEngine::OrderBySpec order;
    order.colName = column;
    order.ascending = true;
    order.nullsFirst = nullsFirst;
    return order;
}

void assertQueryOrder(const std::string& database, const std::string& column,
                      const std::vector<std::string>& expected,
                      bool nullsFirst = false) {
    const std::vector<dbms::StorageEngine::OrderBySpec> order = {
        ascending(column, nullsFirst)};
    assert(g_engine.query(database, "values_to_sort", {}, {"id"}, order) ==
           expected);

    dbms::StorageEngine::SelectExpr idExpression;
    idExpression.displayName = "id";
    idExpression.colName = "id";
    assert(g_engine.queryExpr(database, "values_to_sort", {},
                              {idExpression}, order) == expected);
}

// Reorder real scan buffers without discarding their physical NULL metadata.
// MaterializedRowsOp intentionally has no origin and is not a nullable heap
// stream, so it would not be a valid source for this NULL-ordering check.
class ReplayRowsOp final : public dbms::Operator {
public:
    struct Row { std::string raw; ScanOrigin origin; };
    explicit ReplayRowsOp(std::vector<Row> rows) : rows_(std::move(rows)) {}
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

bool checkExecutorFloatingSpecialOrder() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("executor_floating_special_order");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    TableSchema schema;
    schema.tablename = "floats";
    schema.formatVersion = DATA_FILE_FORMAT_VERSION;
    schema.append(makeIntColumn("id", false, 4));
    schema.append(makeFloatColumn("single_value", true));
    schema.append(makeDoubleColumn("double_value", true));
    assert(g_engine.createTable(database, schema) == DBStatus::OK);
    // Explicit expected order, not an oracle using the production comparator.
    const std::vector<std::optional<std::string>> values = {
        "-Infinity", "-2.25", "0", "1.75", "3.5", "Infinity", "NaN", std::nullopt};
    for (size_t id = 0; id < values.size(); ++id)
        assert(g_engine.insertRow(database, schema.tablename,
            {{"id", std::to_string(id)}, {"single_value", values[id]},
             {"double_value", values[id]}}) == DBStatus::OK);
    TableScanOp scan(&g_engine, database, schema.tablename);
    assert(scan.open());
    std::vector<ReplayRowsOp::Row> raw;
    std::string row;
    while (scan.next(row)) {
        assert(scan.scanOrigin().engine && scan.scanOrigin().rid != 0);
        raw.push_back({row, scan.scanOrigin()});
    }
    assert(!scan.hasError() && raw.size() == values.size());
    scan.close();

    size_t checked = 0, failures = 0;
    const auto check = [&](const std::string& label,
                           const std::vector<std::string>& actual,
                           const std::vector<std::string>& expected) {
        ++checked;
        if (actual != expected) {
            ++failures;
            std::cout << "SORT_FLOATING_SPECIAL_FAILURE " << label << " actual=";
            for (const auto& value : actual) std::cout << value << ',';
            std::cout << " expected=";
            for (const auto& value : expected) std::cout << value << ',';
            std::cout << '\n';
        }
    };
    for (const std::string column : {"single_value", "double_value"}) {
        for (size_t permutation = 0; permutation < 8; ++permutation) {
            auto input = raw;
            std::rotate(input.begin(), input.begin() + permutation, input.end());
            // The last four input shapes also reverse all finite/NaN/NULL
            // positions. No order requirement is imposed on equal values.
            if (permutation >= 4) std::reverse(input.begin(), input.end());
            for (const bool ascending : {true, false}) {
                for (const int nullMode : {-1, 0, 1}) {
                    const bool nullsFirst = nullMode < 0 ? !ascending : nullMode != 0;
                    std::vector<std::string> expected;
                    if (nullsFirst) expected.push_back("7");
                    for (size_t i = 0; i < 7; ++i)
                        expected.push_back(std::to_string(ascending ? i : 6 - i));
                    if (!nullsFirst) expected.push_back("7");
                    SortOp sort(std::make_unique<ReplayRowsOp>(input), schema,
                                column, ascending, nullsFirst, nullMode >= 0);
                    assert(sort.open());
                    std::vector<std::string> actual;
                    while (sort.next(row)) actual.push_back(
                        StorageEngine::extractColumnValueStatic(row, schema, 0));
                    assert(!sort.hasError());
                    sort.close();
                    check(column + " permutation=" + std::to_string(permutation) +
                          " asc=" + std::to_string(ascending) +
                          " nullMode=" + std::to_string(nullMode), actual, expected);
                }
            }
        }
        for (const bool ascending : {true, false}) {
            for (const bool nullsFirst : {true, false}) {
                PlanContext context;
                context.dbname = database;
                context.tablename = schema.tablename;
                context.selectCols = {"id"};
                context.orderByCol = column;
                context.orderByAsc = ascending;
                context.orderByNullsFirst = nullsFirst;
                context.hasExplicitOrderNulls = true;
                const auto result = QueryPlanner::executePlanChecked(
                    QueryPlanner::buildSelectPlan(&g_engine, context));
                result.throwIfFailed();
                std::vector<std::string> expected;
                if (nullsFirst) expected.push_back("7 ");
                for (size_t i = 0; i < 7; ++i)
                    expected.push_back(std::to_string(ascending ? i : 6 - i) + " ");
                if (!nullsFirst) expected.push_back("7 ");
                check(column + " planner asc=" + std::to_string(ascending) +
                      " nullsFirst=" + std::to_string(nullsFirst), result.rows, expected);
            }
        }
    }
    assert(checked == 104);
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    std::cout << "SORT_FLOATING_SPECIAL_NATIVE_COMPLETE " << checked
              << " FAILURES " << failures << std::endl;
    return failures == 0;
}

}  // namespace

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string testName = "floating_order_by";
    const std::string database = testDbPath(testName);
    cleanupTestDb(testName);
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);

    dbms::TableSchema schema;
    schema.tablename = "values_to_sort";
    schema.formatVersion = 2;
    schema.append(dbms::makeIntColumn("id", false, 4, true));
    schema.append(dbms::makeFloatColumn("single_value", true));
    schema.append(dbms::makeDoubleColumn("double_value", true));
    schema.append(dbms::makeDecimalColumn("decimal_value", true, 20, 4));
    assert(g_engine.createTable(database, schema) == dbms::DBStatus::OK);

    const std::vector<std::vector<std::string>> rows = {
        {"1", "3.5", "-2.25", "10.75"},
        {"2", "-2.25", "3.5", "-10.25"},
        {"3", "1.75", "1.75", "2.5"},
        {"4", "Infinity", "-Infinity", "2.05"},
        {"5", "-Infinity", "Infinity", "2.005"},
        {"6", "NaN", "NaN", "0.125"},
        {"7", "NULL", "NULL", "NULL"},
    };
    for (const auto& row : rows) {
        assert(g_engine.insert(
                   database, "values_to_sort",
                   {{"id", row[0]}, {"single_value", row[1]},
                    {"double_value", row[2]}, {"decimal_value", row[3]}}) ==
               dbms::DBStatus::OK);
    }

    // PostgreSQL floating ordering is numeric, with NaN after +Infinity.
    assertQueryOrder(database, "single_value",
                     ids({"5", "2", "3", "1", "4", "6", "7"}));
    assertQueryOrder(database, "double_value",
                     ids({"4", "1", "3", "2", "5", "6", "7"}));
    assertQueryOrder(database, "decimal_value",
                     ids({"2", "6", "5", "4", "3", "1", "7"}));
    assertQueryOrder(database, "single_value",
                     ids({"7", "5", "2", "3", "1", "4", "6"}), true);

    dbms::StorageEngine::OrderBySpec descending = ascending("single_value");
    descending.ascending = false;
    assert(g_engine.query(database, "values_to_sort", {}, {"id"},
                          {descending}) ==
           ids({"6", "4", "1", "3", "2", "5", "7"}));

    dbms::TableSchema numericSchema;
    numericSchema.tablename = "numeric_function_order";
    numericSchema.formatVersion = 2;
    numericSchema.append(dbms::makeDecimalColumn("v", false, 20, 4));
    assert(g_engine.createTable(database, numericSchema) ==
           dbms::DBStatus::OK);
    for (const auto& value : {"-1.4", "-1.6"})
        assert(g_engine.insert(database, "numeric_function_order",
                               {{"v", value}}) == dbms::DBStatus::OK);
    dbms::StorageEngine::OrderBySpec functionSort;
    functionSort.isExpression = true;
    functionSort.exprFunc = "round";
    functionSort.expressionSql = "round(v)";
    assert(g_engine.query(database, "numeric_function_order", {}, {"v"},
                          {functionSort}) == ids({"-1.6", "-1.4"}));
    assert(g_engine.sortByExpression(
               database, "numeric_function_order",
               ids({"-1.4", "-1.6"}), {functionSort}) ==
           ids({"-1.6", "-1.4"}));
    functionSort.exprFunc = "abs";
    functionSort.expressionSql = "abs(v)";
    functionSort.ascending = false;
    assert(g_engine.query(database, "numeric_function_order", {}, {"v"},
                          {functionSort}) == ids({"-1.6", "-1.4"}));

    dbms::TableSchema powerSchema;
    powerSchema.tablename = "power_function_order";
    powerSchema.formatVersion = 2;
    powerSchema.append(dbms::makeIntColumn("v", false, 4));
    assert(g_engine.createTable(database, powerSchema) ==
           dbms::DBStatus::OK);
    for (const auto& value : {"-2", "-1"})
        assert(g_engine.insert(database, "power_function_order",
                               {{"v", value}}) == dbms::DBStatus::OK);
    functionSort.exprFunc = "power";
    functionSort.expressionSql = "power(v, 2)";
    functionSort.ascending = true;
    assert(g_engine.query(database, "power_function_order", {}, {"v"},
                          {functionSort}) == ids({"-1", "-2"}));

    dbms::TableSchema roundingSchema;
    roundingSchema.tablename = "rounding_function_order";
    roundingSchema.formatVersion = 2;
    roundingSchema.append(dbms::makeDecimalColumn("v", false, 20, 4));
    assert(g_engine.createTable(database, roundingSchema) ==
           dbms::DBStatus::OK);
    for (const auto& value : {"2.2", "10.2"})
        assert(g_engine.insert(database, "rounding_function_order",
                               {{"v", value}}) == dbms::DBStatus::OK);
    for (const auto& name : {"floor", "ceil", "trunc", "sign"}) {
        functionSort.exprFunc = name;
        functionSort.expressionSql = std::string(name) + "(v)";
        assert(g_engine.query(database, "rounding_function_order", {},
                              {"v"}, {functionSort}) ==
               ids({"2.2", "10.2"}));
    }
    functionSort.exprFunc = "mod";
    functionSort.expressionSql = "mod(v, 3)";
    assert(g_engine.query(database, "rounding_function_order", {}, {"v"},
                          {functionSort}) == ids({"10.2", "2.2"}));
    assert(g_engine.sortByExpression(
               database, "rounding_function_order",
               ids({"2.2", "10.2"}), {functionSort}) ==
           ids({"10.2", "2.2"}));

    dbms::TableSchema integerSchema;
    integerSchema.tablename = "integer_function_order";
    integerSchema.formatVersion = 2;
    integerSchema.append(dbms::makeIntColumn("v", false, 4));
    assert(g_engine.createTable(database, integerSchema) ==
           dbms::DBStatus::OK);
    for (const auto& value : {"9", "4"})
        assert(g_engine.insert(database, "integer_function_order",
                               {{"v", value}}) == dbms::DBStatus::OK);
    for (const auto& name : {"gcd", "lcm", "div"}) {
        functionSort.exprFunc = name;
        functionSort.expressionSql = std::string(name) + "(v, 6)";
        assert(g_engine.query(database, "integer_function_order", {},
                              {"v"}, {functionSort}) == ids({"4", "9"}));
        assert(g_engine.sortByExpression(
                   database, "integer_function_order", ids({"9", "4"}),
                   {functionSort}) == ids({"4", "9"}));
    }
    for (const auto& name : {"sqrt", "ln", "log", "exp"}) {
        functionSort.exprFunc = name;
        functionSort.expressionSql = std::string(name) + "(v)";
        assert(g_engine.query(database, "rounding_function_order", {},
                              {"v"}, {functionSort}) ==
               ids({"2.2", "10.2"}));
    }

    dbms::TableSchema trigSchema;
    trigSchema.tablename = "trig_function_order";
    trigSchema.formatVersion = 2;
    trigSchema.append(dbms::makeDecimalColumn("v", false, 20, 4));
    assert(g_engine.createTable(database, trigSchema) ==
           dbms::DBStatus::OK);
    for (const auto& value : {"1.5", "4.5"})
        assert(g_engine.insert(database, "trig_function_order",
                               {{"v", value}}) == dbms::DBStatus::OK);
    for (const auto& name : {"sin", "cos", "tan"}) {
        functionSort.exprFunc = name;
        functionSort.expressionSql = std::string(name) + "(v)";
        assert(g_engine.query(database, "trig_function_order", {},
                              {"v"}, {functionSort}) ==
               ids({"4.5", "1.5"}));
    }
    functionSort.exprFunc = "sin";
    functionSort.expressionSql = "sin(v)";
    assert(g_engine.sortByExpression(
               database, "trig_function_order", ids({"1.5", "4.5"}),
               {functionSort}) == ids({"4.5", "1.5"}));

    dbms::TableSchema bucketSchema;
    bucketSchema.tablename = "bucket_function_order";
    bucketSchema.formatVersion = 2;
    bucketSchema.append(dbms::makeIntColumn("v", false, 4));
    assert(g_engine.createTable(database, bucketSchema) == dbms::DBStatus::OK);
    for (const auto& value : {"6", "1"})
        assert(g_engine.insert(database, "bucket_function_order",
                               {{"v", value}}) == dbms::DBStatus::OK);
    functionSort.exprFunc = "width_bucket";
    functionSort.expressionSql = "width_bucket(v, 0, 10, 2)";
    assert(g_engine.query(database, "bucket_function_order", {}, {"v"},
                          {functionSort}) == ids({"1", "6"}));
    assert(g_engine.sortByExpression(database, "bucket_function_order",
                                     ids({"6", "1"}), {functionSort}) ==
           ids({"1", "6"}));

    dbms::TableSchema formatSchema;
    formatSchema.tablename = "format_function_order";
    formatSchema.formatVersion = 2;
    formatSchema.append(dbms::makeIntColumn("v", false, 4));
    assert(g_engine.createTable(database, formatSchema) == dbms::DBStatus::OK);
    for (const auto& value : {"2", "10"})
        assert(g_engine.insert(database, "format_function_order",
                               {{"v", value}}) == dbms::DBStatus::OK);
    functionSort.exprFunc = "to_char";
    functionSort.expressionSql = "to_char(v, 'FM99')";
    assert(g_engine.query(database, "format_function_order", {}, {"v"},
                          {functionSort}) == ids({"10", "2"}));
    assert(g_engine.sortByExpression(database, "format_function_order",
                                     ids({"2", "10"}), {functionSort}) ==
           ids({"10", "2"}));

    dbms::TableSchema parseSchema;
    parseSchema.tablename = "parse_function_order";
    parseSchema.formatVersion = 2;
    parseSchema.append(dbms::makeTextColumn("v", false));
    assert(g_engine.createTable(database, parseSchema) == dbms::DBStatus::OK);
    for (const auto& value : {"10", "2"})
        assert(g_engine.insert(database, "parse_function_order",
                               {{"v", value}}) == dbms::DBStatus::OK);
    functionSort.exprFunc = "to_number";
    functionSort.expressionSql = "to_number(v, '999')";
    assert(g_engine.query(database, "parse_function_order", {}, {"v"},
                          {functionSort}) == ids({"2", "10"}));
    assert(g_engine.sortByExpression(database, "parse_function_order",
                                     ids({"10", "2"}), {functionSort}) ==
           ids({"2", "10"}));

    for (const auto& expression : {
             "least(v, 6)", "greatest(v, 6)",
             "coalesce(v, 0)", "nullif(v, 9)"}) {
        functionSort.expressionSql = expression;
        functionSort.exprFunc = std::string(expression).substr(
            0, std::string(expression).find('('));
        assert(g_engine.query(database, "integer_function_order", {}, {"v"},
                              {functionSort}) == ids({"4", "9"}));
        assert(g_engine.sortByExpression(
                   database, "integer_function_order", ids({"9", "4"}),
                   {functionSort}) == ids({"4", "9"}));
    }
    for (const auto& expression : {
             "coalesce(NULL, v)", "least(NULL, v)",
             "greatest(NULL, v)"}) {
        functionSort.expressionSql = expression;
        functionSort.exprFunc = std::string(expression).substr(
            0, std::string(expression).find('('));
        assert(g_engine.query(database, "rounding_function_order", {}, {"v"},
                              {functionSort}) == ids({"2.2", "10.2"}));
        assert(g_engine.sortByExpression(
                   database, "rounding_function_order",
                   ids({"2.2", "10.2"}), {functionSort}) ==
               ids({"2.2", "10.2"}));
    }
    dbms::TableSchema polymorphicTextSchema;
    polymorphicTextSchema.tablename = "polymorphic_text_order";
    polymorphicTextSchema.formatVersion = 2;
    polymorphicTextSchema.append(dbms::makeTextColumn("v", false));
    assert(g_engine.createTable(database, polymorphicTextSchema) ==
           dbms::DBStatus::OK);
    for (const auto& value : {"b", "a"})
        assert(g_engine.insert(database, "polymorphic_text_order",
                               {{"v", value}}) == dbms::DBStatus::OK);
    functionSort.exprFunc = "least";
    functionSort.expressionSql = "least(v, 'z')";
    assert(g_engine.query(database, "polymorphic_text_order", {}, {"v"},
                          {functionSort}) == ids({"a", "b"}));

    assert(checkExecutorFloatingSpecialOrder());

    cleanupTestDb(testName);
    finalCleanupTestData();
    std::cout << "[FLOATING ORDER BY] numeric/NaN/null ordering OK"
              << std::endl;
    return 0;
}
