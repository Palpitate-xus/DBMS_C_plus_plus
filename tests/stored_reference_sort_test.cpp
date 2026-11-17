// Run in a fresh private CWD: test_stubs constructs its engine before main.
#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/StoredRegtypeValue.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
size_t checked = 0;
void need(bool good, const std::string& label) {
    ++checked;
    if (!good) throw std::runtime_error(label);
}

struct Input {
    TableSchema schema;
    QueryRowDescriptor descriptor;
    PreparedQueryRows rows;
    DatumTypePtr arrayDeclaration;
};

Input actualInput(const std::string& database, const std::string& type,
                  Session& session) {
    const auto table = "sort_" + type;
    need(!DdlExecutor{}.executeSql("CREATE TABLE " + table +
        "(id integer PRIMARY KEY,k " + type + "[],payload text)",
        session), "create actual reference-array source");
    const std::vector<std::optional<std::string>> raw{
        "{25}", "{23}", "[0:0]={23}", "{23,NULL}", "{23,0}", "{0}",
        "{4294967295}", "{}", std::nullopt, "{23}", "{{23,23}}",
        "{{23},{23}}", "[0:1]={23,23}", "{{0}}", "{NULL}"};
    for (size_t i = 0; i < raw.size(); ++i)
        need(g_engine.insertRow(database, table,
            {{"id", std::to_string(i + 1)}, {"k", raw[i]},
             {"payload", std::string(8192, 'a' + static_cast<int>(i % 20))}}) == DBStatus::OK,
            "insert actual array input, including zero/NULL/bounds");
    Input result;
    result.schema = g_engine.getTableSchema(database, table);
    const auto metadata = g_engine.prepareDatumMetadataContext(database, table);
    need(metadata && metadata->columns.count("k"), "actual source declaration metadata");
    result.arrayDeclaration = metadata->columns.at("k");
    need(result.arrayDeclaration && result.arrayDeclaration->kind == DatumKind::Array &&
         result.arrayDeclaration->arrayElement &&
         result.arrayDeclaration->arrayElement->codecOid == (type == "regtype" ? 2206U : 2205U),
         "array element codec comes from the real declared column");
    for (size_t i = 0; i < result.schema.len; ++i) {
        const auto& column = result.schema.cols[i];
        const auto declaration = metadata->columns.at(column.dataName);
        result.descriptor.push_back({column.dataName,
            ExprHelper::canonicalResultTypeName(column.dataType + (column.isArray ? "[]" : "")),
            false, 0, declaration->actualOid, declaration->effectiveTypeMod, declaration});
    }
    StorageEngine::OrderBySpec order;
    order.colName = "id";
    std::vector<std::vector<std::string>> cells;
    std::vector<std::vector<bool>> nulls;
    const auto display = g_engine.query(database, table, {}, {}, {order},
        false, false, false, 0, {}, &cells, &nulls);
    need(display.size() == raw.size() && cells.size() == raw.size() && nulls.size() == raw.size(),
         "physical producer returned every actual row");
    const auto catalog = regtype_detail::metadata(g_engine, database);
    for (size_t row = 0; row < cells.size(); ++row) {
        need(cells[row].size() == result.schema.len && nulls[row].size() == result.schema.len,
             "physical source has exact cell/NULL width");
        result.rows.push_back({ExprValue("integer", cells[row][0], nulls[row][0]),
            stored_regtype_detail::load(cells[row][1], true, nulls[row][1],
                catalog, &session, result.arrayDeclaration, type),
            ExprValue("text", cells[row][2], nulls[row][2])});
    }
    return result;
}

PlanExecutionResult execute(const std::string& database, const Input& input,
    const std::string& sql, std::shared_ptr<const PreparedQueryRows> values = {}) {
    QueryBindingMetadata metadata;
    metadata.relation = [columns = input.descriptor](const std::string& name) {
        return QueryRelationMetadata{"public", name, columns, {}};
    };
    auto query = std::make_shared<PreparedQuery>(prepareQuery(sql, {}, metadata));
    if (!values) values = std::make_shared<const PreparedQueryRows>(input.rows);
    auto source = std::make_unique<PreparedSourceRowsOp>(input.descriptor,
        [values](size_t index, std::vector<ExprValue>& row) {
            if (index >= values->size()) return false;
            row = values->at(index);
            return true;
        });
    auto* select = static_cast<SelectStmt*>(query->ast.get());
    auto plan = QueryPlanner::buildPreparedSelectPlan(&g_engine, database, query,
        select, input.schema, std::move(source));
    auto result = QueryPlanner::executePlanChecked(std::move(plan), 0, true);
    result.throwIfFailed();
    return result;
}

std::vector<size_t> ids(const PlanExecutionResult& result) {
    std::vector<size_t> output;
    for (size_t row = 0; row < result.structuredRows.size(); ++row) {
        need(result.structuredRows[row].size() == 1 &&
             result.structuredNulls[row] == std::vector<bool>{false},
             "sort preserves exact projected id/NULL cells");
        output.push_back(std::stoul(result.structuredRows[row][0]));
    }
    return output;
}

void check(const std::string& database, const std::string& type, Session& session) {
    const auto input = actualInput(database, type, session);
    const std::vector<size_t> ascending{8,6,14,3,2,10,5,13,11,12,4,1,7,15,9};
    for (const size_t workMem : {size_t(65536), size_t(64)}) {
        session.workMem.assign(workMem, false);
        for (const bool descending : {false, true})
            for (const bool first : {false, true}) {
                auto expected = ascending;
                expected.pop_back();
                if (descending) std::reverse(expected.begin(), expected.end());
                if (first) expected.insert(expected.begin(), 9);
                else expected.push_back(9);
                const auto sql = "SELECT id FROM input ORDER BY k " +
                    std::string(descending ? "DESC" : "ASC") + " NULLS " +
                    (first ? "FIRST" : "LAST") + ",id " + (descending ? "DESC" : "ASC");
                need(ids(execute(database, input, sql)) == expected,
                     type + " array ordering: unsigned identities, NULLs, shape and bounds");
            }
        const auto distinct = execute(database, input,
            "SELECT DISTINCT k FROM input ORDER BY k ASC NULLS LAST");
        auto uniqueIds = ascending;
        uniqueIds.erase(std::find(uniqueIds.begin(), uniqueIds.end(), 10));
        need(distinct.structuredRows.size() == uniqueIds.size() && distinct.typedRowsAvailable,
             type + " DISTINCT keeps one genuinely equal owning array");
        for (size_t row = 0; row < uniqueIds.size(); ++row) {
            const auto& expected = input.rows[uniqueIds[row] - 1][1];
            need(distinct.structuredRows[row] == std::vector<std::string>{expected.value} &&
                 distinct.structuredNulls[row] == std::vector<bool>{expected.isNull},
                 "DISTINCT retains empty/whole NULL/shape/bounds separately");
            const auto& owned = distinct.typedRows[row][0];
            need(owned.isNull == expected.isNull && owned.elementOids == expected.elementOids &&
                 (owned.isNull || (owned.compound && owned.compound->type &&
                  owned.compound->type->actualOid == input.arrayDeclaration->actualOid)),
                 "sorted output retains the actual array declaration and element identities");
        }
        need(ids(execute(database, input,
            "SELECT id FROM input ORDER BY k ASC NULLS LAST FETCH FIRST 5 ROWS WITH TIES")) ==
             std::vector<size_t>{8,6,14,3,2,10}, "WITH TIES compares genuine owning element identities");
        need(ids(execute(database, input,
            "SELECT id FROM input ORDER BY k ASC NULLS LAST FETCH FIRST 4 ROWS WITH TIES")) ==
             std::vector<size_t>{8,6,14,3}, "different lower bounds are not sort peers");
    }
    session.workMem.assign(65536, false);
    auto unrelatedDisplay = std::make_shared<PreparedQueryRows>(input.rows);
    for (auto& row : *unrelatedDisplay) row[1].value = "this is not an array literal";
    need(ids(execute(database, input,
        "SELECT id FROM input ORDER BY k ASC NULLS LAST,id ASC", unrelatedDisplay)) == ascending,
         "owning array sorting never parses or resolves display strings");
    const auto reject = [&](PreparedQueryRows rows, const std::string& state,
                            const std::string& label) {
        try {
            (void)execute(database, input,
                "SELECT id FROM input ORDER BY k ASC NULLS LAST", std::make_shared<PreparedQueryRows>(std::move(rows)));
            need(false, label);
        } catch (const DbError& error) { need(error.sqlState() == state, label); }
    };
    auto missing = input.rows;
    auto data = std::make_shared<CompoundDatum>(*missing[0][1].compound);
    data->type.reset();
    missing[0][1].compound = std::move(data);
    reject(std::move(missing), "0A000", "missing actual declaration has no display-based fallback");
    auto absentOid = input.rows;
    data = std::make_shared<CompoundDatum>(*absentOid[0][1].compound);
    data->arrayElements->front().objectOid.reset();
    absentOid[0][1].compound = std::move(data);
    reject(std::move(absentOid), "XX000", "missing genuine element OID cannot be recovered from a displayed name");
}
} // namespace

int main() {
    try {
        TypeRegistry::instance().bootstrap();
        const auto database = testDbPath("stored_reference_sort_" + std::to_string(::getpid()));
        need(!std::filesystem::exists(database), "fresh private database");
        need(g_engine.createDatabase(database, "utf8") == DBStatus::OK, "create actual database");
        Session session;
        session.username = "admin"; session.permission = 1; session.currentDB = database;
        struct Restore { Session* old = currentSession(); ~Restore() { setCurrentSession(old); } } restore;
        setCurrentSession(&session);
        check(database, "regtype", session);
        check(database, "regclass", session);
        need(g_engine.dropDatabase(database) == DBStatus::OK, "remove actual test database");
        std::cout << "STORED_REFERENCE_SORT_PASS checks=" << checked << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "STORED_REFERENCE_SORT_FAIL checks=" << checked << " error=" << error.what() << '\n';
        return 1;
    }
}
