#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    const std::string name = "pattern_predicate_binding", database = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    TableSchema schema{}; schema.tablename = "pattern_rows"; schema.len = 2;
    schema.cols[0].dataName = "id"; schema.cols[0].dataType = "integer";
    schema.cols[1].dataName = "name"; schema.cols[1].dataType = "text";
    assert(g_engine.createTable(database, schema) == DBStatus::OK);
    TableSchema arrays{}; arrays.tablename="pattern_arrays"; arrays.len=3;
    const std::vector<std::string> arrayTypes={"text","character","bytea"};
    for(size_t i=0;i<arrayTypes.size();++i) {
        arrays.cols[i].dataName=std::string(1,static_cast<char>('a'+i));
        arrays.cols[i].dataType=arrayTypes[i]; arrays.cols[i].isArray=true;
    }
    assert(g_engine.createTable(database, arrays)==DBStatus::OK);
    for(const auto& sql:std::vector<std::string>{
        "SELECT a LIKE '%' FROM pattern_arrays WHERE false",
        "SELECT b NOT ILIKE '%' FROM pattern_arrays LIMIT 0",
        "SELECT c LIKE '%' ESCAPE '#' FROM pattern_arrays WHERE false",
        "SELECT NULL::TEXT[] NOT LIKE '%' LIMIT 0",
        "SELECT ARRAY['a'] SIMILAR TO '%' WHERE false"}) {
        std::string state;
        try{(void)g_engine.prepareBoundQuery(database,sql);}
        catch(const DbError& error){state=error.sqlState();}
        std::cout<<"PATTERN_ARRAY_REJECT "<<sql<<" state="<<state<<std::endl;
        assert(state=="42883");
    }
    assert(g_engine.createSequence(database, "pattern_calls") == DBStatus::OK);
    const std::vector<std::string> predicates = {
        "name LIKE 'a%'", "name NOT LIKE 'a%'",
        "name ILIKE 'A%'", "name NOT ILIKE 'A%'",
        "name SIMILAR TO '(ann|cat)'", "name NOT SIMILAR TO '(ann|cat)'",
        "name LIKE 'a#_%' ESCAPE '#'", "name NOT LIKE 'a#_%' ESCAPE '#'",
        "name NOT ILIKE NULL", "name NOT SIMILAR TO NULL"
    };
    size_t failures = 0;
    for (const auto& predicate : predicates) {
        const std::vector<std::string> queries = {
            "SELECT id FROM pattern_rows WHERE " + predicate,
            "UPDATE pattern_rows SET id=id WHERE " + predicate,
            "DELETE FROM pattern_rows WHERE " + predicate,
            "SELECT CASE WHEN " + predicate + " THEN 1 ELSE 0 END FROM pattern_rows"
        };
        for (const auto& sql : queries) {
            std::string state;
            try { (void)g_engine.prepareBoundQuery(database, sql); }
            catch (const DbError& error) { state = error.sqlState(); }
            std::cout << "PATTERN_BIND " << sql << " state=" << state << '\n';
            if (!state.empty()) ++failures;
        }
        auto prepared = std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,
            "SELECT " + predicate + " AS predicate FROM pattern_rows"));
        std::cout << "PATTERN_DESCRIPTOR " << predicate << " width=" << prepared->output.size()
            << " type=" << (prepared->output.empty() ? "<none>" : prepared->output[0].type) << std::endl;
        assert(prepared->output.size() == 1 && prepared->output[0].type == "boolean");
        auto* select = dynamic_cast<SelectStmt*>(prepared->ast.get());
        assert(select && select->selectList.size() == 1 && prepared->sourceRanges.size() == 1);
        PreparedQueryExecution execution(prepared, &g_engine, database);
        auto* expression = select->selectList[0].expr.get();
        execution.prepareExpression(expression);
        auto row = execution.context();
        execution.setSourceRow(row, prepared->sourceRanges[0].ordinal,
            {ExprValue("integer", "1", false), ExprValue("text", "", true)});
        const auto result = execution.evaluate(expression, row);
        assert(result.typeName == "boolean" && result.isNull);
    }
    bool rejected = false;
    try { (void)g_engine.prepareBoundQuery(database, "UPDATE pattern_rows SET id=nextval('pattern_calls') WHERE 1"); }
    catch (const DbError& error) { rejected = error.sqlState() == "42804"; }
    assert(rejected && g_engine.nextval(database, "pattern_calls") == 1);
    assert(!g_engine.inTransaction());
    assert(g_engine.dropDatabase(database) == DBStatus::OK);
    cleanupTestDb(name);
    std::cout << "PATTERN_BIND failures=" << failures << std::endl;
    assert(failures == 0);
}
