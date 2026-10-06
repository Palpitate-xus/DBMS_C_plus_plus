#include "executor/ExecutionPlan.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const std::string db = testDbPath("ordinary_scalar_where_plan");
    assert(owner.createDatabase(db,"utf8") == DBStatus::OK);
    TableSchema schema; schema.len=3;
    schema.cols[0].dataName="id"; schema.cols[0].dataType="int"; schema.cols[0].dsize=4;
    schema.cols[1]=schema.cols[0]; schema.cols[1].dataName="ID"; schema.cols[1].dataType="bigint"; schema.cols[1].dsize=8;
    schema.cols[2].dataName="payload"; schema.cols[2].dataType="text"; schema.cols[2].dsize=64; schema.cols[2].isNull=true;
    assert(owner.createTable(db,"source",schema) == DBStatus::OK);
    for (const auto& [id,payload] : std::vector<std::pair<int,std::optional<std::string>>>{{1,""},{2,"null"},{3,std::nullopt}})
        assert(owner.insertRow(db,"source",std::map<std::string,std::optional<std::string>>{
            {"id",std::to_string(id)},{"ID",std::to_string(2147483647LL+id)},{"payload",payload}}) == DBStatus::OK);
    const auto run = [&](const std::string& sql, size_t demand=0) {
        auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(
            &owner,db,"source",owner.prepareBoundQuery(db,sql)),demand);
        if (!result.ok) {
            assert(!result.errorSqlState.empty());
            assert(result.rows.empty() && result.structuredRows.empty() && result.structuredNulls.empty());
        }
        result.throwIfFailed();
        return result;
    };
    auto result=run("SELECT d.id,d.\"ID\" FROM source d WHERE(SELECT d.\"ID\")=2147483649");
    assert(result.ok && result.structuredRowsAvailable);
    assert((result.structuredRows==std::vector<std::vector<std::string>>{{"2","2147483649"}}));
    result=run("SELECT d.id,d.payload FROM source d WHERE(SELECT d.payload)='' ");
    assert((result.structuredRows==std::vector<std::vector<std::string>>{{"1",""}}));
    assert(!result.structuredNulls[0][1]);
    result=run("SELECT d.id,d.payload FROM source d WHERE(SELECT d.payload)='null'");
    assert((result.structuredRows==std::vector<std::vector<std::string>>{{"2","null"}}));
    result=run("SELECT d.id,d.payload FROM source d WHERE(SELECT d.payload) IS NULL");
    assert(result.structuredRows.size()==1 && result.structuredRows[0][0]=="3" && result.structuredNulls[0][1]);
    assert(owner.createUDF(db,"native_where_failure",{},{},
        "BEGIN RAISE EXCEPTION 'native where error'; END;",'v',"plpgsql","int") == DBStatus::OK);
    const auto function=owner.getUDF(db,"native_where_failure");
    assert(function.name=="native_where_failure" && function.paramNames.empty() && !function.expression.empty());
    bool failed=false;
    try { (void)run("SELECT id FROM source WHERE(SELECT native_where_failure())>0"); }
    catch(const DbError& error) {
        failed=error.sqlState()=="P0001";
        if (!failed) std::cerr << "native failure actual " << error.sqlState() << " " << error.what() << '\n';
    }
    assert(failed);
    result=run("SELECT id FROM source WHERE CASE WHEN false THEN(SELECT native_where_failure())>0 ELSE true END",1);
    assert(result.ok && result.structuredRows.size()==1);
    size_t calls=0;
    owner.setPlpgsqlQueryExecutor([&](const std::string& database,const std::string&,
                                    const PlPgsqlQueryOptions& options) {
        assert(database==db && options.purpose==PlPgsqlQueryOptions::Purpose::OrdinarySubquery && options.maxRows==2);
        ++calls;
        PlPgsqlQueryResult value; value.ok=true; value.columnCount=value.rowCount=1;
        value.columnTypes={"integer"}; value.firstRow={"1"}; return value;
    });
    result=run("SELECT id FROM source WHERE(SELECT 1)>0");
    assert(result.structuredRows.size()==3 && calls==1);
    calls=0;
    result=run("SELECT id FROM source WHERE(SELECT 1)>0 AND(SELECT 1)>0");
    assert(result.structuredRows.size()==3 && calls==2); // genuine sites never coalesce
    calls=0;
    result=run("SELECT id FROM source WHERE(SELECT 1)>0",1);
    assert(result.structuredRows.size()==1 && calls==1);
    owner.setPlpgsqlQueryExecutor({});
    assert(owner.dropDatabase(db)==DBStatus::OK);
    cleanupTestDb("ordinary_scalar_where_plan");
    std::cout << "[ORDINARY SCALAR WHERE PLAN] passed\n";
}
