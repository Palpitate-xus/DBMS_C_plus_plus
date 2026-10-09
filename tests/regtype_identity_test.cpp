#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "expression/regtype_input.h"
#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    size_t checks=0,failures=0;
    const auto require=[&](bool pass,const std::string& role){++checks;failures+=!pass;std::cout<<"REGTYPE_IDENTITY "<<role<<" pass="<<pass<<'\n';};
    const auto db=testDbPath("regtype_identity");
    require(g_engine.createDatabase(db)==DBStatus::OK,"create isolated database");
    const auto execute=[&](const std::string& sql) {
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
        auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get());
        auto result=QueryPlanner::executePlanChecked(std::move(plan));result.throwIfFailed();return result;
    };
    for(const auto& expression:{"'integer'::regtype = 23::oid","23::oid = 'integer'::regtype","'boolean'::regtype < 'bigint'::regtype"}) {
        require(execute(std::string("SELECT ")+expression+" AS value").structuredRows==std::vector<std::vector<std::string>>{{"t"}},std::string("actual typed comparison ")+expression);
        const auto legacy=ExprHelper::evalString(expression,{},{},db);
        require(legacy.ok && legacy.value=="t","stored expression uses same physical comparator");
    }
    ExprValue type("regtype","integer");type.objectOid=23;
    ExprValue oid("oid","00023");
    const auto binding=ExprEvaluator::resolveComparison("=","regtype","oid");
    ExprEvaluator evaluator;evaluator.setCurrentDB(db);
    const auto first=evaluator.coerceComparison(binding,type,true),second=evaluator.coerceComparison(binding,oid,false);
    require(evaluator.comparePrepared(binding,type,oid).asBool(),"same physical OID despite different renderings");
    require(ExprEvaluator::comparisonHashKey(binding,first,true)==ExprEvaluator::comparisonHashKey(binding,second,false),"equality and hash agree");
    require(execute("SELECT CAST(ARRAY[23,-1] AS regtype[]) AS value").structuredRows==std::vector<std::vector<std::string>>{{"{integer,4294967295}"}},"contextual ARRAY uses actual integer/OID cast");
    require(execute("SELECT CAST('[0:1]={integer,boolean}'::regtype[] AS integer[]) AS value").structuredRows==std::vector<std::vector<std::string>>{{"[0:1]={23,16}"}},"array output retains physical identity and lower bound");
    const auto sorted=execute("SELECT value FROM (VALUES ('integer'::regtype),('boolean'::regtype),('bigint'::regtype),('text'::regtype),(NULL::regtype)) AS types(value) ORDER BY value NULLS LAST");
    require(sorted.structuredRows==std::vector<std::vector<std::string>>{{"boolean"},{"bigint"},{"integer"},{"text"},{""}} && sorted.structuredNulls.back()==std::vector<bool>{true},"derived VALUES sorts physical OIDs and preserves NULL");
    const auto distinct=execute("SELECT DISTINCT value FROM (VALUES ('integer'::regtype),('boolean'::regtype),('bigint'::regtype),('int4'::regtype),(NULL::regtype),(NULL::regtype)) AS types(value) ORDER BY value NULLS FIRST");
    require(distinct.structuredRows==std::vector<std::vector<std::string>>{{""},{"boolean"},{"bigint"},{"integer"}} && distinct.structuredNulls.front()==std::vector<bool>{true},"DISTINCT aliases/NULL share physical identity");
    std::cout<<"REGTYPE_IDENTITY_CHECKED="<<checks<<" FAILED="<<failures<<'\n';
    return checks==13 && !failures?0:1;
}
