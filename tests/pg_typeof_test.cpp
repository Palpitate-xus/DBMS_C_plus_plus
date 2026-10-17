#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    ExprEvaluator evaluator;
    const std::vector<std::pair<std::string,uint32_t>> cases={
        {"1",23},{"2147483648",20},{"1.5",1700},{"true",16},
        {"NULL",705},{"'abc'",705},{"NULL::SMALLINT",21},{"NULL::NAME",19},
        {"ARRAY[1,2]",1007},{"NULL::INTEGER[]",1007},{"current_user",19},
        {"CURRENT_DATE",1082},{"'integer'::REGTYPE",2206}};
    for(const auto& item:cases) {
        auto parsed=SQLParser().parseForBinding("SELECT pg_typeof("+item.first+")");
        assert(parsed.success);
        auto* expression=static_cast<SelectStmt*>(parsed.stmt.get())->selectList[0].expr.get();
        evaluator.bindScalarFunctions(expression);
        ExprHelper::prepareArrayTypes(expression);
        const auto value=evaluator.eval(expression,{});
        assert(value.typeName=="regtype" && !value.isNull && value.objectOid==item.second);
    }
    auto parsed=SQLParser().parseForBinding("SELECT pg_typeof(tick())");
    assert(parsed.success);
    auto* outer=static_cast<FunctionCallExpr*>(static_cast<SelectStmt*>(parsed.stmt.get())->selectList[0].expr.get());
    static_cast<FunctionCallExpr*>(outer->args.front().get())->resolvedResultType="smallint";
    size_t effects=0;
    evaluator.registerFunction("tick",[&](const std::vector<ExprValue>&){++effects;return ExprValue("smallint","",true);});
    evaluator.bindScalarFunctions(outer);
    assert(effects==0 && evaluator.scalarFunctionResultType(outer)=="regtype");
    const auto tickType=evaluator.eval(outer,{});
    if(tickType.objectOid!=21 || effects!=1)
        std::cerr<<"PG_TYPEOF_TICK oid="<<tickType.objectOid.value_or(0)<<" effects="<<effects
                 <<" declared="<<static_cast<FunctionCallExpr*>(outer->args.front().get())->resolvedResultType<<'\n';
    assert(tickType.objectOid==21);
    assert(effects==1);
    auto bad=SQLParser().parseForBinding("SELECT pg_typeof(1/0)");
    assert(bad.success);
    std::string state;
    try{(void)evaluator.eval(static_cast<SelectStmt*>(bad.stmt.get())->selectList[0].expr.get(),{});}
    catch(const DbError& error){state=error.sqlState();}
    assert(state=="22012");

    // The actual independent engine owns stored declarations, even when the
    // result is NULL and no global engine has that database or routine.
    StorageEngine local;
    const auto database=testDbPath("pg_typeof_local_owner");
    assert(local.createDatabase(database)==DBStatus::OK);
    assert(local.createUDF(database,"localtyped",{}, {},"SELECT NULL::SMALLINT",'i',"sql","smallint")==DBStatus::OK);
    const auto result=ExprHelper::evalString("pg_typeof(localtyped())",{}, {},database,"",&local);
    if(!result.ok)std::cerr<<result.sqlState<<' '<<result.error<<'\n';
    assert(result.ok && !result.isNull && result.typeName=="regtype" && result.value=="smallint");
    const auto query=local.prepareBoundQuery(database,"SELECT pg_typeof(NULL::SMALLINT),pg_typeof(localtyped())");
    assert(query.output.size()==2 && query.output[0].typeOid==2206 && query.output[1].typeOid==2206);
    assert(local.dropDatabase(database)==DBStatus::OK);
    std::cout<<"[PG TYPEOF] declaration identity, UNKNOWN, typed NULL, effects, errors and local owner passed\n";
}
