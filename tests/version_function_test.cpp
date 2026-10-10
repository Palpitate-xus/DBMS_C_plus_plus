#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "common/version.h"
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
    const auto banner=std::string("DBMS-C++ ")+DBMS_VERSION_STRING+" (PostgreSQL protocol 3.0)";
    for(const auto* sql:{"SELECT version()","SELECT pg_catalog.version()"}) {
        auto parsed=SQLParser().parseForBinding(sql);assert(parsed.success);
        auto* expression=static_cast<SelectStmt*>(parsed.stmt.get())->selectList[0].expr.get();
        const auto* call=dynamic_cast<const FunctionCallExpr*>(expression);assert(call);
        assert(evaluator.scalarFunctionResultType(call)=="text" && evaluator.scalarFunctionVolatility(call)=='s');
        evaluator.bindScalarFunctions(expression);
        const auto value=evaluator.eval(expression,{});
        assert(!value.isNull && value.typeName=="text" && value.value==banner);
    }
    for(const auto* sql:{"SELECT version(1)","SELECT version(arg => 1)","SELECT version() OVER ()"}) {
        auto parsed=SQLParser().parseForBinding(sql);assert(parsed.success);
        std::string state;
        try{evaluator.bindScalarFunctions(static_cast<SelectStmt*>(parsed.stmt.get())->selectList[0].expr.get());}
        catch(const DbError& error){state=error.sqlState();}
        assert(state==(std::string(sql).find("OVER")!=std::string::npos?"42809":"42883"));
    }
    StorageEngine local;
    const auto database=testDbPath("version_function");
    assert(local.createDatabase(database)==DBStatus::OK);
    const auto query=local.prepareBoundQuery(database,"SELECT USER AS who,version() AS banner,pg_typeof(version()) AS kind");
    assert(query.output.size()==3 && query.output[0].typeOid==19 && query.output[1].typeOid==25 && query.output[2].typeOid==2206);
    const auto result=ExprHelper::evalString("version()",{}, {},database,"",&local);
    assert(result.ok && !result.isNull && result.typeName=="text" && result.value==banner);
    assert(local.dropDatabase(database)==DBStatus::OK);
    std::cout<<"[VERSION FUNCTION] truthful identity, metadata, namespace, signature and ordinary expression binding passed\n";
}
