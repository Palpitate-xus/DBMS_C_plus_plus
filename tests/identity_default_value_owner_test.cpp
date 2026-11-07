#include "commands/TableManage.h"
#include "catalog/CatalogService.h"
#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "expression/IdentityValue.h"
#include "expression/LocalExpressionSyntax.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <iostream>
#include <stdexcept>
#include <string>
extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
unsigned checked=0;
void need(bool value,const std::string& reason) {
    ++checked;if(!value)throw std::runtime_error(reason);
}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000,60000);
        StorageEngine owner;owner.setBackgroundIntervals(60000,60000);
        const auto database=testDbPath("identity_default_value_owner");
        cleanupTestDb("identity_default_value_owner");
        need(owner.createDatabase(database,"utf8")==DBStatus::OK,"real native owner database");
        need(owner.createSequence(database,"native_identity_seq",70,3)==DBStatus::OK,"real native sequence file");
        auto& catalog=owner.catalogService().get(database);
        const auto schema=catalog.getNamespaceByName("public");
        need(bool(schema),"real catalog namespace");
        PgClassRow relation;relation.relkind='S';relation.relname="native_identity_seq";relation.relnamespace=schema->oid;
        const auto oid=catalog.createClass(relation);need(oid!=0,"real allocated catalog sequence OID");
        // The local owner can use its own live class before publication. No
        // synthetic SQL session or global-engine redirection is involved.
        auto value=identity_value_detail::make(oid);
        auto* call=static_cast<FunctionCallExpr*>(value.get());
        ExprEvaluator evaluator;evaluator.setCurrentDB(database);unsigned callbackCalls=0;
        evaluator.registerFunction(call->funcName,[&](const std::vector<ExprValue>&) {
            ++callbackCalls;return ExprValue("bigint","900");
        },'i');
        evaluator.bindScalarFunctions(value.get(),&owner);
        need(evaluator.hasScalarFunction(call,&owner),"internal value has analyzed metadata");
        need(evaluator.scalarFunctionVolatility(call,&owner)=='v',"identity value stays volatile despite same-named immutable callback");
        need(evaluator.scalarFunctionResultType(call,&owner)=="bigint","generator metadata is declared before evaluation");
        auto first=evaluator.eval(value.get(),RowContext{});
        need(!first.isNull && first.typeName=="bigint" && first.value=="70","analysis consumed no value and actual owner generates first value");
        need(callbackCalls==0,"bound identity never invokes same-named native callback");
        auto copied=local_expression_syntax_detail::cloneSyntaxExpression(value.get());
        evaluator.bindScalarFunctions(copied.get(),&owner);
        need(evaluator.eval(copied.get(),RowContext{}).value=="73","structured expression copy retains real binding");
        auto malformed=local_expression_syntax_detail::cloneSyntaxExpression(value.get());
        static_cast<LiteralExpr*>(static_cast<FunctionCallExpr*>(malformed.get())->args[0].get())->value="0";
        bool rejected=false;
        try {evaluator.bindScalarFunctions(malformed.get(),&owner);}
        catch(const DbError& error){rejected=error.sqlState()=="XX001";}
        need(rejected,"invalid copied binding rejects before generation");
        auto ordinary=local_expression_syntax_detail::cloneSyntaxExpression(value.get());
        static_cast<FunctionCallExpr*>(ordinary.get())->identitySequenceOid=0;
        evaluator.bindScalarFunctions(ordinary.get(),&owner);
        need(evaluator.eval(ordinary.get(),RowContext{}).value=="900" && callbackCalls==1,
             "ordinary native callback retains its actual independent role");
        evaluator.bindScalarFunctions(value.get(),&owner);
        need(evaluator.eval(value.get(),RowContext{}).value=="76" && callbackCalls==1,
             "rejected or ordinary calls consumed no bound sequence value");
        need(catalog.persistAll() && owner.checkpoint(database),"actual native definition and runtime persist");
        std::cout<<"[IDENTITY VALUE OWNER] checked="<<checked<<" first=70 copied=73 final=76 callback_calls="<<callbackCalls
                 <<" native_value_owner_only=true\n";
        return 0;
    } catch(const std::exception& failure) {
        std::cerr<<"IDENTITY_VALUE_OWNER_FAIL "<<failure.what()<<'\n';return 1;
    }
}
