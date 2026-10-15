// Finite MOD result domains, typed NULLs, widths and callback ownership.
// SQL unsupported-type rejection is deliberately not a native API contract.
#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <algorithm>
#include <exception>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
struct Kind {const char* type; uint32_t oid;};
const std::vector<Kind> kinds = {{"smallint",21},{"integer",23},{"bigint",20},{"numeric",1700}};
struct Control {
    std::string expression, type;
    uint32_t oid;
    std::optional<std::string> value;
    std::string state;
};

std::string stateOf(const std::exception& error) {
    if (const auto* structured = dynamic_cast<const dbms::DbError*>(&error)) return structured->sqlState();
    const std::string message = error.what(); const auto at = message.find("SQLSTATE ");
    return at == std::string::npos ? std::string{} : message.substr(at + 9, 5);
}

std::unique_ptr<dbms::FunctionCallExpr> cellsCall(const std::vector<dbms::ExprValue>& cells) {
    auto call = std::make_unique<dbms::FunctionCallExpr>(); call->funcName = "mod";
    for (size_t i = 0; i < cells.size(); ++i) {
        auto parameter = std::make_unique<dbms::ParameterExpr>();
        parameter->slot = i; parameter->declaredType = cells[i].typeName;
        call->args.push_back(std::move(parameter));
    }
    return call;
}

std::vector<Control> controls() {
    std::vector<Control> result;
    for (size_t l = 0; l < kinds.size(); ++l) for (size_t r = 0; r < kinds.size(); ++r) {
        const auto& domain = kinds[std::max(l,r)];
        for (const auto nulls : {std::pair{false,false},std::pair{true,false},std::pair{false,true},std::pair{true,true}}) {
            const std::string left = std::string(nulls.first?"NULL":"9") + "::" + kinds[l].type;
            const std::string right = std::string(nulls.second?"NULL":"4") + "::" + kinds[r].type;
            result.push_back({"mod("+left+','+right+')',domain.type,domain.oid,
                nulls.first || nulls.second ? std::nullopt : std::optional<std::string>{"1"},{}});
        }
    }
    result.push_back({"pg_catalog.mod(5000000000::bigint,6000000000::bigint)","bigint",20,"5000000000",{}});
    result.push_back({"mod(5000000000::bigint,6000000000::bigint)/2","bigint",20,"2500000000",{}});
    for (const auto& kind : kinds) for (const auto& sign : std::vector<std::vector<std::string>>{
        {"-10","3","-1"},{"10","-3","1"},{"-10","-3","-1"}})
        result.push_back({"mod("+sign[0]+"::"+kind.type+','+sign[1]+"::"+kind.type+')',kind.type,kind.oid,sign[2],{}});
    for (size_t i = 0; i < 3; ++i) {
        const std::string minimum = std::vector<std::string>{"-32768","-2147483648","-9223372036854775808"}[i];
        result.push_back({"mod('"+minimum+"'::"+kinds[i].type+",-1::"+kinds[i].type+')',kinds[i].type,kinds[i].oid,"0",{}});
    }
    result.push_back({"mod(100000000000000000000001::numeric,3::integer)","numeric",1700,"2",{}});
    result.push_back({"mod(10.50::numeric,3.0::numeric)","numeric",1700,"1.50",{}});
    for (const auto& special : std::vector<std::vector<std::string>>{
        {"NaN","0","NaN"},{"0","NaN","NaN"},{"NaN","Infinity","NaN"},
        {"Infinity","2","NaN"},{"-Infinity","2","NaN"},{"Infinity","Infinity","NaN"},
        {"2.00","Infinity","2.00"},{"-2.00","Infinity","-2.00"},{"2.00","-Infinity","2.00"}})
        result.push_back({"mod('"+special[0]+"'::numeric,'"+special[1]+"'::numeric)","numeric",1700,special[2],{}});
    result.push_back({"mod(NULL::numeric,'NaN'::numeric)","numeric",1700,std::nullopt,{}});
    for (const auto& kind : kinds)
        result.push_back({std::string("mod(9::")+kind.type+",0::"+kind.type+')',kind.type,kind.oid,std::nullopt,"22012"});
    result.push_back({"mod('Infinity'::numeric,0::numeric)","numeric",1700,std::nullopt,"22012"});
    return result;
}
} // namespace

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string testName = "mod_result_domain", database = testDbPath(testName);
    cleanupTestDb(testName);
    if (g_engine.createDatabase(database) != DBStatus::OK) return 2;
    Session session; session.username="testuser"; session.permission=1; session.currentDB=database;
    auto* previous=currentSession(); setCurrentSession(&session);
    struct Restore {Session* previous; ~Restore(){setCurrentSession(previous);}} restore{previous};
    size_t checks=0, failures=0;
    const auto check=[&](bool good,const std::string& label) {
        ++checks; if(!good){++failures;std::cerr<<"MOD_RESULT_DOMAIN_FAIL "<<label<<'\n';}
    };
    const auto cases=controls();
    for(const auto& control:cases) {
        try {
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+control.expression);
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            if(!select || select->selectList.size()!=1)return 2;
            auto* expression=select->selectList.front().expr.get();
            ExprEvaluator evaluator;evaluator.setCurrentDB(database);
            const auto metadata=ExprHelper::inferParsedResultType(expression,{},database,&g_engine,&evaluator);
            evaluator.bindScalarFunctions(expression,&g_engine);
            const auto actual=evaluator.eval(expression,RowContext{});
            check(control.state.empty() && metadata==control.type && actual.typeName==control.type &&
                actual.isNull==!control.value && (!control.value || actual.value==*control.value),
                control.expression+" metadata="+metadata+" actual="+actual.typeName+" value="+actual.value);
        } catch(const std::exception& error) {
            check(!control.state.empty() && stateOf(error)==control.state,
                control.expression+" SQLSTATE="+stateOf(error)+" "+error.what());
        }
    }
    // Real empty-source descriptors must use both input declarations, even
    // when the builtin never executes. Include both argument orders.
    for(const auto& left:kinds)for(const auto& right:kinds)
        for(const std::string tail:{""," WHERE false"," LIMIT 0"}) {
            const auto rank=[&](const char* type){for(size_t i=0;i<kinds.size();++i)if(std::string(kinds[i].type)==type)return i;return size_t{0};};
            const auto& domain=kinds[std::max(rank(left.type),rank(right.type))];
            const std::string sql=std::string("SELECT mod(9::")+left.type+",4::"+right.type+')'+tail;
            try {
                const auto query=g_engine.prepareBoundQuery(database,sql);
                check(query.output.size()==1 && query.output[0].type==domain.type && query.output[0].typeOid==domain.oid,sql);
            } catch(const std::exception& error){check(false,sql+" "+error.what());}
        }
    ExprEvaluator builtin;
    // Public native typed-NULL cells, not legacy untyped NULL literal copies.
    for(size_t l=0;l<kinds.size();++l)for(size_t r=0;r<kinds.size();++r)
        for(const auto nulls:{std::pair{false,false},std::pair{true,false},std::pair{false,true},std::pair{true,true}}) {
            const std::vector<ExprValue> cells={{kinds[l].type,nulls.first?"":"9",nulls.first},
                                               {kinds[r].type,nulls.second?"":"4",nulls.second}};
            auto call=cellsCall(cells);RowContext row;row.setParameters(cells);
            try {
                const auto actual=builtin.eval(call.get(),row);
                check(actual.typeName==kinds[std::max(l,r)].type && actual.isNull==(nulls.first||nulls.second) &&
                    (actual.isNull || actual.value=="1"),"native typed cells "+std::string(kinds[l].type)+"/"+kinds[r].type);
            } catch(const std::exception& error){check(false,"native typed cells "+std::string(error.what()));}
        }
    // The matching old Root baseline verifies these permissive native values.
    // They intentionally are NOT PostgreSQL SQL overloads.
    struct Legacy {std::string type,left,right,resultType,value;};
    for(const auto& legacy:std::vector<Legacy>{{"double precision","10.5","3.0","numeric","1.5"},
        {"real","10.50","3.0","numeric","1.50"},{"text","10","3","integer","1"},
        {"text","10.50","3.0","numeric","1.50"}}) {
        const std::vector<ExprValue> cells={{legacy.type,legacy.left},{legacy.type,legacy.right}};
        auto call=cellsCall(cells);RowContext row;row.setParameters(cells);
        try {const auto actual=builtin.eval(call.get(),row);
            check(!actual.isNull && actual.typeName==legacy.resultType && actual.value==legacy.value,
                "legacy native permissive "+legacy.type);
        } catch(const std::exception& error){check(false,"native permissive "+std::string(error.what()));}
    }
    // Opaque MOD replacement has no return declaration. A new genuine MOD
    // domain must not change its old arg0-derived outer ABS selection.
    struct Opaque {std::vector<ExprValue> cells;std::string oldNestedType;};
    const std::vector<Opaque> opaque={
        {{{"smallint","9"},{"bigint","4"}},"smallint"},
        {{{"numeric","9"},{"integer","4"}},"numeric"},
        {{{"text","9"},{"text","4"}},"text"},
        {{{"double precision","10.5"},{"double precision","3.0"}},"double precision"},
    };
    for(const auto& context:std::vector<std::string>{"",database})for(const bool immutable:{false,true}) {
        session.currentDB=context;
        for(const auto& input:opaque)for(const bool nested:{false,true}) {
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);size_t calls=0;bool same=true;
            const auto callback=[&](const std::vector<ExprValue>& actual) {
                ++calls;same=same && actual.size()==input.cells.size();
                for(size_t i=0;i<actual.size() && i<input.cells.size();++i)
                    same=same && actual[i].typeName==input.cells[i].typeName && actual[i].value==input.cells[i].value &&
                        actual[i].isNull==input.cells[i].isNull;
                return ExprValue("integer","-7");
            };
            if(immutable)evaluator.registerFunction("mod",callback,'i');else evaluator.registerFunction("mod",callback);
            auto native=cellsCall(input.cells);auto* nativeSite=native.get();ExprPtr expression=std::move(native);
            if(nested){auto outer=std::make_unique<FunctionCallExpr>();outer->funcName="abs";outer->args.push_back(std::move(expression));expression=std::move(outer);}
            RowContext row;row.setParameters(input.cells);bool pure=false;
            const bool reject=nested && input.oldNestedType=="text";
            try {
                pure=evaluator.hasScalarFunction(nativeSite,&g_engine) && evaluator.scalarFunctionResultType(nativeSite,&g_engine).empty() &&
                    evaluator.scalarFunctionVolatility(nativeSite,&g_engine)==(immutable?'i':'v') && calls==0;
                evaluator.bindScalarFunctions(expression.get(),&g_engine);
                pure=pure && calls==0 && evaluator.scalarFunctionResultType(nativeSite,&g_engine).empty();
                const auto actual=evaluator.eval(expression.get(),row);
                check(!reject && pure && same && calls==1 && !actual.isNull && actual.value==(nested?"7":"-7") &&
                    actual.typeName==(nested?input.oldNestedType:"integer"),"opaque direct/nested owner "+input.oldNestedType);
            } catch(const std::exception& error) {
                check(reject && pure && calls==0 && stateOf(error)=="42883","opaque nested error "+stateOf(error));
            }
        }
        ExprEvaluator zero;zero.setCurrentDB(context);size_t calls=0;
        const auto callback=[&](const std::vector<ExprValue>& args){++calls;return ExprValue("text",args.empty()?"owned":"bad");};
        if(immutable)zero.registerFunction("mod",callback,'i');else zero.registerFunction("mod",callback);
        FunctionCallExpr call;call.funcName="mod";
        try {
            const bool pure=zero.scalarFunctionResultType(&call,&g_engine).empty() && calls==0;
            zero.bindScalarFunctions(&call,&g_engine);const bool pureBind=calls==0;
            const auto actual=zero.eval(&call,RowContext{});
            check(pure && pureBind && calls==1 && actual.typeName=="text" && actual.value=="owned","opaque zero-arg owner");
        } catch(const std::exception& error){check(false,"opaque zero args "+std::string(error.what()));}
    }
    session.currentDB=database;
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE SCHEMA mod_result_user",session) ||
       ddl.executeSql("CREATE FUNCTION mod_result_user.mod(x INTEGER,y INTEGER) RETURNS TEXT LANGUAGE SQL IMMUTABLE AS $$SELECT 'stored'::text$$",session))return 2;
    for(const std::string tail:{""," WHERE false"," LIMIT 0"}) {
        try {
            const auto query=g_engine.prepareBoundQuery(database,"SELECT mod_result_user.mod(9,4)"+tail);
            bool good=query.output.size()==1 && query.output[0].type=="text" && query.output[0].typeOid==25;
            if(tail.empty()) {
                SQLParser parser;auto parsed=parser.parseForBinding("SELECT mod_result_user.mod(9,4)");
                auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;if(!select)return 2;
                ExprEvaluator evaluator;evaluator.setCurrentDB(database);auto* expression=select->selectList.front().expr.get();
                evaluator.bindScalarFunctions(expression,&g_engine);const auto actual=evaluator.eval(expression,RowContext{});
                good=good && actual.typeName=="text" && actual.value=="stored" && !actual.isNull;
            }
            check(good,"qualified actual UDF return declaration"+tail);
        } catch(const std::exception& error){check(false,"qualified UDF "+std::string(error.what()));}
    }
    check(cases.size()==98 && checks==253,"complete finite result-domain/compatibility matrix");
    std::cout<<"MOD_RESULT_DOMAIN_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;cleanupTestDb(testName);
    return failures?1:0;
}
