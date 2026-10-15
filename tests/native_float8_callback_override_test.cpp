#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <exception>
#include <functional>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="r83_callback_override",database=testDbPath(name);
    cleanupTestDb(name);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,const std::function<bool()>& action) {
        ++controls;
        try {
            if(action())return;
            std::cerr<<"R83_CALLBACK_DIRECT_FAIL "<<label<<" assertion\n";
        } catch(const DbError& error) {
            std::cerr<<"R83_CALLBACK_DIRECT_FAIL "<<label<<" SQLSTATE="<<error.sqlState()<<' '<<error.what()<<'\n';
        } catch(const std::exception& error) {
            std::cerr<<"R83_CALLBACK_DIRECT_FAIL "<<label<<' '<<error.what()<<'\n';
        }
        ++failures;
    };
    const std::vector<std::string> names={
        "sin","cos","tan","asin","acos","atan","sinh","cosh","tanh",
        "asinh","acosh","atanh","degrees","radians","cot","cbrt","atan2","pi"};
    for(const auto& context:std::vector<std::string>{"",database}) {
        session.currentDB=context;
        const std::string contextLabel=context.empty()?"empty-db":"real-db";
        for(const auto& callee:names)for(const bool withText:{false,true}) {
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);
            size_t calls=0;bool argumentsPreserved=true;
            const auto callback=[&](const std::vector<ExprValue>& args) {
                ++calls;
                argumentsPreserved=argumentsPreserved && args.size()==(withText?1u:0u);
                if(withText && args.size()==1)
                    argumentsPreserved=argumentsPreserved && args.front().typeName=="text" &&
                        !args.front().isNull && args.front().value=="payload";
                return ExprValue("integer","-7");
            };
            if(withText)evaluator.registerFunction(callee,callback,'s');
            else evaluator.registerFunction(callee,callback);
            SQLParser parser;
            auto parsed=parser.parseForBinding("SELECT "+callee+"("+(withText?"'payload'::text":"")+")");
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            auto* call=select?dynamic_cast<FunctionCallExpr*>(select->selectList.front().expr.get()):nullptr;
            if(!call)return 2;
            const std::string label=contextLabel+" "+callee+(withText?"(TEXT)":"()");
            check(label+" metadata",[&] {
                return evaluator.hasScalarFunction(call,&g_engine) &&
                    evaluator.scalarFunctionResultType(call,&g_engine).empty() &&
                    evaluator.scalarFunctionVolatility(call,&g_engine)==(withText?'s':'v') && calls==0;
            });
            check(label+" binding",[&] {
                evaluator.bindScalarFunctions(call,&g_engine);
                return calls==0 && evaluator.scalarFunctionResultType(call,&g_engine).empty();
            });
            check(label+" runtime",[&] {
                const auto result=evaluator.eval(call,RowContext{});
                return calls==1 && argumentsPreserved && !result.isNull &&
                    result.typeName=="integer" && result.value=="-7";
            });
        }
        // Preserve the independently observed pre-change nested behavior;
        // this does not claim opaque callback return declarations are known.
        for(const bool withText:{false,true}) {
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);size_t calls=0;
            evaluator.registerFunction("sin",[&](const std::vector<ExprValue>& args) {
                ++calls;
                return ExprValue("integer",args.empty()?"-7":"-9");
            });
            SQLParser parser;
            auto parsed=parser.parseForBinding(std::string("SELECT abs(sin(")+
                (withText?"'payload'::text":"")+"))");
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            if(!select)return 2;
            check(contextLabel+(withText?" abs(sin(TEXT))":" abs(sin())"),[&] {
                evaluator.bindScalarFunctions(select->selectList.front().expr.get(),&g_engine);
                const auto value=evaluator.eval(select->selectList.front().expr.get(),RowContext{});
                return !value.isNull && value.typeName=="double precision" &&
                    value.value==(withText?"9":"7") && calls==1;
            });
        }
        // UNKNOWN input belongs to this callback too, including when nested
        // in a cast/operator. A default registry must not parse it as float8.
        for(const auto* literal:{"'payload'","'0'"})for(const int form:{0,1,2,3}) {
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);
            size_t calls=0;bool originalArgument=true;
            evaluator.registerFunction("sin",[&](const std::vector<ExprValue>& args) {
                ++calls;
                originalArgument=originalArgument && args.size()==1 &&
                    args.front().typeName=="character varying" && !args.front().isNull &&
                    args.front().value==(std::string(literal)=="'payload'"?"payload":"0");
                return ExprValue("integer","-7");
            });
            const std::string call=std::string("sin(")+literal+")";
            const std::string expression=form==0?call:form==1?"abs("+call+")":
                form==2?"CAST("+call+" AS INTEGER)":call+"+1";
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            if(!select)return 2;
            check(contextLabel+" UNKNOWN "+expression,[&] {
                evaluator.bindScalarFunctions(select->selectList.front().expr.get(),&g_engine);
                if(calls!=0)return false;
                const auto value=evaluator.eval(select->selectList.front().expr.get(),RowContext{});
                return calls==1 && originalArgument && !value.isNull &&
                    value.typeName==(form==1?"double precision":"integer") &&
                    value.value==(form==1?"7":form==3?"-6":"-7");
            });
        }
    }
    check("complete direct and nested matrix",[&]{return controls==237;});
    std::cout<<"R83_CALLBACK_DIRECT_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    session.currentDB=database;
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb(name);
    return failures?1:0;
}
