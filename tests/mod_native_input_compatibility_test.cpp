// Preserve native MOD inputs with opaque, CASE and unbound metadata shapes.
// Historical and matching-current comparisons are documented independently.
#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <exception>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
std::string stateOf(const std::exception& error) {
    if (const auto* db = dynamic_cast<const dbms::DbError*>(&error)) return db->sqlState();
    const std::string message=error.what();const auto pos=message.find("SQLSTATE ");
    return pos==std::string::npos?std::string{}:message.substr(pos+9,5);
}
struct Input {
    const char* sql;
    const char* leftType;
    const char* leftValue;
    const char* rightValue;
    const char* builtinType;
    const char* builtinValue;
    unsigned inputCount = 1;
};
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="mod_native_input_compatibility",database=testDbPath(name);
    cleanupTestDb(name);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    size_t checks=0,failures=0;
    const auto check=[&](bool good,const std::string& label) {
        ++checks;failures+=!good;std::cout<<"MOD_COMPLEX_INPUT "<<label<<" pass="<<good<<'\n';
    };
    const std::vector<Input> inputs={
        {"mod(9::integer,opaque_input()-1)","integer","9","1","integer","0"},
        {"mod('9'::text,opaque_input()-1)","text","9","1","integer","0"},
        {"mod(9.5::double precision,opaque_input()-1)","double precision","9.5","1","numeric","0.5"},
        {"mod(9::integer,opaque_input())","integer","9","2","integer","1"},
        {"mod(9::integer,CASE WHEN true THEN opaque_input() ELSE 1 END)","integer","9","2","integer","1"},
        // An intentionally permissive native CASE, not a legal PG SQL type
        // combination: the unselected TEXT arm must not add new analysis.
        {"mod(9::integer,CASE WHEN true THEN 2 ELSE 'payload'::text END)","integer","9","2","integer","1",0},
    };
    // mode0: genuine MOD; mode1: default opaque replacement;
    // mode2: explicitly immutable opaque replacement (not a builtin marker).
    for(const auto& context:std::vector<std::string>{"",database})
        for(int mode=0;mode<3;++mode)for(const auto& input:inputs)for(const bool nested:{false,true}) {
            session.currentDB=context;
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);
            size_t inputCalls=0,modCalls=0;bool sameArguments=true;
            evaluator.registerFunction("opaque_input",[&](const std::vector<ExprValue>& args) {
                ++inputCalls;sameArguments=sameArguments && args.empty();return ExprValue("integer","2");
            });
            const auto replacement=[&](const std::vector<ExprValue>& args) {
                ++modCalls;sameArguments=sameArguments && args.size()==2;
                if(args.size()==2)sameArguments=sameArguments && !args[0].isNull && !args[1].isNull &&
                    args[0].typeName==input.leftType && args[0].value==input.leftValue &&
                    args[1].typeName=="integer" && args[1].value==input.rightValue;
                return ExprValue("integer","-7");
            };
            if(mode==1)evaluator.registerFunction("mod",replacement);
            if(mode==2)evaluator.registerFunction("mod",replacement,'i');
            const std::string expression=nested?std::string("abs(")+input.sql+')':input.sql;
            const std::string label=std::string(context.empty()?"emptyDB ":"realDB ")+"mode="+
                std::to_string(mode)+' '+expression;
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            auto* root=select?dynamic_cast<FunctionCallExpr*>(select->selectList.front().expr.get()):nullptr;
            auto* mod=root && nested?dynamic_cast<FunctionCallExpr*>(root->args.front().get()):root;
            if(!mod)return 2;
            // Standalone MOD metadata used to need only arg0. Probe this
            // independently so an early 42883 cannot impersonate the old
            // later outer-ABS TEXT rejection (which must still have calls0).
            std::string metadata,metadataState;
            bool ownerPure=false;
            try {
                ownerPure=evaluator.hasScalarFunction(mod,&g_engine) &&
                    evaluator.scalarFunctionResultType(mod,&g_engine).empty();
                metadata=ExprHelper::inferParsedInputType(mod,{},context,&g_engine,&evaluator);
            } catch(const std::exception& error){metadataState=stateOf(error);}
            check(ownerPure && metadataState.empty() && metadata==input.leftType && inputCalls==0 && modCalls==0,
                label+" metadata="+metadata+" state="+metadataState+" inputCalls="+std::to_string(inputCalls)+
                " modCalls="+std::to_string(modCalls));
            const bool reject=nested && std::string(input.leftType)=="text";
            std::string stage="bind";
            try {
                evaluator.bindScalarFunctions(root,&g_engine);
                const bool pureBind=inputCalls==0 && modCalls==0;
                stage="eval";const auto actual=evaluator.eval(root,RowContext{});
                const std::string expectedType=nested?input.leftType:(mode==0?input.builtinType:"integer");
                const std::string expectedValue=mode==0?input.builtinValue:(nested?"7":"-7");
                check(!reject && pureBind && !actual.isNull && actual.typeName==expectedType &&
                    actual.value==expectedValue && sameArguments && inputCalls==input.inputCount && modCalls==(mode==0?0u:1u),
                    label+" stage="+stage+" type="+actual.typeName+" value="+actual.value+
                    " inputCalls="+std::to_string(inputCalls)+" modCalls="+std::to_string(modCalls));
            } catch(const std::exception& error) {
                const auto state=stateOf(error);
                check(reject && stage=="bind" && state=="42883" && inputCalls==0 && modCalls==0,
                    label+" stage="+stage+" state="+state+" inputCalls="+std::to_string(inputCalls)+
                    " modCalls="+std::to_string(modCalls)+" error="+error.what());
            }
        }
    // Standalone metadata without an evaluator must not assume missing
    // callee/column declarations are a reliable numeric input shape.
    for (const auto& context : std::vector<std::string>{"", database})
        for (const bool borrow : {false, true})
            for (const auto* sql : {"mod(9::integer,opaque_input()-1)", "mod(9::integer,v-1)"}) {
                ExprEvaluator evaluator; evaluator.setCurrentDB(context);
                size_t calls = 0;
                evaluator.registerFunction("opaque_input", [&](const std::vector<ExprValue>&) {
                    ++calls; return ExprValue("integer", "2");
                });
                SQLParser parser; auto parsed = parser.parseForBinding(std::string("SELECT ") + sql);
                auto* select = parsed.success ? dynamic_cast<SelectStmt*>(parsed.stmt.get()) : nullptr;
                if (!select || select->selectList.size() != 1) return 2;
                try {
                    const auto type = ExprHelper::inferParsedInputType(select->selectList[0].expr.get(),
                        {}, context, &g_engine, borrow ? &evaluator : nullptr);
                    check(type == "integer" && calls == 0, "standalone unknown input metadata");
                } catch (const std::exception& error) {
                    check(false, "standalone metadata " + std::string(error.what()));
                }
            }
    session.currentDB=database;
    check(checks==152,"complete 144 callback controls plus8 standalone metadata");
    std::cout<<"MOD_COMPLEX_INPUT_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb(name);return failures?1:0;
}
