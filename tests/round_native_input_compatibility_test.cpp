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

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string name="round_native_input_compatibility",database=testDbPath(name);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    size_t checks=0,failures=0;
    const auto check=[&](bool good,const std::string& label) {
        ++checks;failures+=!good;
        if(!good)std::cerr<<"ROUND_NATIVE_COMPATIBILITY_FAIL "<<label<<'\n';
    };
    struct Input {const char* sql;const char* type;const char* value;bool zero;bool scaled;};
    const std::vector<Input> inputs={
        {"round('1.25'::text)","text","1.25",false,false},
        {"round(1.25::numeric,1::integer)","numeric","1.25",false,true},
        {"round(1::integer,1::integer)","integer","1",false,true},
        {"round()","unknown","",true,false}};
    for(const auto& context:std::vector<std::string>{"",database})
        for(int mode=0;mode<3;++mode)for(const auto& input:inputs)for(bool nested:{false,true}) {
            session.currentDB=context;
            ExprEvaluator evaluator;evaluator.setCurrentDB(context);
            size_t calls=0;bool sameArguments=true;
            const auto replacement=[&](const std::vector<ExprValue>& args) {
                ++calls;
                const size_t expected=input.zero?0:input.scaled?2:1;
                sameArguments=sameArguments && args.size()==expected;
                if(!input.zero && args.size()==expected) {
                    sameArguments=sameArguments && !args[0].isNull &&
                        args[0].typeName==input.type && args[0].value==input.value;
                    if(input.scaled)sameArguments=sameArguments && !args[1].isNull &&
                        args[1].typeName=="integer" && args[1].value=="1";
                }
                return ExprValue("integer","-7");
            };
            if(mode==1)evaluator.registerFunction("round",replacement);
            if(mode==2)evaluator.registerFunction("round",replacement,'i');
            const std::string sql=nested?std::string("abs(")+input.sql+')':input.sql;
            const std::string label=std::string(context.empty()?"emptyDB ":"realDB ")+
                "mode="+std::to_string(mode)+' '+sql;
            SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+sql);
            auto* select=parsed.success?dynamic_cast<SelectStmt*>(parsed.stmt.get()):nullptr;
            auto* root=select?dynamic_cast<FunctionCallExpr*>(select->selectList.front().expr.get()):nullptr;
            auto* round=root && nested?dynamic_cast<FunctionCallExpr*>(root->args.front().get()):root;
            if(!round)return 2;
            try {
                const auto metadata=ExprHelper::inferParsedInputType(round,{},context,&g_engine,&evaluator);
                check(metadata==input.type && calls==0,label+" metadata="+metadata);
            } catch(const std::exception& error) {check(false,label+" metadata "+error.what());}
            std::string phase="binding";
            const bool rejected=nested && std::string(input.type)=="text";
            try {
                evaluator.bindScalarFunctions(root,&g_engine);
                const size_t bindCalls=calls;phase="runtime";
                const auto actual=evaluator.eval(root,RowContext{});
                const bool nullResult=mode==0 && input.zero;
                const std::string expectedType=nested
                    ?(input.zero?"double precision":input.type)
                    :(mode?"integer":std::string(input.type)=="numeric"?"numeric":"double precision");
                const std::string expectedValue=mode?(nested?"7":"-7"):
                    (std::string(input.type)=="numeric"?"1.3":"1");
                check(!rejected && bindCalls==0 && calls==(mode?1u:0u) && sameArguments &&
                    actual.typeName==expectedType && actual.isNull==nullResult &&
                    (nullResult || actual.value==expectedValue),label+" runtime type="+
                    actual.typeName+" value="+actual.value);
            } catch(const DbError& error) {
                check(rejected && phase=="binding" && error.sqlState()=="42883" && calls==0,
                    label+" error="+error.sqlState()+" phase="+phase);
            } catch(const std::exception& error) {check(false,label+' '+error.what());}
        }
    session.currentDB=database;
    check(checks==96,"complete 96 metadata/binding/runtime controls");
    std::cout<<"ROUND_NATIVE_COMPATIBILITY_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    return failures?1:0;
}
