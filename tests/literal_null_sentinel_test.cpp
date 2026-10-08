#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    size_t checked=0,failed=0;
    const auto require=[&](const std::string& role,const auto& check) {
        bool good=false;
        try{good=check();}catch(const DbError& error){std::cerr<<role<<" state="<<error.sqlState()<<'\n';}
        ++checked;failed+=!good;std::cout<<"NULL_SENTINEL "<<role<<" pass="<<good<<'\n';
    };
    const auto database=testDbPath("literal_null_sentinel");
    require("create isolated database",[&]{return g_engine.createDatabase(database)==DBStatus::OK;});
    ExprEvaluator evaluator;evaluator.setCurrentDB(database);RowContext context;
    const auto literal=[](std::string value,std::string type) {
        auto expression=std::make_unique<LiteralExpr>();expression->value=std::move(value);expression->typeName=std::move(type);return expression;
    };
    for(const auto& spelling:std::vector<std::pair<std::string,std::string>>{{"null","null"},{"NULL","NULL"},{"NuLl","nUlL"}}) {
        auto expression=literal(spelling.first,spelling.second);
        require("runtime NULL",[&]{const auto value=evaluator.eval(expression.get(),context);return value.isNull && value.typeName=="unknown";});
        require("pure standalone input",[&]{return ExprHelper::inferParsedInputType(expression.get(),{})=="unknown";});
        require("pure cold input",[&]{return ExprHelper::inferParsedInputType(expression.get(),{},database,&g_engine)=="unknown";});
        require("pure cold result",[&]{return ExprHelper::inferParsedResultType(expression.get(),{},database,&g_engine)=="text";});
        require("NULL has no strong or type label",[&]{return ExprHelper::projectionLabel(expression.get())==std::make_pair(std::string(),0);});
        CastExpr cast;cast.typeName="integer";cast.operand=literal(spelling.first,spelling.second);
        require("cast supplies genuine label",[&]{return ExprHelper::projectionLabel(&cast)==std::make_pair(std::string("int4"),1);});
        require("cast supplies genuine input type",[&]{return ExprHelper::inferParsedInputType(&cast,{},database,&g_engine)=="integer";});
        for(const auto& function:{"array_append","array_prepend","array_cat"}) {
            FunctionCallExpr call;call.funcName=function;
            auto null=literal(spelling.first,spelling.second);
            auto integer=literal("9","integer");
            auto array=literal("{1}","integer[]");
            if(call.funcName=="array_prepend"){call.args.push_back(std::move(integer));call.args.push_back(std::move(null));}
            else {call.args.push_back(std::move(null));call.args.push_back(call.funcName=="array_cat"?std::move(array):std::move(integer));}
            const auto expected=call.funcName=="array_cat"?"{1}":"{9}";
            require(std::string(function)+" with legacy NULL",[&]{const auto value=evaluator.eval(&call,context);return !value.isNull && value.value==expected;});
        }
    }
    auto text=literal("'null'","text");
    require("quoted text is not NULL",[&]{const auto value=evaluator.eval(text.get(),context);return !value.isNull && value.value=="null";});
    require("quoted text retains text input",[&]{return ExprHelper::inferParsedInputType(text.get(),{},database,&g_engine)=="text";});
    require("quoted text retains type label",[&]{return ExprHelper::projectionLabel(text.get())==std::make_pair(std::string("text"),1);});
    auto invalid=literal("'null'","null");
    require("invalid declaration not hidden",[&]{try{(void)ExprHelper::inferParsedInputType(invalid.get(),{},database,&g_engine);}catch(const DbError& error){return error.sqlState()=="42601";}return false;});
    auto typedNull=literal("NULL","integer");
    require("real typed annotation retains integer metadata",[&]{return ExprHelper::inferParsedInputType(typedNull.get(),{},database,&g_engine)=="integer";});
    std::cout<<"NULL_SENTINEL_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==36 && !failed?0:1;
}


