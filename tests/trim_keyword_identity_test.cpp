#include "Session.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms;
struct Case { std::string expression; std::optional<std::string> value; std::string type = "text"; };
struct Sample { std::string source, characters, both, leading, trailing; };
const std::vector<Sample> samples = {
    {"both","b","oth","oth","both"}, {"leading","l","eading","eading","leading"},
    {"trailing","t","railing","railing","trailing"}, {"xxbothxx","x","both","bothxx","xxboth"},
    {"","x","","",""}, {"ééxé","é","x","xé","ééx"},
    {"😀界a界😀","😀界","a","a界😀","😀界a"}, {"éxé","é","x","xé","éx"},
    {" x ",""," x "," x "," x "}, {"NULL","N","ULL","ULL","NULL"}
};
const std::vector<std::string> directions = {"", "both ", "leading ", "trailing "};
const std::vector<std::string> malformed = {
    "trim()", "trim(from)", "trim(both)", "trim(leading)", "trim(trailing)",
    "trim('x' from)", "trim(both 'x' from)", "trim(from 'x',)", "trim('x',)",
    "trim('x' from 'y' from 'z')", "trim('x' 'y')", "trim(leading, 'x')",
    "trim(both from from 'x')", "trim(both leading from 'x')", "trim('x' from 'y'",
    "trim(both from 'x' 'y')"
};
void require(bool good,const std::string& label) { if (!good) throw std::runtime_error(label); }
std::string literal(const std::string& value) {
    std::string result = "'";
    for (char c : value) { result += c; if (c == '\'') result += c; }
    return result + "'";
}
std::vector<std::string> forms(const std::string& direction,const std::string& source,const std::string& characters) {
    return {"trim("+direction+characters+" from "+source+")", "trim("+direction+source+","+characters+")",
            "trim("+direction+"from "+source+","+characters+")"};
}
std::string spaces(std::string value,const std::string& direction) {
    if (direction != "trailing ") { const auto first = value.find_first_not_of(' '); value.erase(0, first == std::string::npos ? value.size() : first); }
    if (direction != "leading ") { const auto last = value.find_last_not_of(' '); value.erase(last == std::string::npos ? 0 : last+1); }
    return value;
}
std::vector<Case> cases() {
    std::vector<Case> result;
    for (const auto& item : samples) for (const auto& direction : directions) {
        const auto expected = direction == "leading " ? item.leading : direction == "trailing " ? item.trailing : item.both;
        for (const auto& sql : forms(direction,literal(item.source),literal(item.characters))) result.push_back({sql,expected});
    }
    for (const auto& source : std::vector<std::string>{"","both","leading","trailing"," both ","  x","x  ","\t x \t"})
        for (const auto& direction : directions) for (const auto& separator : {"", "from "})
            result.push_back({"trim("+direction+separator+literal(source)+")",spaces(source,direction)});
    for (const auto& pair : std::vector<std::pair<std::string,std::string>>{{"NULL::text","'x'"},{"'xx'","NULL::text"},{"NULL::text","NULL::text"}})
        for (const auto& direction : directions) for (const auto& sql : forms(direction,pair.first,pair.second)) result.push_back({sql,std::nullopt});
    for (const auto& item : std::vector<Case>{
        {"trim('x' from trim(leading 'y' from 'yyxxbothxxyy'))","bothxxyy"},
        {"trim(leading 'x' from trim(trailing 'y' from 'xxbothyy'))","both"},
        {"trim(substring('xx' from 1 for 1) from 'xxbothxx')","both"},
        {"trim('x' from substring('xxbothxx' from 1 for 8))","both"},
        {"trim(' both '::char(10))","both"}, {"trim('x' from 'xxbothxx'::char(10))","both"},
        {"trim(leading from ' x '::char(6))","x"}, {"trim(trailing from ' x '::varchar)"," x"},
        {"trim(NULL)",std::nullopt}, {"trim(leading NULL)",std::nullopt},
        {"trim(trailing from NULL)",std::nullopt}, {"trim(both from NULL)",std::nullopt}
    }) result.push_back(item);
    for (const auto& direction : directions) for (const auto& sql : forms(direction,"'\\x78796178'::bytea","'\\x78'::bytea"))
        result.push_back({sql,direction == "leading " ? "\\x796178" : direction == "trailing " ? "\\x787961" : "\\x7961","bytea"});
    return result;
}
void matches(const ExprValue& actual,const Case& item) {
    require(actual.isNull == !item.value && actual.typeName == item.type,"wrong type or NULL bit: "+actual.typeName);
    if (item.value) require(actual.value == *item.value,"expected "+*item.value+", got "+actual.value);
}
}

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap(); size_t controls=0,failures=0;
    const auto check=[&](const std::string& label,const std::function<void()>& action) {
        ++controls; try { action(); } catch(const std::exception& error) {
            ++failures; std::cerr<<"TRIM_KEYWORD_IDENTITY_FAIL "<<label<<" : "<<error.what()<<'\n';
        }
    };
    const std::string name="trim_keyword_identity",database=testDbPath(name);
    check("private database",[&] { cleanupTestDb(name); require(g_engine.createDatabase(database)==DBStatus::OK,"create database"); });
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore { Session* previous;~Restore(){setCurrentSession(previous);} } restore{previous};
    const auto inventory=cases();
    for (const auto& item : inventory) {
        for (bool bindingParse : {false,true}) for (bool bind : {false,true})
            check(item.expression+(bindingParse?" binding parse":" parse")+(bind?" bound":" raw"),[&] {
                SQLParser parser; auto parsed=bindingParse?parser.parseForBinding("SELECT "+item.expression):parser.parse("SELECT "+item.expression);
                require(parsed.success,parsed.error);
                auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());require(select && select->selectList.size()==1,"missing target");
                auto* expression=select->selectList.front().expr.get();
                auto* call=dynamic_cast<FunctionCallExpr*>(expression);
                require(call && call->schema=="pg_catalog" && (call->funcName=="btrim" || call->funcName=="ltrim" || call->funcName=="rtrim"),"TRIM is not a canonical system call");
                ExprEvaluator evaluator;evaluator.setCurrentDB(database);
                if(bind)evaluator.bindScalarFunctions(expression,&g_engine);
                matches(evaluator.eval(expression,{}),item);
            });
        check(item.expression+" helper",[&] {
            const auto value=ExprHelper::evalString(item.expression,{},{},database,"",&g_engine);
            require(value.ok,value.error);matches(ExprValue(value.typeName,value.value,value.isNull),item);
        });
    }
    for (const auto& expression : malformed) for (bool bindingParse : {false,true}) check("syntax "+expression,[&] {
        SQLParser parser;const auto parsed=bindingParse?parser.parseForBinding("SELECT "+expression):parser.parse("SELECT "+expression);
        require(!parsed.success && parsed.sqlState=="42601","expected structured syntax error");
    });
    for (const auto& direction : std::vector<std::pair<std::string,std::string>>{{"both ","btrim"},{"leading ","ltrim"},{"trailing ","rtrim"}})
        for (bool bind : {false,true}) for (bool declaredVolatility : {false,true}) for (bool withDatabase : {false,true})
            check(direction.second+" opaque callback",[&] {
                auto parsed=SQLParser().parseForBinding("SELECT trim("+direction.first+"'x' from $1)");require(parsed.success,parsed.error);
                auto* select=dynamic_cast<SelectStmt*>(parsed.stmt.get());require(select,"missing select");auto* expression=select->selectList.front().expr.get();
                ExprEvaluator evaluator;if(withDatabase)evaluator.setCurrentDB(database);int calls=0;
                ScalarFunction callback=[&](const std::vector<ExprValue>& args) {
                    ++calls;require(args.size()==2 && args[0].typeName=="character" && args[0].value=="both    " && args[1].value=="x","opaque arguments rewritten or reordered");
                    return ExprValue("text","opaque");
                };
                if(declaredVolatility)evaluator.registerFunction(direction.second,callback,'v');else evaluator.registerFunction(direction.second,callback);
                if(bind)evaluator.bindScalarFunctions(expression,&g_engine);
                require(calls==0,"binding executed callback");
                RowContext row;row.setParameters({ExprValue("character","both    ")});const auto actual=evaluator.eval(expression,row);
                require(calls==1 && actual.typeName=="text" && !actual.isNull && actual.value=="opaque","opaque callback result changed");
            });
    for(const auto& text:std::vector<std::string>{"both","leading","trailing","ordinary"}) check("native trim name remains available",[&] {
        ExprEvaluator evaluator;int calls=0;
        evaluator.registerFunction("trim",[&](const std::vector<ExprValue>& args){++calls;require(args.size()==1 && args.front().value==text,"native arguments");return ExprValue("text","native:"+text);});
        FunctionCallExpr call;call.funcName="trim";auto parameter=std::make_unique<ParameterExpr>();parameter->slot=0;call.args.push_back(std::move(parameter));
        RowContext row;row.setParameters({ExprValue("text",text)});const auto actual=evaluator.eval(&call,row);
        require(calls==1 && actual.value=="native:"+text,"native trim callback bypassed");
    });
    if(inventory.size()!=244 || controls!=1281){++failures;std::cerr<<"TRIM_KEYWORD_IDENTITY_INVENTORY "<<inventory.size()<<' '<<controls<<'\n';}
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    cleanupTestDb(name);
    std::cout<<"TRIM_KEYWORD_IDENTITY_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
