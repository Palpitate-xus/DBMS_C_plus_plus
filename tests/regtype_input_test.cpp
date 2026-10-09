#include "commands/TableManage.h"
#include "expression/ExprEvaluator.h"
#include "expression/regtype_input.h"
#include "executor/ExecutionPlan.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <filesystem>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    size_t checked=0,failures=0;
    const auto require=[&](bool pass,const std::string& role){++checked;failures+=!pass;std::cout<<"REGTYPE_INPUT "<<role<<" pass="<<pass<<'\n';};
    const auto database=testDbPath("regtype_input");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    ExprEvaluator evaluator;evaluator.setCurrentDB(database);
    const auto evaluate=[&](const std::string& expression){
        SQLParser parser;const auto parsed=parser.parseForBinding("SELECT "+expression);
        if(!parsed.success)throw DbError(parsed.sqlState,parsed.error);
        const auto* select=static_cast<const SelectStmt*>(parsed.stmt.get());
        return evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});
    };
    for(const auto& item:std::vector<std::pair<std::string,Oid>>{{"'integer'",23},{"23",23},{"1007",1007},{"'integer[]'",1007},{"0",0},{"-1",4294967295U},{"'regtype'",2206},{"'regtype[]'",2211}}) {
        const auto value=evaluate("CAST("+item.first+" AS regtype)");
        require(value.objectOid==item.second,"physical identity "+item.first);
    }
    const auto array=evaluate("CAST('[0:2]={integer,NULL,boolean}' AS regtype[])");
    require(array.value=="[0:2]={integer,NULL,boolean}" && array.elementOids==std::vector<std::optional<uint32_t>>{23,std::nullopt,16},"array retains dimensions/NULL/OIDs");
    const auto elements=ExprEvaluator::arrayElements(array);
    require(elements.size()==3 && elements[0].objectOid==23 && elements[1].isNull && elements[2].objectOid==16,"array extraction retains physical identity");
    const auto numericArray=evaluate("CAST(ARRAY[23,-1] AS regtype[])");
    require(numericArray.value=="{integer,4294967295}" && numericArray.elementOids==std::vector<std::optional<uint32_t>>{23,4294967295U},"typed integer array uses integer cast, not text input");
    const auto constructed=evaluate("ARRAY[CAST(23 AS regtype),CAST(16 AS regtype)]");
    require(constructed.elementOids==std::vector<std::optional<uint32_t>>{23,16},"typed constructor preserves actual OIDs");
    require(evaluate("CAST(CAST(-1 AS regtype) AS integer)").value=="-1","unsigned identity back to signed int4");
    require(evaluate("CAST(CAST(-1 AS regtype) AS bigint)").value=="4294967295","unsigned identity to int8");
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"CAST('unknown_type' AS regtype)","42704"},{"CAST('4294967296' AS regtype)","22003"},
        {"CAST(-1::bigint AS regtype)","22003"},{"CAST(NULL::boolean AS regtype)","42846"}}) {
        bool pass=false;try{(void)evaluate(item.first);}catch(const DbError& error){pass=error.sqlState()==item.second;}
        require(pass,"actual input error "+item.first);
    }
    for(const auto& expression:std::vector<std::string>{"CAST('unknown_type' AS regtype)","'unknown_type'::regtype"}) {
        bool pass=false;try{(void)g_engine.prepareBoundQuery(database,"SELECT "+expression+" WHERE false");}
        catch(const DbError& error){pass=error.sqlState()=="42704";}
        require(pass,"unknown constant validated before row demand "+expression);
    }
    require(!g_engine.catalogService().has(database) && !std::filesystem::exists(g_engine.dbPath(database)/"pg_catalog"),"binding/codec do not initialize cold catalog");
    auto catalog=regtype_detail::metadata(g_engine,database);
    PgTypeRow first;first.oid=17000;first.typname="loop_a";first.typnamespace=2200;first.typcategory='A';first.typelem=17001;first.typarray=17001;
    auto second=first;second.oid=17001;second.typname="loop_b";second.typelem=17000;second.typarray=17000;
    catalog.types.push_back(first);catalog.types.push_back(second);
    bool rejectsCycle=false;try{(void)regtype_detail::output(17000,catalog,nullptr);}catch(const DbError& error){rejectsCycle=error.sqlState()=="XX001";}
    require(rejectsCycle,"cyclic metadata fails instead of unbounded recursion");
    std::cout<<"REGTYPE_INPUT_CHECKED="<<checked<<" FAILED="<<failures<<'\n';
    return checked==23 && !failures?0:1;
}
