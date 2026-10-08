#include "parser/parser.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "catalog/declared_type.h"
#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    SQLParser parser;
    ExprEvaluator evaluator;
    const std::vector<std::pair<std::string,std::string>> inputs={
        {"",""},{"abc","a"},{"é","\\303"},{"中","\\344"},
        {"\\000",""},{"\\101","A"},{"\\200","\\200"},{"\\777","\\377"}
    };
    const std::vector<std::string> declarations={"\"char\"","pg_catalog.\"char\""};
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role) {
        ++checked;failed+=!pass;std::cout<<"INTERNAL_CHAR "<<role<<" pass="<<pass<<'\n';
    };
    const auto checkExpression=[&](const std::string& expression,const std::string& expected,bool null,const std::string& state) {
        for(bool binding:{false,true}) {
            bool pass=false;
            try {
                const auto parsed=binding?parser.parseForBinding("SELECT "+expression):parser.parse("SELECT "+expression);
                const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
                if(!select)pass=!state.empty() && parsed.sqlState==state;
                else {
                    const auto value=evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});
                    pass=state.empty() && value.isNull==null && value.value==expected &&
                        ExprHelper::canonicalResultTypeName(value.typeName)=="\"char\"" &&
                        mapBuiltinTypeNameToOid(ExprHelper::canonicalResultTypeName(value.typeName))==18;
                }
            }catch(const DbError& error){pass=!state.empty() && error.sqlState()==state;}
            require(pass,(binding?"binding ":"ordinary ")+expression);
        }
        const auto value=ExprHelper::evalString(expression,{},{});
        require(state.empty()?value.ok && value.isNull==null && value.value==expected &&
            ExprHelper::canonicalResultTypeName(value.typeName)=="\"char\"":
            !value.ok && value.sqlState==state,"stored-expression "+expression);
    };
    for(const auto& declaration:declarations) {
        for(const auto& input:inputs) {
            for(const auto& expression:{declaration+" '"+input.first+"'",
                "CAST('"+input.first+"' AS "+declaration+")","'"+input.first+"'::"+declaration})
                checkExpression(expression,input.second,false,{});
        }
        for(const auto& expression:{"CAST(NULL AS "+declaration+")","NULL::"+declaration})
            checkExpression(expression,{},true,{});
        for(const auto& expression:{declaration+"(2) 'abc'","CAST('abc' AS "+declaration+"(2))","'abc'::"+declaration+"(2)"})
            checkExpression(expression,{},false,"42601");
    }
    const auto database=testDbPath("internal_char_declared_type");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    (void)g_engine.catalogService().get(database);
    const auto catalog=g_engine.catalogService().metadataSnapshot(database);
    for(const auto& declaration:declarations) {
        const auto binding=resolveDeclaredTypeName(declaration,&catalog,nullptr);
        require(binding.typeOid==18 && binding.typeName=="\"char\"" && binding.inputType=="\"char\"",
            "actual copied catalog identity "+declaration);
        for(const auto& expression:{declaration+" 'abc'","CAST(NULL AS "+declaration+")"}) {
            const auto query=g_engine.prepareBoundQuery(database,"SELECT "+expression+" AS value WHERE false");
            require(query.output.size()==1 && query.output[0].typeOid==18 && query.output[0].type=="\"char\"",
                "empty-source catalog descriptor "+expression);
        }
    }
    std::cout<<"INTERNAL_CHAR_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==181 && !failed?0:1;
}
