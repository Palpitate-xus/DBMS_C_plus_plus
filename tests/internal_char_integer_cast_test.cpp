#include "parser/parser.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    SQLParser parser;ExprEvaluator evaluator;
    struct Case {std::string expression,value,type,state;};
    const std::vector<Case> cases={
        {"CAST(65 AS \"char\")","A","\"char\"",{}},
        {"CAST(0 AS \"char\")","","\"char\"",{}},
        {"CAST(-1 AS \"char\")","\\377","\"char\"",{}},
        {"CAST(-128 AS \"char\")","\\200","\"char\"",{}},
        {"CAST(127 AS \"char\")",std::string(1,'\x7f'),"\"char\"",{}},
        {"CAST(128 AS \"char\")",{}, {},"22003"},
        {"CAST(-129 AS \"char\")",{}, {},"22003"},
        {"CAST(\"char\" 'A' AS INTEGER)","65","integer",{}},
        {"\"char\" '\\377'::int4","-1","integer",{}},
        {"CAST(\"char\" '\\200' AS INTEGER)","-128","integer",{}},
        {"CAST(\"char\" '' AS INTEGER)","0","integer",{}},
        {"\"char\" '\\177'::integer","127","integer",{}},
        {"CAST('abc'::text AS \"char\")","a","\"char\"",{}},
        {"CAST(CAST(65 AS \"char\") AS text)","A","text",{}},
        {"CAST(65::bigint AS \"char\")",{}, {},"42846"},
        {"CAST(65::smallint AS \"char\")",{}, {},"42846"},
        {"CAST(true AS \"char\")",{}, {},"42846"},
        {"CAST(1.2 AS \"char\")",{}, {},"42846"},
        {"CAST(NULL::bigint AS \"char\")",{}, {},"42846"},
        {"CAST(NULL::smallint AS \"char\")",{}, {},"42846"},
        {"CAST(NULL::boolean AS \"char\")",{}, {},"42846"},
        {"CAST(NULL::numeric AS \"char\")",{}, {},"42846"}
    };
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role){++checked;failed+=!pass;std::cout<<"CHAR_INTEGER_CAST "<<role<<" pass="<<pass<<'\n';};
    for(const auto& item:cases) {
        for(bool binding:{false,true}) {
            bool pass=false;
            try {
                const auto parsed=binding?parser.parseForBinding("SELECT "+item.expression):parser.parse("SELECT "+item.expression);
                const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
                if(select) {
                    const auto value=evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});
                    pass=item.state.empty() && !value.isNull && value.value==item.value &&
                        ExprHelper::canonicalResultTypeName(value.typeName)==item.type;
                } else pass=!item.state.empty() && parsed.sqlState==item.state;
            }catch(const DbError& error){pass=!item.state.empty() && error.sqlState()==item.state;}
            require(pass,(binding?"binding ":"ordinary ")+item.expression);
        }
        const auto value=ExprHelper::evalString(item.expression,{},{});
        require(item.state.empty()?value.ok && !value.isNull && value.value==item.value &&
            ExprHelper::canonicalResultTypeName(value.typeName)==item.type:
            !value.ok && value.sqlState==item.state,"stored-expression "+item.expression);
    }
    const auto database=testDbPath("internal_char_integer_cast");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    (void)g_engine.catalogService().get(database);
    for(const auto& expression:{"CAST(65::bigint AS \"char\")","CAST(65::smallint AS \"char\")",
        "CAST(true AS \"char\")","CAST(1.2 AS \"char\")"}) {
        bool pass=false;
        try{(void)g_engine.prepareBoundQuery(database,std::string("SELECT ")+expression+" AS value WHERE false");}
        catch(const DbError& error){pass=error.sqlState()=="42846";}
        require(pass,std::string("pure empty-source cast eligibility ")+expression);
    }
    std::cout<<"CHAR_INTEGER_CAST_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==71 && !failed?0:1;
}
