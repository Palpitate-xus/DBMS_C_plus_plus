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
    struct Case {std::string expression,value,state;};
    const std::vector<Case> cases={
        {"\"char\" 'A' || \"char\" 'B'",{},"42725"},
        {"\"char\" 'A' || TEXT 'B'",{},"42725"},
        {"TEXT 'A' || \"char\" 'B'",{},"42725"},
        {"\"char\" 'A' || 'B'",{},"42725"},
        {"CAST(NULL AS \"char\") || \"char\" 'B'",{},"42725"},
        {"\"char\" 'A' || CAST(NULL AS \"char\")",{},"42725"},
        // The protocol counterpart additionally retains WHERE false.
        {"pg_catalog.\"char\" 'A' || pg_catalog.\"char\" 'B'",{},"42725"},
        {"CAST(NULL AS text) || \"char\" 'B'",{},"42725"},
        {"\"char\" 'A' || CAST(NULL AS text)",{},"42725"},
        {"\"char\" 'A' || VARCHAR 'B'",{},"42725"},
        {"VARCHAR 'A' || \"char\" 'B'",{},"42725"},
        {"\"char\" 'A' || CHAR 'B'",{},"42725"},
        {"CHAR 'A' || \"char\" 'B'",{},"42725"},
        {"\"char\" 'A' || 1","A1",{}},
        {"1 || \"char\" 'B'","1B",{}},
        {"\"char\" 'A' || true","Atrue",{}},
        {"true || \"char\" 'B'","trueB",{}}
    };
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role){++checked;failed+=!pass;std::cout<<"CHAR_CONCAT "<<role<<" pass="<<pass<<'\n';};
    for(const auto& item:cases) {
        for(bool binding:{false,true}) {
            bool pass=false;
            try {
                const auto parsed=binding?parser.parseForBinding("SELECT "+item.expression):parser.parse("SELECT "+item.expression);
                const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
                if(select){const auto value=evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});pass=item.state.empty() && !value.isNull && value.typeName=="text" && value.value==item.value;}
                else pass=!item.state.empty() && parsed.sqlState==item.state;
            }catch(const DbError& error){pass=!item.state.empty() && error.sqlState()==item.state;}
            require(pass,(binding?"binding ":"ordinary ")+item.expression);
        }
        const auto value=ExprHelper::evalString(item.expression,{},{});
        require(item.state.empty()?value.ok && !value.isNull && value.typeName=="text" && value.value==item.value:
            !value.ok && value.sqlState==item.state,"stored-expression "+item.expression);
    }
    require(!ExprHelper::resolveArrayConcatTypes("\"char\"","text"),"array-function helper has no scalar text overload");
    const auto database=testDbPath("internal_char_concat");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    (void)g_engine.catalogService().get(database);
    for(const auto& item:cases) {
        bool pass=false;
        try {
            const auto query=g_engine.prepareBoundQuery(database,"SELECT "+item.expression+" AS value WHERE false");
            pass=item.state.empty() && query.output.size()==1 && query.output[0].type=="text" && query.output[0].typeOid==25;
        }catch(const DbError& error){pass=!item.state.empty() && error.sqlState()==item.state;}
        require(pass,"pure empty-source concat binding "+item.expression);
    }
    std::cout<<"CHAR_CONCAT_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==70 && !failed?0:1;
}
