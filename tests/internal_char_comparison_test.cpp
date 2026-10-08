#include "parser/parser.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "catalog/CatalogService.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <iomanip>
#include <iostream>
#include <sstream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    SQLParser parser;ExprEvaluator evaluator;
    const std::vector<int> bytes={0,1,65,127,128,255};
    const std::vector<std::string> operations={"=","<>","<",">","<=",">="};
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role){++checked;failed+=!pass;std::cout<<"CHAR_COMPARISON "<<role<<" pass="<<pass<<'\n';};
    const auto literal=[](int byte){std::ostringstream text;text<<"\"char\" '\\"<<std::oct<<std::setw(3)<<std::setfill('0')<<byte<<"'";return text.str();};
    const auto datum=[&](int byte){const auto parsed=parser.parse("SELECT "+literal(byte));const auto* select=dynamic_cast<const SelectStmt*>(parsed.stmt.get());return evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});};
    for(int a:bytes)for(int b:bytes)for(const auto& operation:operations) {
        const bool truth=operation=="="?a==b:operation=="<>"?a!=b:operation=="<"?a<b:
            operation==">"?a>b:operation=="<="?a<=b:a>=b;
        const std::string expected=truth?"t":"f";
        const auto expression=literal(a)+" "+operation+" "+literal(b);
        for(bool binding:{false,true}) {
            bool pass=false;
            try {
                const auto parsed=binding?parser.parseForBinding("SELECT "+expression):parser.parse("SELECT "+expression);
                const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
                if(select){const auto value=evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});pass=!value.isNull && value.typeName=="boolean" && value.value==expected;}
            }catch(const DbError&){ }
            require(pass,(binding?"binding ":"ordinary ")+expression);
        }
        const auto value=ExprHelper::evalString(expression,{},{});
        require(value.ok && !value.isNull && value.typeName=="boolean" && value.value==expected,"stored-expression "+expression);
        bool prepared=false;
        try {
            const auto binding=ExprEvaluator::resolveComparison(operation,"\"char\"","\"char\"");
            const auto result=evaluator.comparePrepared(binding,datum(a),datum(b));
            prepared=!result.isNull && result.typeName=="boolean" && result.value==expected;
        }catch(const DbError&){ }
        require(prepared,"typed prepared "+expression);
    }
    bool hash=false;
    try {
        const auto binding=ExprEvaluator::resolveComparison("=","\"char\"","\"char\"");
        const auto a=evaluator.coerceComparison(binding,ExprValue("\"char\"","A"),true);
        const auto b=evaluator.coerceComparison(binding,ExprValue("\"char\"","\\101"),false);
        const auto c=evaluator.coerceComparison(binding,datum(128),false);
        hash=binding.hashable && ExprEvaluator::comparisonHashKey(binding,a,true)==ExprEvaluator::comparisonHashKey(binding,b,false) &&
            ExprEvaluator::comparisonHashKey(binding,a,true)!=ExprEvaluator::comparisonHashKey(binding,c,false);
    }catch(const DbError&){ }
    require(hash,"byte hash agrees with equality and is not a collation key");
    const auto database=testDbPath("internal_char_comparison");
    require(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    (void)g_engine.catalogService().get(database);
    for(const auto& operation:operations) {
        bool pass=false;
        try {
            const auto query=g_engine.prepareBoundQuery(database,"SELECT "+literal(128)+" "+operation+" "+literal(127)+" AS value WHERE false");
            pass=query.output.size()==1 && query.output[0].type=="boolean" && query.output[0].typeOid==16;
            if(!pass && !query.output.empty())std::cout<<"actual descriptor "<<query.output[0].type<<" OID="<<query.output[0].typeOid<<'\n';
        }catch(const DbError& error){std::cout<<"actual operator error "<<error.sqlState()<<" "<<error.what()<<'\n';}
        require(pass,"pure empty-source operator binding "+operation);
    }
    std::cout<<"CHAR_COMPARISON_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==872 && !failed?0:1;
}
