#include "parser/parser.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <iostream>
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();SQLParser parser;ExprEvaluator evaluator;
    const std::vector<std::pair<std::string,std::string>> cases={
        {"INTEGER(2)","42601"},{"int4(2)","42601"},{"TEXT(3)","42601"},
        {"VARCHAR(0)","22023"},{"VARCHAR(-1)","42601"},{"VARCHAR(2,3)","42601"},
        {"pg_catalog.varchar(-1)","22023"},{"pg_catalog.varchar(2,3)","22023"},
        {"\"varchar\"(-1)","22023"},{"\"varchar\"(2,3)","22023"}
    };
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role){++checked;failed+=!pass;std::cout<<"TYPE_MODIFIER_OWNER "<<role<<" pass="<<pass<<'\n';};
    for(const auto& item:cases)for(const auto& expression:{item.first+" '123'","CAST('123' AS "+item.first+")","'123'::"+item.first}) {
        for(bool binding:{false,true}) {
            bool pass=false;
            try {
                const auto parsed=binding?parser.parseForBinding("SELECT "+expression):parser.parse("SELECT "+expression);
                const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
                if(!select)pass=parsed.sqlState==item.second;
                else (void)evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});
            }catch(const DbError& error){pass=error.sqlState()==item.second;}
            require(pass,(binding?"binding ":"ordinary ")+expression);
        }
        const auto value=ExprHelper::evalString(expression,{},{});
        require(!value.ok && value.sqlState==item.second,"stored-expression "+expression);
    }
    std::cout<<"TYPE_MODIFIER_OWNER_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==90 && !failed?0:1;
}
