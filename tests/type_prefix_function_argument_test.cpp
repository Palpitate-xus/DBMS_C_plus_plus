#include "parser/parser.h"
#include "expression/ExprEvaluator.h"
#include "catalog/type_registry.h"
#include <iostream>
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();SQLParser parser;ExprEvaluator evaluator;
    struct Case {std::string expression,value;};
    const std::vector<Case> cases={
        {"lower('A' || 'B')","ab"},{"upper(('a'))","A"},
        {"length('a' || 'b')","2"},{"coalesce(NULL, 'yes')","yes"},
        {"lower(upper('a' || 'b'))","ab"},{"concat('a' || 'b','c')","abc"}
    };
    size_t checked=0,failed=0;
    for(const auto& item:cases)for(bool binding:{false,true}) {
        bool pass=false;
        try {
            auto parsed=binding?parser.parseForBinding("SELECT "+item.expression):parser.parse("SELECT "+item.expression);
            const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
            if(select && select->selectList.size()==1) {
                const auto value=evaluator.eval(select->selectList[0].expr.get(),RowContext{});
                pass=parsed.sqlState.empty() && !value.isNull && value.value==item.value;
            }
        }catch(const std::exception& error){std::cerr<<error.what()<<'\n';}
        ++checked;failed+=!pass;
        std::cout<<"TYPE_PREFIX_ARGUMENT "<<item.expression<<" binding="<<binding<<" pass="<<pass<<'\n';
    }
    std::cout<<"TYPE_PREFIX_ARGUMENT_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==12 && !failed?0:1;
}
