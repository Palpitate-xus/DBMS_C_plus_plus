#include "parser/query_binding.h"
#include "parser/parser.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <iostream>
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    struct Case {std::string sql,type,value;};
    const std::vector<Case> cases={
        {"CASE 'x' WHEN 'x' THEN 1 ELSE 2 END","integer","1"},
        {"CASE 'x' WHEN NULL THEN 1 ELSE 2 END","integer","2"},
        {"CASE NULL WHEN NULL THEN 1 ELSE 2 END","integer","2"},
        {"CASE '01'::varbit WHEN B'01' THEN VARBIT 'b01' ELSE NULL END","bit varying","01"},
        {"CASE pg_catalog.varbit 'b01' WHEN B'01' THEN 1 ELSE 2 END","integer","1"},
        {"CASE true WHEN true THEN VARBIT 'b01' ELSE NULL END","bit varying","01"},
        {"CASE 'x' WHEN 'x' THEN CASE 'y' WHEN 'y' THEN 1 ELSE 2 END ELSE 3 END","integer","1"},
        {"CAST(CASE '12' WHEN '12' THEN INTEGER '12' ELSE INTEGER '0' END AS integer)","integer","12"}
    };
    size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role) {
        ++checked;failed+=!pass;std::cout<<"TYPE_CONSTANT_CASE "<<role<<" pass="<<pass<<'\n';
    };
    SQLParser parser;ExprEvaluator evaluator;
    for(const auto& item:cases)for(bool binding:{false,true}) {
        bool pass=false;
        try {
            auto parsed=binding?parser.parseForBinding("SELECT "+item.sql):parser.parse("SELECT "+item.sql);
            const auto* select=parsed.success?dynamic_cast<const SelectStmt*>(parsed.stmt.get()):nullptr;
            if(select && select->selectList.size()==1) {
                const auto value=evaluator.eval(select->selectList[0].expr.get(),RowContext{});
                pass=!value.isNull && value.typeName==item.type && value.value==item.value;
            }
        } catch(const DbError& error){std::cerr<<error.sqlState()<<' '<<error.what()<<'\n';}
        require(pass,(binding?"binding ":"ordinary ")+item.sql);
    }
    std::cout<<"TYPE_CONSTANT_CASE_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==16 && !failed?0:1;
}
