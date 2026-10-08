#include "parser/parser.h"
#include "expression/expr_helper.h"
#include "catalog/type_registry.h"
#include <iostream>
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    SQLParser parser;size_t checked=0,failed=0;
    const auto require=[&](bool pass,const std::string& role) {
        ++checked;failed+=!pass;std::cout<<"DECLARATION_SYNTAX_STATUS "<<role<<" pass="<<pass<<'\n';
    };
    for(const auto& expression:{
        "FLOAT(-1) '1.25'","CAST('1.25' AS FLOAT(-1))","'1.25'::FLOAT(-1)",
        "FLOAT(+1) '1.25'","CAST('1.25' AS FLOAT(+1))","'1.25'::FLOAT(+1)",
        "FLOAT(24,25) '1.25'","CAST('1.25' AS FLOAT(24,25))","'1.25'::FLOAT(24,25)",
        "CAST('1' AS)","'1'::","CAST('1' AS numeric(5,))"}) {
        for(bool binding:{false,true}) {
            const auto sql=std::string("SELECT ")+expression;
            const auto invalid=binding?parser.parseForBinding(sql):parser.parse(sql);
            require(!invalid.success && !invalid.stmt && invalid.sqlState=="42601",sql);
            const auto valid=binding?parser.parseForBinding("SELECT 1"):parser.parse("SELECT 1");
            require(valid.success && valid.stmt && valid.sqlState.empty(),"next parse owns a clean diagnostic context");
        }
        const auto invalid=ExprHelper::evalString(expression,{},{});
        require(!invalid.ok && invalid.sqlState=="42601",std::string("stored expression ")+expression);
    }
    std::cout<<"DECLARATION_SYNTAX_STATUS_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return checked==60 && !failed?0:1;
}
