#include "parser/parser.h"
#include <algorithm>
#include <iostream>
#include <vector>

int main() {
    using namespace dbms;
    SQLParser parser;
    size_t checks=0,failures=0;
    for(bool binding:{false,true}) {
        for(const auto& attribute:{"VOLATILE","STABLE","IMMUTABLE","STRICT"}) {
            std::vector<std::string> options={"AS $$SELECT 1$$","LANGUAGE SQL",attribute};
            std::sort(options.begin(),options.end());
            do {
                const auto sql="CREATE FUNCTION order_control() RETURNS BIGINT "+options[0]+" "+options[1]+" "+options[2];
                const auto parsed=binding?parser.parseForBinding(sql):parser.parse(sql);
                const auto* function=dynamic_cast<const CreateFunctionStmt*>(parsed.stmt.get());
                const bool correct=parsed.isValid() && function && function->funcName=="order_control" &&
                    function->language=="sql" && function->body=="SELECT 1" &&
                    (std::string(attribute)!="STRICT" || function->strict) &&
                    (std::string(attribute)!="STABLE" || function->stable) &&
                    (std::string(attribute)!="IMMUTABLE" || function->immutable) &&
                    (std::string(attribute)!="VOLATILE" || function->volatile_);
                ++checks;if(!correct){++failures;std::cerr<<"FUNCTION_ORDER_FAILURE "<<sql<<'\n';}
            } while(std::next_permutation(options.begin(),options.end()));
        }
        for(const auto& sql:{
            "CREATE FUNCTION order_control() RETURNS INT AS $$SELECT 'as language volatile'$$ LANGUAGE SQL STRICT",
            "CREATE FUNCTION order_control() RETURNS INT LANGUAGE SQL CALLED ON NULL INPUT AS $$SELECT 1$$"}) {
            ++checks;const auto parsed=binding?parser.parseForBinding(sql):parser.parse(sql);
            const auto* function=dynamic_cast<const CreateFunctionStmt*>(parsed.stmt.get());
            if(!parsed.isValid() || !function || function->body.empty())++failures;
        }
    }
    std::cout<<"FUNCTION_ORDER_CHECKS "<<checks<<" FAILURES "<<failures<<std::endl;
    return failures?1:0;
}
