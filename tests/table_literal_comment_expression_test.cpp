#include "expression/expr_helper.h"
#include <cassert>
#include <iostream>
#include <string>
#include <utility>

int main() {
    for (const auto& entry : {std::pair<std::string, std::string>{"'; /* literal */'", "; /* literal */"},
             {"E'escaped; -- text'", "escaped; -- text"},
             {"$body$dollar; /* text */$body$", "dollar; /* text */"},
             {"$BoDy$MiXeD  Text$BoDy$", "MiXeD  Text"},
             {"$body$Line1\nLine2\tTab$body$", "Line1\nLine2\tTab"},
             {"E'MiXeD\\'  Text\\tEnd'", "MiXeD'  Text\tEnd"}}) {
        const auto& expression = entry.first;
        const auto value = dbms::ExprHelper::evalString(expression, {});
        assert(value.ok && !value.isNull && value.value == entry.second);
    }
    std::cout << "[TABLE LITERAL COMMENT EXPRESSION] passed\n";
}
