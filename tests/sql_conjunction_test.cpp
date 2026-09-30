#include "common/SqlConjunction.h"
#include <cassert>
#include <iostream>

int main() {
    using dbms::splitSqlConjunction;
    assert(splitSqlConjunction("id between 1 and 2 and v=7") ==
        (std::vector<std::string>{"id between 1 and 2", "v=7"}));
    assert(splitSqlConjunction("v=7 AND id NOT BETWEEN 1 AND 2 AND w BETWEEN 3 AND 4") ==
        (std::vector<std::string>{"v=7", "id NOT BETWEEN 1 AND 2", "w BETWEEN 3 AND 4"}));
    assert(splitSqlConjunction("s='between and ''and''' and \"and\"='x and y'") ==
        (std::vector<std::string>{"s='between and ''and'''", "\"and\"='x and y'"}));
    assert(splitSqlConjunction("(a=1 and b=2) and f('and')=3") ==
        (std::vector<std::string>{"(a=1 and b=2)", "f('and')=3"}));
    assert(splitSqlConjunction("brand=1 and between_value=2") ==
        (std::vector<std::string>{"brand=1", "between_value=2"}));
    assert(splitSqlConjunction("id BETWEEN (1 + 2) AND (3 + 4) AND v=7") ==
        (std::vector<std::string>{"id BETWEEN (1 + 2) AND (3 + 4)", "v=7"}));
    std::cout << "[SQL CONJUNCTION] passed\n";
}
