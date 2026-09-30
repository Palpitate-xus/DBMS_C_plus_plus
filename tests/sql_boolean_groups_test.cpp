#include "common/SqlConjunction.h"
#include <cassert>
#include <iostream>

int main() {
    using Groups = std::vector<std::vector<std::string>>;
    using dbms::sqlBooleanGroups;
    assert(sqlBooleanGroups("id=1 OR id=2 AND v=7") ==
        (Groups{{"id=1"}, {"id=2", "v=7"}}));
    assert(sqlBooleanGroups("(id=1 OR id=2) AND (v=7 OR v=8)") ==
        (Groups{{"id=1", "v=7"}, {"id=1", "v=8"}, {"id=2", "v=7"}, {"id=2", "v=8"}}));
    assert(sqlBooleanGroups("id BETWEEN 1 AND 2 OR (s='and or' AND v=7)") ==
        (Groups{{"id BETWEEN 1 AND 2"}, {"s='and or'", "v=7"}}));
    assert(sqlBooleanGroups("(id NOT BETWEEN 1 AND 2 AND v IN (7,8)) OR v IS NULL") ==
        (Groups{{"id NOT BETWEEN 1 AND 2", "v IN (7,8)"}, {"v IS NULL"}}));
    assert(sqlBooleanGroups("f('or', (1+2))=3 AND \"or\"='it''s and or'") ==
        (Groups{{"f('or', (1+2))=3", "\"or\"='it''s and or'"}}));
    for (const std::string invalid : {"", "a=1 AND", "OR a=1", "(a=1", "a=1)", "s='x"}) {
        bool rejected = false;
        try { sqlBooleanGroups(invalid); }
        catch (const std::invalid_argument&) { rejected = true; }
        assert(rejected);
    }
    bool bounded = false;
    try { sqlBooleanGroups("(a=1 OR a=2) AND (b=1 OR b=2)", 3); }
    catch (const std::length_error&) { bounded = true; }
    assert(bounded);
    bounded = false;
    try { sqlBooleanGroups("(a=1 OR a=2) AND (b=1 OR b=2)", 8, 7); }
    catch (const std::length_error&) { bounded = true; }
    assert(bounded);
    bounded = false;
    try { sqlBooleanGroups(std::string(130, '(') + "a=1" + std::string(130, ')')); }
    catch (const std::length_error&) { bounded = true; }
    assert(bounded);
    std::cout << "[SQL BOOLEAN GROUPS] passed\n";
}
