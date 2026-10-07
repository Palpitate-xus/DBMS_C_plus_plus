#include "catalog/collation.h"
#include "catalog/type_registry.h"
#include "expression/expr_helper.h"
#include <cassert>
#include <iostream>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    assert(dbms::collation::isValid("C.utf8"));
    assert(dbms::collation::isBinary("C.utf8"));
    assert(dbms::collation::compare("Z","a","C.utf8")<0);
    assert(dbms::collation::compare("é","z","C.utf8")>0);
    for (const auto& [sql,expected] : {
        std::pair<const char*,const char*>{"'É' COLLATE \"C.utf8\" ILIKE 'é'","t"},
        {"'Z' COLLATE \"C.utf8\" < 'a'","t"},
        {"'é' COLLATE \"C.utf8\" > 'z'","t"},
        {"'é' COLLATE \"C.utf8\" SIMILAR TO '[[:alpha:]]'","t"},
        {"'é' COLLATE \"C\" SIMILAR TO '[[:alpha:]]'","f"}}) {
        const auto value=dbms::ExprHelper::evalString(sql,{});
        assert(value.ok && !value.isNull && value.typeName=="boolean" && value.value==expected);
    }
    std::cout<<"[C.utf8 PATTERN ALIAS] passed\n";
}
