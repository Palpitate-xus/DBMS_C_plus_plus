#include "catalog/type_registry.h"
#include "expression/common_type.h"
#include "expression/expr_helper.h"
#include "expression/type_spelling.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"\"public\".\"MixedRank\"","\"public\".\"MixedRank\""},
        {"Public.\"MixedRank\"","public.\"MixedRank\""},
        {"\"Scope.X\".\"Rank(Name)\"","\"Scope.X\".\"Rank(Name)\""},
        {"\"Scope.X\".\"Ra\"\"nk(Name)\"","\"Scope.X\".\"Ra\"\"nk(Name)\""},
        {"\"CaseType\"[]","\"CaseType\"[]"},
        {"\"CaseType\"[][]","\"CaseType\"[]"},
        {"\"CHAR\"","\"CHAR\""},{"\"char\"","\"char\""},
        {"NUMERIC(4,2)","numeric"},{" INT4 ","integer"}}) {
        const auto type=ExprHelper::canonicalResultTypeName(item.first);
        const auto common=common_type_detail::canonical(item.first);
        assert(type==item.second && common==item.second);
    }
    assert(type_spelling_detail::scalarIdentity("\"T(4)\"(7)")=="\"T(4)\"");
    assert(ExprHelper::canonicalResultTypeName("\"MixedRank\"")!=ExprHelper::canonicalResultTypeName("\"mixedrank\""));
    assert(selectCommonType({"\"public\".\"MixedRank\"","unknown"},"CASE")=="\"public\".\"MixedRank\"");
    std::cout<<"[QUOTED TYPE SPELLING] case, escaped quotes, punctuation, arrays, aliases and named common types passed\n";
}
