#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    ExprEvaluator evaluator;
    const std::vector<std::pair<std::string,std::string>> types={
        {"pg_catalog.int2","smallint"},{"\"pg_catalog\".\"int4\"","integer"},
        {"pg_catalog.int8","bigint"},{"pg_catalog.float4","real"},
        {"pg_catalog.float8","double precision"},{"pg_catalog.numeric(4,2)","numeric"},
        {"pg_catalog.bit","bit"},{"\"pg_catalog\".\"varbit\"","bit varying"},
        {"pg_catalog.bpchar","character"},{"pg_catalog.varchar","character varying"},
        {"pg_catalog._int4","integer[]"},{"pg_catalog._text","text[]"}};
    for(const auto& item:types)for(const auto& sql:{
        "SELECT CAST(NULL AS "+item.first+")", "SELECT NULL::"+item.first}) {
        const auto parsed=SQLParser().parseForBinding(sql);
        assert(parsed.success);
        const auto value=evaluator.eval(static_cast<const SelectStmt*>(parsed.stmt.get())->selectList[0].expr.get(),{});
        if(value.typeName!=item.second)std::cerr<<sql<<" actual="<<value.typeName<<" expected="<<item.second<<'\n';
        assert(value.isNull && value.typeName==item.second);
    }
    std::cout<<"[QUALIFIED NULL CAST] actual scalar/array identities, aliases and typmod-free nullable results passed\n";
}
