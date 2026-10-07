#include "catalog/systables.h"
#include "expression/expr_helper.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    const struct { const char* name; Oid oid; } aliases[] = {
        {"bool[]", 1000}, {"boolean[]", 1000},
        {"integer[]", 1007}, {"text[]", 1009}, {"bigint[]", 1016}
    };
    for (const auto& alias : aliases) {
        assert(mapBuiltinTypeNameToOid(alias.name) == alias.oid);
        assert(isBuiltinTypeOid(alias.oid));
    }
    assert(mapBuiltinTypeNameToOid("bool") == 16);
    assert(mapBuiltinTypeNameToOid("boolean") == 16);
    assert(ExprHelper::canonicalResultTypeName("BOOL[][]") == "boolean[]");
    assert(mapBuiltinTypeNameToOid(ExprHelper::canonicalResultTypeName("BOOL[][]")) == 1000);
    assert(mapBuiltinTypeNameToOid("not_a_builtin_array[]") == INVALID_OID);
    std::cout << "[PHYSICAL ARRAY BUILTIN OID] passed\n";
}
