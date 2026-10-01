#include "commands/TableManage.h"
#include "catalog/type_registry.h"

#include <cassert>
#include <iostream>

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    for (const auto& value : {"canonical-zero", "12345678901234567890-alpha", "a-b"}) {
        const auto conditions = dbms::StorageEngine::parseConditions(
            {std::string("=code ") + value});
        assert(conditions.size() == 1);
        assert(conditions.front().op == "=");
        assert(conditions.front().colName == "code");
        assert(conditions.front().value == value);
    }
    for (const auto& value : {"1+1", "CAST(2 AS integer)", "2::numeric"}) {
        const auto conditions = dbms::StorageEngine::parseConditions(
            {std::string("=id ") + value});
        assert(conditions.size() == 1 && conditions.front().op == "typedexpr");
    }
    const auto arithmetic = dbms::StorageEngine::parseConditions({">id/0.5 1"});
    assert(arithmetic.size() == 1 && arithmetic.front().op == "typedexpr");
    std::cout << "[COMPACT LITERAL HYPHEN] passed" << std::endl;
}
