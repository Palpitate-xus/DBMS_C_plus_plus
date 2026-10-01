#include "parser/parser.h"

#include <cassert>
#include <iostream>
#include <string>
#include <tuple>
#include <vector>

int main() {
    dbms::SQLParser parser;
    const std::vector<std::tuple<std::string, bool, bool, bool>> cases = {
        {"", false, false, false},
        {"READ ONLY", false, true, false},
        {"READ WRITE", false, true, false},
        {"ISOLATION LEVEL READ UNCOMMITTED", true, false, false},
        {"ISOLATION LEVEL READ COMMITTED", true, false, false},
        {"ISOLATION LEVEL REPEATABLE READ", true, false, false},
        {"ISOLATION LEVEL SERIALIZABLE READ ONLY", true, true, false},
        {"NOT DEFERRABLE", false, false, true},
        {"ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE", true, true, true}
    };
    for (const std::string prefix : {"BEGIN", "BEGIN WORK", "START TRANSACTION"}) {
        for (const auto& [options, isolation, readMode, deferrable] : cases) {
            auto parsed = parser.parse(prefix + " " + options + ";");
            assert(parsed.success);
            const auto* original = dynamic_cast<const dbms::TransactionStmt*>(parsed.stmt.get());
            assert(original && original->isolationSpecified == isolation &&
                   original->readOnlySpecified == readMode &&
                   original->deferrableSpecified == deferrable);
            auto roundtrip = parser.parse(original->toString());
            assert(roundtrip.success);
            const auto* copy = dynamic_cast<const dbms::TransactionStmt*>(roundtrip.stmt.get());
            assert(copy && copy->kind == original->kind &&
                   copy->isolationSpecified == original->isolationSpecified &&
                   copy->readOnlySpecified == original->readOnlySpecified &&
                   copy->deferrableSpecified == original->deferrableSpecified &&
                   copy->isolation == original->isolation &&
                   copy->readOnly == original->readOnly &&
                   copy->deferrable == original->deferrable);
        }
    }
    auto repeatedModes = parser.parse("BEGIN READ ONLY READ WRITE;");
    assert(repeatedModes.success);
    const auto* repeated = dynamic_cast<const dbms::TransactionStmt*>(repeatedModes.stmt.get());
    assert(repeated && repeated->readOnlySpecified && !repeated->readOnly);
    assert(!parser.parse("BEGIN READ COMMITTED;").success);
    assert(!parser.parse("START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ COMMITTED;").success);
    std::cout << "[TRANSACTION BEGIN PRESENCE] passed\n";
}
