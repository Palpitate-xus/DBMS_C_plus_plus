#include "parser/parser.h"

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

int main() {
    dbms::SQLParser parser;
    const std::vector<std::string> invalid = {
        "READ COMMITTED", "READ UNCOMMITTED", "REPEATABLE READ", "SERIALIZABLE",
        "ISOLATION READ COMMITTED", "ISOLATION SERIALIZABLE", "ISOLATION LEVEL",
        "ISOLATION LEVEL READ", "ISOLATION LEVEL REPEATABLE", ", READ ONLY",
        "READ ONLY,", "READ ONLY,, READ WRITE",
        "ISOLATION LEVEL READ COMMITTED, , READ ONLY"
    };
    for (const std::string prefix : {"BEGIN", "BEGIN WORK", "BEGIN TRANSACTION", "START TRANSACTION"}) {
        for (const auto& options : invalid) {
            assert(!parser.parse(prefix + " " + options + ";").success);
        }
        for (const std::string separator : {" ", ", "}) {
            auto parsed = parser.parse(prefix +
                " ISOLATION LEVEL SERIALIZABLE" + separator +
                "READ ONLY" + separator + "NOT DEFERRABLE;");
            assert(parsed.success);
            const auto* txn = dynamic_cast<const dbms::TransactionStmt*>(parsed.stmt.get());
            assert(txn && txn->isolationSpecified && txn->readOnlySpecified &&
                   txn->deferrableSpecified && txn->isolation == dbms::IsolationLevel::SERIALIZABLE &&
                   txn->readOnly && !txn->deferrable);
        }
        assert(parser.parse(prefix +
            " ISOLATION /* level */ LEVEL READ COMMITTED, /* mode */ READ WRITE;").success);
        auto parsed = parser.parse(prefix +
            " ISOLATION LEVEL READ COMMITTED, ISOLATION LEVEL SERIALIZABLE"
            " READ ONLY, READ WRITE DEFERRABLE, NOT DEFERRABLE;");
        assert(parsed.success);
        const auto* txn = dynamic_cast<const dbms::TransactionStmt*>(parsed.stmt.get());
        assert(txn && txn->isolation == dbms::IsolationLevel::SERIALIZABLE &&
               !txn->readOnly && !txn->deferrable && txn->isolationSpecified &&
               txn->readOnlySpecified && txn->deferrableSpecified);
        auto roundtrip = parser.parse(txn->toString());
        assert(roundtrip.success);
        const auto* copy = dynamic_cast<const dbms::TransactionStmt*>(roundtrip.stmt.get());
        assert(copy && copy->kind == txn->kind && copy->isolation == txn->isolation &&
               copy->readOnly == txn->readOnly && copy->deferrable == txn->deferrable);
    }
    std::cout << "[TRANSACTION BEGIN GRAMMAR] passed\n";
}
