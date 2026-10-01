#include "parser/parser.h"

#include <cassert>
#include <iostream>

int main() {
    dbms::SQLParser parser;
    for (const std::string ending : {"COMMIT", "COMMIT WORK", "COMMIT TRANSACTION",
                                    "ROLLBACK", "ROLLBACK WORK", "END WORK", "ABORT TRANSACTION"}) {
        for (const std::string option : {"", " AND CHAIN", " AND NO CHAIN"}) {
            auto parsed = parser.parse(ending + option + ";");
            assert(parsed.success);
            const auto* transaction = dynamic_cast<const dbms::TransactionStmt*>(parsed.stmt.get());
            assert(transaction);
            assert(transaction->chainSpecified == !option.empty());
            assert(transaction->chain == (option == " AND CHAIN"));
            const auto roundtrip = parser.parse(transaction->toString());
            assert(roundtrip.success);
            const auto* copy = dynamic_cast<const dbms::TransactionStmt*>(roundtrip.stmt.get());
            assert(copy && copy->chainSpecified == transaction->chainSpecified &&
                   copy->chain == transaction->chain);
        }
    }
    for (const std::string prefix : {"ROLLBACK", "ROLLBACK WORK", "ROLLBACK TRANSACTION"}) {
        const auto sql = prefix + " /*target*/ TO /*name*/ SAVEPOINT recovery;";
        assert(parser.classify(sql) == dbms::SqlCommand::RollbackToSavepoint);
        auto result = parser.parse(sql);
        assert(result.success);
        const auto* savepoint = dynamic_cast<const dbms::TransactionStmt*>(result.stmt.get());
        assert(savepoint && savepoint->kind == dbms::TransactionStmt::Kind::RollbackTo &&
               savepoint->savepointName == "recovery");
    }
    auto parsed = parser.parse("COMMIT /*first*/ AND /*second*/ CHAIN;");
    assert(parsed.success);
    const auto* transaction = dynamic_cast<const dbms::TransactionStmt*>(parsed.stmt.get());
    assert(transaction && transaction->chain);
    for (const std::string invalid : {"COMMIT AND CHAIN junk", "COMMIT AND",
                                     "ROLLBACK TO recovery AND CHAIN", "END AND OTHER"}) {
        assert(!parser.parse(invalid).success);
    }
    std::cout << "[TRANSACTION CHAIN OPTIONS] passed\n";
}
