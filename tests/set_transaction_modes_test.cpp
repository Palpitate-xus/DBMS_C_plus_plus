#include "parser/parser.h"

#include <cassert>
#include <iostream>
#include <string>

int main() {
    dbms::SQLParser parser;
    for (const std::string modes : {"READ ONLY, ISOLATION LEVEL SERIALIZABLE",
            "ISOLATION LEVEL READ COMMITTED READ WRITE NOT DEFERRABLE",
            "READ ONLY, READ WRITE, READ ONLY",
            "ISOLATION LEVEL SERIALIZABLE, ISOLATION LEVEL READ COMMITTED"}) {
        auto parsed = parser.parse("SET TRANSACTION " + modes + ";");
        assert(parsed.success);
        const auto* transaction = dynamic_cast<const dbms::TransactionStmt*>(parsed.stmt.get());
        assert(transaction && transaction->kind == dbms::TransactionStmt::Kind::SetCharacteristics);
        assert(transaction->command == dbms::SqlCommand::Set && transaction->modes.size() >= 2);
        auto roundtrip = parser.parse(transaction->toString());
        const auto* copy = dynamic_cast<const dbms::TransactionStmt*>(roundtrip.stmt.get());
        assert(roundtrip.success && copy && copy->modes.size() == transaction->modes.size());
        for (size_t i = 0; i < copy->modes.size(); ++i) {
            assert(copy->modes[i].kind == transaction->modes[i].kind);
            assert(copy->modes[i].isolation == transaction->modes[i].isolation);
            assert(copy->modes[i].value == transaction->modes[i].value);
        }
    }
    for (const std::string modes : {"", "WORK", "READ COMMITTED", "ISOLATION SERIALIZABLE",
            "ISOLATION LEVEL", "READ ONLY,", ", READ ONLY", "READ ONLY,, READ WRITE",
            "READ ONLY GARBAGE", "READ ONLY; READ WRITE"}) {
        assert(!parser.parse("SET TRANSACTION " + modes + ";").success);
    }
    assert(dbms::SQLParser::isSetTransactionStatement("/* lead */ SET /* gap */ TRANSACTION READ ONLY;"));
    assert(!dbms::SQLParser::isSetTransactionStatement("SET transaction_timeout = 1;"));
    assert(!dbms::SQLParser::isSetTransactionStatement("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;"));
    auto setting = parser.parse("SET search_path TO public;");
    assert(setting.success && dynamic_cast<const dbms::SetStmt*>(setting.stmt.get()));
    std::cout << "[SET TRANSACTION MODES] passed\n";
}
