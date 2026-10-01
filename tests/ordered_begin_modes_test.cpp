#include "parser/parser.h"

#include <cassert>
#include <iostream>
#include <string>

int main() {
    dbms::SQLParser parser;
    using Mode = dbms::TransactionStmt::Mode;
    for (const std::string prefix : {"BEGIN", "BEGIN WORK", "BEGIN TRANSACTION", "START TRANSACTION"}) {
        auto parsed = parser.parse(prefix +
            " READ ONLY, ISOLATION LEVEL SERIALIZABLE READ WRITE,"
            " ISOLATION LEVEL READ COMMITTED DEFERRABLE, NOT DEFERRABLE;");
        assert(parsed.success);
        const auto* original = dynamic_cast<const dbms::TransactionStmt*>(parsed.stmt.get());
        assert(original && original->modes.size() == 6);
        assert(original->modes[0].kind == Mode::Kind::ReadOnly && original->modes[0].value);
        assert(original->modes[1].kind == Mode::Kind::Isolation &&
               original->modes[1].isolation == dbms::IsolationLevel::SERIALIZABLE);
        assert(original->modes[2].kind == Mode::Kind::ReadOnly && !original->modes[2].value);
        assert(original->modes[3].kind == Mode::Kind::Isolation &&
               original->modes[3].isolation == dbms::IsolationLevel::READ_COMMITTED);
        assert(original->modes[4].kind == Mode::Kind::Deferrable && original->modes[4].value);
        assert(original->modes[5].kind == Mode::Kind::Deferrable && !original->modes[5].value);
        assert(!original->readOnly && !original->deferrable &&
               original->isolation == dbms::IsolationLevel::READ_COMMITTED);
        auto roundtrip = parser.parse(original->toString());
        assert(roundtrip.success);
        const auto* copy = dynamic_cast<const dbms::TransactionStmt*>(roundtrip.stmt.get());
        assert(copy && copy->kind == original->kind && copy->modes.size() == original->modes.size());
        for (size_t i = 0; i < original->modes.size(); ++i) {
            assert(copy->modes[i].kind == original->modes[i].kind &&
                   copy->modes[i].isolation == original->modes[i].isolation &&
                   copy->modes[i].value == original->modes[i].value);
        }
    }
    // Programmatically built ASTs still use their final-value flags when
    // no source-ordered mode list was supplied.
    dbms::TransactionStmt constructed(dbms::TransactionStmt::Kind::Begin);
    constructed.isolationSpecified = true;
    constructed.isolation = dbms::IsolationLevel::REPEATABLE_READ;
    assert(parser.parse(constructed.toString()).success);
    std::cout << "[ORDERED BEGIN MODES] passed\n";
}
