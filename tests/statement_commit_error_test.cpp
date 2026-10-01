#include "common/DbError.h"

#include <cassert>
#include <iostream>
#include <type_traits>

int main() {
    static_assert(std::is_base_of_v<dbms::DbError, dbms::StatementCommitError>);
    static_assert(sizeof(dbms::DbError) == sizeof(dbms::StatementCommitError));
    try {
        throw dbms::StatementCommitError("23503", "commit recheck failed");
    } catch (const dbms::DbError& error) {
        assert(error.sqlState() == "23503");
        assert(error.message() == "commit recheck failed");
        assert(dynamic_cast<const dbms::StatementCommitError*>(&error));
    }
    const dbms::DbError ordinary("23505", "statement failed");
    const dbms::DbError* ordinaryError = &ordinary;
    assert(dynamic_cast<const dbms::StatementCommitError*>(ordinaryError) == nullptr);
    std::cout << "[STATEMENT COMMIT ERROR] passed" << std::endl;
}
