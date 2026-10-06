#include "TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine owner;
    const auto db = testDbPath("window_query_metadata");
    assert(owner.createDatabase(db, "utf8") == DBStatus::OK);
    const auto output = [&](const std::string& sql, const std::string& type) {
        try {
            const auto prepared = owner.prepareBoundQuery(db, sql);
            assert(prepared.output.size() == 1);
            if (prepared.output.front().type != type)
                std::cerr << "WINDOW_METADATA " << sql << " expected type " << type
                          << " actual " << prepared.output.front().type << '\n';
            assert(prepared.output.front().type == type);
        } catch (const DbError& error) {
            std::cerr << "WINDOW_METADATA " << sql << " actual " << error.sqlState()
                      << ' ' << error.message() << '\n';
            throw;
        }
    };
    output("SELECT row_number() OVER () AS n", "bigint");
    output("SELECT rank() OVER () AS n", "bigint");
    output("SELECT dense_rank() OVER () AS n", "bigint");
    output("SELECT percent_rank() OVER () AS n", "double precision");
    output("SELECT cume_dist() OVER () AS n", "double precision");
    output("SELECT ntile(2) OVER () AS n", "integer");
    output("SELECT lag(1) OVER () AS n", "integer");
    output("SELECT lead(2147483648::bigint) OVER () AS n", "bigint");
    output("SELECT first_value('a'::text) OVER () AS n", "text");
    output("SELECT last_value(NULL::int) OVER () AS n", "integer");
    output("SELECT nth_value(1,2) OVER () AS n", "integer");
    output("SELECT pg_catalog.row_number() OVER () AS n", "bigint");
    output("SELECT pg_catalog.\"row_number\"() OVER () AS n", "bigint");
    const auto error = [&](const std::string& sql, const std::string& state) {
        bool failed = false;
        try { (void)owner.prepareBoundQuery(db, sql); }
        catch (const DbError& value) {
            if (value.sqlState() != state)
                std::cerr << "WINDOW_METADATA " << sql << " expected " << state
                          << " actual " << value.sqlState() << ' ' << value.message() << '\n';
            assert(value.sqlState() == state);
            failed = true;
        }
        assert(failed);
    };
    error("SELECT row_number()", "42809");
    error("SELECT row_number(1) OVER ()", "42883");
    error("SELECT ntile() OVER ()", "42883");
    error("SELECT ntile(2::bigint) OVER ()", "42883");
    error("SELECT lead() OVER ()", "42883");
    error("SELECT lag(1,2,3,4) OVER ()", "42883");
    error("SELECT nth_value(1) OVER ()", "42883");
    error("SELECT public.row_number() OVER ()", "42883");
    error("SELECT \"Row_Number\"() OVER ()", "42883");
    error("SELECT row_number() FILTER (WHERE true) OVER ()", "0A000");
    assert(owner.dropDatabase(db) == DBStatus::OK);
    cleanupTestDb("window_query_metadata");
    std::cout << "[WINDOW QUERY METADATA] typed signatures, namespace and OVER contract passed\n";
}
