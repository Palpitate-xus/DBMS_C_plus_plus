#include "utils/plpgsql.h"
#include <iostream>
#include <string>

int main() {
    dbms::PlPgsqlHost host;
    std::string sql;
    host.query = [&](const std::string& query, const dbms::PlPgsqlQueryOptions&) {
        sql = query;
        dbms::PlPgsqlQueryResult result;
        result.ok = true;
        result.rowCount = result.columnCount = 1;
        result.firstRow = {"a"};
        result.columnTypes = {"text"};
        return result;
    };
    int failures = 0;
    auto check = [&](const std::string& expression, const std::string& expected) {
        std::string value, error, state;
        const std::string body = "DECLARE \"C\" TEXT := 'bad'; "
            "\"pg_catalog.C\" TEXT := 'bad'; \"NoSuchCollation\" TEXT := 'bad'; "
            "value TEXT := 'a'; n TEXT; BEGIN SELECT " + expression +
            " INTO n; RETURN n; END;";
        const bool ok = dbms::PlPgsql::run(body, {}, host, value, error,
                                         nullptr, nullptr, nullptr, &state);
        if (!ok || sql != expected) {
            std::cerr << "expression=" << expression << " sql=" << sql
                      << " error=" << state << ':' << error << '\n';
            ++failures;
        }
    };
    check("value COLLATE \"C\"", "SELECT CAST('a' AS TEXT) COLLATE \"C\"");
    check("value COLLATE /* label */ \"C\"", "SELECT CAST('a' AS TEXT) COLLATE /* label */ \"C\"");
    check("value COLLATE pg_catalog.\"C\"", "SELECT CAST('a' AS TEXT) COLLATE pg_catalog.\"C\"");
    check("value COLLATE \"NoSuchCollation\"", "SELECT CAST('a' AS TEXT) COLLATE \"NoSuchCollation\"");
    check("value || \"C\"", "SELECT CAST('a' AS TEXT) || CAST('bad' AS TEXT)");
    if (failures) return 1;
    std::cout << "[PLPGSQL COLLATION LABEL] passed\n";
}
