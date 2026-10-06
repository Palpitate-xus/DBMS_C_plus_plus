#include "utils/plpgsql.h"
#include <iostream>

int main() {
    dbms::PlPgsqlHost host;
    std::string sql;
    host.query = [&](const std::string& query, const dbms::PlPgsqlQueryOptions&) {
        sql = query;
        dbms::PlPgsqlQueryResult result;
        result.ok = true;
        result.rowCount = result.columnCount = 1;
        result.firstRow = {"t"};
        result.columnTypes = {"boolean"};
        return result;
    };
    int failures = 0;
    auto check = [&](const std::string& query, const std::string& expected) {
        std::string value, error, state;
        const std::string body = "DECLARE wanted INT := 2; empty INT; "
            "\"from\" INT := 9; source INT := 5; r INT := 6; n BOOLEAN; BEGIN " +
            query + "; RETURN n; END;";
        const bool ok = dbms::PlPgsql::run(body, {}, host, value, error,
                                         nullptr, nullptr, nullptr, &state);
        if (!ok || sql != expected) {
            std::cerr << query << " got " << sql << ' ' << state << ':' << error << '\n';
            ++failures;
        }
    };
    check("SELECT 1 IS DISTINCT FROM wanted INTO n", "SELECT 1 IS DISTINCT FROM CAST('2' AS INT)");
    check("SELECT 1 IS NOT DISTINCT FROM wanted INTO n", "SELECT 1 IS NOT DISTINCT FROM CAST('2' AS INT)");
    check("SELECT 1 IS /* op */ NOT DISTINCT /* op */ FROM wanted INTO n", "SELECT 1 IS /* op */ NOT DISTINCT /* op */ FROM CAST('2' AS INT)");
    check("SELECT 1 IS DISTINCT FROM empty INTO n", "SELECT 1 IS DISTINCT FROM CAST(NULL AS INT)");
    check("SELECT 1 IS DISTINCT FROM \"from\" INTO n", "SELECT 1 IS DISTINCT FROM CAST('9' AS INT)");
    check("SELECT 1 IS DISTINCT FROM (wanted+1) AS wanted INTO n", "SELECT 1 IS DISTINCT FROM (CAST('2' AS INT)+1) AS wanted");
    check("SELECT r.id IS DISTINCT FROM wanted INTO n FROM source r WHERE r.id=1", "SELECT r.id IS DISTINCT FROM CAST('2' AS INT) FROM source r WHERE r.id=1");
    check("SELECT r.id INTO n FROM source r WHERE r.id IS NOT DISTINCT FROM wanted", "SELECT r.id FROM source r WHERE r.id IS NOT DISTINCT FROM CAST('2' AS INT)");
    if (failures) return 1;
    std::cout << "[PLPGSQL DISTINCT FROM VALUE OPERAND] passed\n";
}
