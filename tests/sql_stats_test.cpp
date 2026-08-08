#include "process/SqlStats.h"

#include <cassert>
#include <cmath>
#include <iostream>

int main() {
    dbms::resetSqlStats();

    const std::string first = " SELECT id FROM t WHERE id = 1 ";
    const std::string second = "select id  from t where id = 2";
    assert(dbms::normalizeSqlForStats(first) == dbms::normalizeSqlForStats(second));
    assert(dbms::normalizeSqlForStats("SELECT name FROM t WHERE name = 'Alice'") ==
           dbms::normalizeSqlForStats("select name from t where name = 'Bob'"));
    assert(dbms::normalizeSqlForStats("SELECT \"CaseSensitive\" FROM t") !=
           dbms::normalizeSqlForStats("SELECT \"casesensitive\" FROM t"));

    dbms::recordSqlStat(first, 2.0, "db1");
    dbms::recordSqlStat(second, 4.0, "db1");
    dbms::recordSqlStat("SELECT id FROM t WHERE id = 3", 9.0, "db2");

    auto db1 = dbms::getSqlStats("db1");
    assert(db1.size() == 1);
    assert(db1[0].calls == 2);
    assert(std::abs(db1[0].totalTimeMs - 6.0) < 1e-9);
    assert(std::abs(db1[0].minTimeMs - 2.0) < 1e-9);
    assert(std::abs(db1[0].maxTimeMs - 4.0) < 1e-9);
    assert(std::abs(db1[0].meanTimeMs - 3.0) < 1e-9);

    auto all = dbms::getSqlStats();
    assert(all.size() == 2);
    dbms::resetSqlStats();
    assert(dbms::getSqlStats().empty());
    std::cout << "[SQL-STATS] normalization, aggregation and reset OK\n";
    return 0;
}
