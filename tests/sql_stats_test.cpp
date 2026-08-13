#include "process/SqlStats.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <filesystem>
#include <fstream>
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

    assert(dbms::setSqlStatsMaxEntries(2));
    dbms::resetSqlStats();
    dbms::recordSqlStat("SELECT * FROM t1", 1.0, "db1");
    dbms::recordSqlStat("SELECT * FROM t2", 2.0, "db1");
    dbms::recordSqlStat("SELECT * FROM t2", 2.0, "db1");
    dbms::recordSqlStat("SELECT * FROM t3", 3.0, "db1");
    auto bounded = dbms::getSqlStats("db1");
    assert(bounded.size() == 2);
    assert(std::find_if(bounded.begin(), bounded.end(), [](const auto& entry) {
               return entry.sql == "SELECT * FROM t1";
           }) == bounded.end());
    assert(dbms::setSqlStatsMaxEntries(5000));

    dbms::resetSqlStats();
    dbms::recordSqlStat(first, 2.0, "db1");
    dbms::recordSqlStat(second, 4.0, "db1");

    const std::filesystem::path persisted = "sql_stats_persist_test.bin";
    std::filesystem::remove(persisted);
    std::filesystem::remove(persisted.string() + ".lock");
    assert(dbms::persistSqlStats("db1", persisted));
    dbms::resetSqlStats();
    assert(dbms::loadSqlStats("db1", persisted));
    auto restored = dbms::getSqlStats("db1");
    assert(restored.size() == 1);
    assert(restored[0].calls == 2);
    assert(std::abs(restored[0].totalTimeMs - 6.0) < 1e-9);

    {
        std::ofstream corrupt(persisted, std::ios::binary | std::ios::app);
        corrupt << "trailing-corruption";
    }
    dbms::resetSqlStats();
    assert(!dbms::loadSqlStats("db1", persisted));
    std::filesystem::remove(persisted);
    std::filesystem::remove(persisted.string() + ".lock");

    dbms::resetSqlStats();
    assert(dbms::getSqlStats().empty());
    std::cout << "[SQL-STATS] normalization, aggregation, persistence and reset OK\n";
    return 0;
}
