#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "utils/interval.h"
#include "Session.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("interval_storage_range");
    assert(g_engine.createDatabase(db,"utf8") == dbms::DBStatus::OK);
    Session session;
    session.username="testuser"; session.permission=1; session.currentDB=db;
    dbms::DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE t(id INT PRIMARY KEY,v INTERVAL)",session));
    int id=0;
    for (const std::string value : {"2147483648 months","2147483648 days","178956971 years",
             "-2147483649 months","-2147483649 days","9223372036854775808 microseconds",
             "-9223372036854775809 microseconds","1 year bogus","1 fortnight", "1 month ago bogus"}) {
        const auto result = g_engine.insert(db,"t",{{"id",std::to_string(++id)},{"v",value}});
        if (result != dbms::DBStatus::INVALID_VALUE) {
            std::cerr << "invalid interval accepted: " << value << " status=" << static_cast<int>(result) << '\n';
            assert(false);
        }
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    }
    assert(g_engine.query(db,"t",{}, {"id"},{}).empty());
    for (const auto& control : std::vector<std::pair<std::string,std::string>>{
        {"9223372036854775807 microseconds","2562047788:00:54.775807"},
        {"-9223372036854775808 microseconds","-2562047788:00:54.775808"},
        {"2147483647 months","178956970 years 7 mons"},
        {"-2147483648 months","-178956970 years -8 mons"},
        {"2147483647 days","2147483647 days"},
        {"-2147483648 days","-2147483648 days"},
        {"1.5 months","1 mon 15 days"}, {"0.5 years","6 mons"},
        {"1.5 weeks","10 days 12:00:00"}, {"1.5 days","1 day 12:00:00"},
        {"-1.5 months","-1 mons -15 days"}, {"1 yr 2 hrs","1 year 02:00:00"},
        {"1.25 seconds","00:00:01.25"}, {"2 milliseconds","00:00:00.002"},
        {"1 microsecond","00:00:00.000001"}, {"P1Y2M3DT4H5M6S","1 year 2 mons 3 days 04:05:06"},
        {"@ 1 day ago","-1 days"}}) {
        const auto inserted = g_engine.insert(db,"t",{{"id",std::to_string(++id)},{"v",control.first}});
        assert(inserted == dbms::DBStatus::OK);
        const auto rows = g_engine.query(db,"t",{"=id "+std::to_string(id)}, {"v"},{});
        assert(rows == std::vector<std::string>{control.second+" "});
        const auto parsed=dbms::parseIntervalInput(control.first);
        assert(parsed.ok && dbms::intervalInputSqlState(parsed).empty());
        assert(dbms::formatIntervalInput(parsed.months,parsed.days,parsed.micros,true)==control.second);
    }
    for (const auto& control : std::vector<std::pair<std::string,std::string>>{
        {"0.9 years","11 mons"},{"-0.9 years","-11 mons"},{"1.9 years","1 year 11 mons"},
        {"0.1 months","3 days"},{"1.9 months","1 mon 27 days"},{"0.1 days","02:24:00"},
        {"9223372036854775807.4 microseconds","2562047788:00:54.775807"},
        {"9223372036854775807.5 microseconds","2562047788:00:54.775807"},
        {"-9223372036854775808.4 microseconds","-2562047788:00:54.775808"},
        {"-9223372036854775808.5 microseconds","-2562047788:00:54.775808"},
        {"1.5 microseconds","00:00:00.000001"},{"1.0000015 seconds","00:00:01.000001"},
        {"1 us","00:00:00.000001"},{"2 ms","00:00:00.002"},
        {"-1-2","-1 years -2 mons"},{"-0-2","-2 mons"}}) {
        assert(g_engine.insert(db,"t",{{"id",std::to_string(++id)},{"v",control.first}})==dbms::DBStatus::OK);
        assert(g_engine.query(db,"t",{"=id "+std::to_string(id)}, {"v"},{})==
               std::vector<std::string>{control.second+" "});
    }
    const auto before = g_engine.query(db,"t",{"=id "+std::to_string(id)}, {"v"},{});
    for (const std::string value : {"2147483648 months","2147483648 days","178956971 years"}) {
        assert(g_engine.update(db,"t",{{"v",value}},{"=id "+std::to_string(id)})==dbms::DBStatus::INVALID_VALUE);
        assert(g_engine.query(db,"t",{"=id "+std::to_string(id)}, {"v"},{})==before);
    }
    assert(g_engine.update(db,"t",{{"v","-9223372036854775808 microseconds"}},
                          {"=id "+std::to_string(id)})==dbms::DBStatus::OK);
    assert(g_engine.query(db,"t",{"=id "+std::to_string(id)}, {"v"},{})==
           std::vector<std::string>{"-2562047788:00:54.775808 "});
    std::cout << "[INTERVAL STORAGE RANGE] rejection, precise integral time and fractional cascade passed\n";
}
