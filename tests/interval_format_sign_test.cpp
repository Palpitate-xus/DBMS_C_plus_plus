#include "commands/TableManage.h"
#include "utils/interval.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    StorageEngine engine;const std::string name="interval_format_sign",db=testDbPath(name);
    assert(engine.createDatabase(db)==DBStatus::OK);
    TableSchema schema;schema.len=2;
    schema.cols[0].dataName="id";schema.cols[0].dataType="integer";schema.cols[0].dsize=4;
    schema.cols[1].dataName="v";schema.cols[1].dataType="interval";schema.cols[1].dsize=128;
    assert(engine.createTable(db,"format_rows",schema)==DBStatus::OK);
    size_t failures=0;int id=0;
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"1 month -2 days 3 hours","1 mon -2 days +03:00:00"},
        {"-1 month 2 days -3 hours","-1 mons +2 days -03:00:00"},
        {"-1 year 1 day 3 hours","-1 years +1 day 03:00:00"},
        {"-1 month 0 days 3 hours","-1 mons +03:00:00"},
        {"1 month -2 days 0 hours","1 mon -2 days"},
        {"-1 month 2 days 0.25 seconds","-1 mons +2 days 00:00:00.25"},
        {"1 month 2 days 3 hours","1 mon 2 days 03:00:00"},
        {"-1 month -2 days -3 hours","-1 mons -2 days -03:00:00"},
        {"0 days","00:00:00"}}) {
        const auto parsed=parseIntervalInput(item.first);assert(parsed.ok);
        const auto actual=formatIntervalInput(parsed.months,parsed.days,parsed.micros,true);
        if(actual!=item.second){++failures;std::cerr<<"INTERVAL_FORMAT_FAILURE "<<item.first<<" actual="<<actual<<" expected="<<item.second<<std::endl;}
        const auto roundtrip=parseIntervalInput(actual);assert(roundtrip.ok);
        assert(roundtrip.months==parsed.months&&roundtrip.days==parsed.days&&roundtrip.micros==parsed.micros);
        const auto key=std::to_string(++id);
        assert(engine.insert(db,"format_rows",{{"id",key},{"v",item.first}})==DBStatus::OK);
        const auto rows=engine.query(db,"format_rows",{"=id "+key},{"v"},{});
        if(rows!=std::vector<std::string>{item.second+" "}){++failures;std::cerr<<"INTERVAL_FORMAT_FAILURE stored id="<<key<<std::endl;}
    }
    assert(engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    assert(failures==0);
    std::cout<<"[INTERVAL FORMAT SIGN] mixed signs, reset, omitted zero and storage roundtrip passed\n";
}
