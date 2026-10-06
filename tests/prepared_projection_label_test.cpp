#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
extern dbms::StorageEngine g_engine;
int main(){
    using namespace dbms;const std::string name="prepared_projection_label",db=testDbPath(name);
    assert(g_engine.createDatabase(db)==DBStatus::OK);size_t failures=0;
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{
        {"SELECT CASE 1 WHEN 1 THEN 1 ELSE 2 END","case"},
        {"SELECT CAST(CASE 1 WHEN 1 THEN 1 ELSE 2 END AS BIGINT)","int8"},
        {"SELECT (CASE 1 WHEN 1 THEN 1 ELSE 2 END)::INTEGER","int4"},
        {"SELECT CASE 1 WHEN 1 THEN CAST('a' AS TEXT) ELSE 'b' END COLLATE \"C\"","case"},
        {"SELECT CASE 1 WHEN 1 THEN 1 ELSE 2 END AS \"C\"","C"},
        {"SELECT -(CASE 1 WHEN 1 THEN 1 ELSE 2 END)","?column?"},
        {"SELECT abs(CASE 1 WHEN 1 THEN 1 ELSE 2 END)","abs"},
        {"SELECT CAST(abs(CASE 1 WHEN 1 THEN 1 ELSE 2 END) AS BIGINT)","abs"}}){
        const auto prepared=g_engine.prepareBoundQuery(db,item.first);
        if(prepared.output.at(0).name!=item.second){++failures;std::cerr<<"PROJECTION_LABEL_FAILURE "<<item.first<<" actual="<<prepared.output.at(0).name<<" expected="<<item.second<<std::endl;}
    }
    try {
        const auto named=g_engine.prepareBoundQuery(db,"WITH r AS (SELECT CASE 1 WHEN 1 THEN 1 ELSE 2 END) SELECT \"case\" FROM r");
        assert(named.output.at(0).name=="case");
    }catch(const DbError& error){++failures;std::cerr<<"PROJECTION_LABEL_FAILURE derived CASE lookup "<<error.sqlState()<<std::endl;}
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cerr<<"PROJECTION_LABEL failures="<<failures<<std::endl;assert(!failures);
}
