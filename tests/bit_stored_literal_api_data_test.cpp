#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("bit_stored_literal_api_data");
    assert(g_engine.createDatabase(db,"utf8")==DBStatus::OK);
    TableSchema table;
    table.tablename="t";
    table.append(makeIntColumn("id",false,4,true));
    table.append(makeTextColumn("v",true));
    assert(g_engine.createTable(db,table)==DBStatus::OK);
    int id=0;
    for(const auto& value:{"B'01'","X'1'","01","B'02'",""})
        assert(g_engine.insertRow(db,"t",{{"id",std::to_string(++id)},{"v",value}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"t",{{"id","6"},{"v",std::nullopt}})==DBStatus::OK);
    for(const auto& control:std::vector<std::pair<std::string,std::vector<std::string>>>{
            {"=v B'01'",{"1"}}, {"=v X'1'",{"2"}}, {"=v 01",{"3"}},
            {"=v B'02'",{"4"}}, {"=v ''",{"5"}}}) {
        auto rows=g_engine.query(db,"t",{control.first},{"id"},{});
        for(auto& row:rows)while(!row.empty() && row.back()==' ')row.pop_back();
        std::sort(rows.begin(),rows.end());
        std::cerr << "[BIT API DATA] " << control.first << " rows=" << rows.size() << '\n';
        assert(rows==control.second);
    }
    assert(g_engine.dropDatabase(db)==DBStatus::OK);
    std::cout << "[BIT API DATA] literal-shaped text, invalid bit-shaped text, empty and NULL remain data\n";
}
