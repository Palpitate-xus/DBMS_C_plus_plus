#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db = testDbPath("bit_stored_literal");
    assert(g_engine.createDatabase(db,"utf8")==DBStatus::OK);
    TableSchema table;
    table.tablename="bits";
    table.append(makeIntColumn("id",false,4,true));
    Column bit;
    bit.dataName="v";
    bit.isNull=true;
    assert(TypeRegistry::instance().resolveColumnType(bit,"varbit",{},false).empty());
    table.append(bit);
    assert(g_engine.createTable(db,table)==DBStatus::OK);
    int id=0;
    for (const auto& value : {"1","01","001","0","00","","01","0001"})
        assert(g_engine.insertRow(db,"bits",{{"id",std::to_string(++id)},{"v",value}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"bits",{{"id","9"},{"v",std::nullopt}})==DBStatus::OK);
    const auto sqlQuery = [&](const std::vector<std::string>& conditions) {
        // This is the real SQL/structured adapter overload, not the native
        // data-only compact API (whose literal-shaped bytes stay data).
        return g_engine.query(db,"bits",conditions,{"id"},{},false,false,false,
                              0,{},nullptr,nullptr,nullptr);
    };
    for (const auto& control : std::vector<std::pair<std::string,std::vector<std::string>>>{
            {"=v B'01'",{"2","7"}}, {"=v b'01'",{"2","7"}},
            {"=v X'1'",{"8"}}, {"=v x'1'",{"8"}},
            {"=v B''",{"6"}}, {"=v X''",{"6"}},
            {"<v B'01'",{"3","4","5","6","8"}},
            {">v B'01'",{"1"}}, {"=v 01",{"2","7"}},
            {"=v '01'",{"2","7"}}}) {
        auto rows = sqlQuery({control.first});
        for(auto& row:rows)while(!row.empty() && row.back()==' ')row.pop_back();
        std::sort(rows.begin(),rows.end());
        std::cerr << "[BIT stored] " << control.first << " rows=" << rows.size() << '\n';
        assert(rows==control.second);
    }
    for(const auto& literal:{"B'02'","X'g'"}) {
        bool failed=false;
        try {(void)sqlQuery({std::string("=v ")+literal});}
        catch(const DbError& error){failed=error.sqlState()=="22P02";}
        assert(failed);
    }
    assert(g_engine.query(db,"bits",{},{"id"},{}).size()==9);
    assert(g_engine.dropDatabase(db)==DBStatus::OK);
    std::cout << "[BIT STORED LITERAL] binary/hex/empty/NULL/invalid literal controls passed\n";
}
