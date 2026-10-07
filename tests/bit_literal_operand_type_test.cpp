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
    const auto database=testDbPath("bit_literal_operand_type");
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    TableSchema table;
    table.tablename="bits";
    table.append(makeIntColumn("id",false,4,true));
    Column bit;
    bit.dataName="v";bit.isNull=true;
    assert(TypeRegistry::instance().resolveColumnType(bit,"varbit",{},false).empty());
    table.append(bit);
    bit.dataName="01";table.append(bit);
    table.append(makeTextColumn("t",true));
    table.append(makeIntColumn("i",true,4));
    assert(g_engine.createTable(database,table)==DBStatus::OK);
    assert(g_engine.insertRow(database,"bits",{{"id","1"},{"v","01"},{"01","1"},{"t","01"},{"i","1"}})==DBStatus::OK);
    assert(g_engine.insertRow(database,"bits",{{"id","2"},{"v","1"},{"01","01"},{"t","1"},{"i","1"}})==DBStatus::OK);
    assert(g_engine.insertRow(database,"bits",{{"id","3"},{"v",""},{"01",""},{"t",""},{"i","0"}})==DBStatus::OK);
    assert(g_engine.insertRow(database,"bits",{{"id","4"},{"v",std::nullopt},{"01",std::nullopt},{"t",std::nullopt},{"i",std::nullopt}})==DBStatus::OK);
    const auto sqlQuery=[&](const std::string& name,const std::string& condition) {
        return g_engine.query(database,name,{condition},{"id"},{},false,false,false,
                              0,{},nullptr,nullptr,nullptr);
    };
    size_t controls=0;
    for(const auto& control:std::vector<std::pair<std::string,std::vector<std::string>>>{
        {"=v B'01'",{"1"}},{"=v '01'",{"1"}},{"=t '01'",{"1"}},
        {"=v B''",{"3"}},{"=v X''",{"3"}},{"=v ''",{"3"}},
        {"inv B''",{"3"}},{"notinv B''",{"1","2"}},
        {"inv X''",{"3"}},{"notinv X''",{"1","2"}},
        {"inv B'' B'01'",{"1","3"}},{"notinv B'' B'01'",{"2"}},
        {"inv B'' NULL",{"3"}},{"notinv B'' NULL",{}},
        {"inv B'01'",{"1"}},{"notinv B'01'",{"2","3"}},
        {"betweenv B'' B'01'",{"1","3"}},{"notbetweenv B'' B'01'",{"2"}}}) {
        ++controls;
        auto rows=sqlQuery("bits",control.first);
        for(auto& row:rows)while(!row.empty()&&row.back()==' ')row.pop_back();
        std::sort(rows.begin(),rows.end());
        std::cerr << "[BIT LITERAL TYPE] " << control.first << " rows=" << rows.size() << '\n';
        assert(rows==control.second);
    }
    for(const auto& column:{std::string("t"),std::string("i")})
        for(const auto& literal:{std::string("B'01'"),std::string("X'1'"),std::string("B''"),std::string("X''")})
            for(const auto& operation:{std::string("="),std::string("<>"),std::string("<"),std::string("<="),std::string(">"),std::string(">=")}) {
                ++controls;
                try {(void)sqlQuery("bits",operation+column+" "+literal);assert(false);}
                catch(const DbError& error){assert(error.sqlState()=="42883");}
            }
    for(const auto& column:{std::string("t"),std::string("i")})
        for(const auto& operation:{std::string("in"),std::string("notin"),std::string("between"),std::string("notbetween")}) {
            ++controls;
            const auto operands=operation.find("between")!=std::string::npos?"B'' B'01'":"B'' B'01' NULL";
            try {(void)sqlQuery("bits",operation+column+" "+operands);assert(false);}
            catch(const DbError& error){assert(error.sqlState()=="42883");}
        }
    table.tablename="empty_bits";
    assert(g_engine.createTable(database,table)==DBStatus::OK);
    for(const auto& condition:{"=t B'01'","=i B''","int B''","betweeni B'' B'01'"}) {
        ++controls;
        try {(void)sqlQuery("empty_bits",condition);assert(false);}
        catch(const DbError& error){assert(error.sqlState()=="42883");}
    }
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    std::cout << "[BIT LITERAL OPERAND TYPE] all " << controls
              << " SQL compact type/empty/list/NULL/column-collision controls passed\n";
}
