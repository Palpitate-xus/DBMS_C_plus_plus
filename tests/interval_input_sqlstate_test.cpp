#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "test_utils.h"

#include <cassert>
#include <iostream>
#include <set>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    using SqlCell = StorageEngine::SqlCell;
    TypeRegistry::instance().bootstrap();
    const std::string name = "interval_input_sqlstate", db = testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db) == DBStatus::OK);
    Session session;
    session.username="admin"; session.permission=1; session.currentDB=db;
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE rows_table(id INT PRIMARY KEY,v INTERVAL)",session));
    assert(g_engine.insertRow(db,"rows_table",{{"id","1"},{"v","1 day"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"rows_table",{{"id","2"},{"v",std::nullopt}})==DBStatus::OK);
    const auto rows = [&] {
        std::vector<std::vector<std::string>> values;
        std::vector<std::vector<bool>> nulls;
        g_engine.query(db,"rows_table",{}, {"id","v"}, {},false,false,false,0,{},&values,&nulls);
        return std::make_pair(values,nulls);
    };
    const auto before=rows();
    for (const auto& input : std::vector<std::pair<std::string,std::string>>{
            {"2147483648 months","22015"},{"-2147483649 days","22015"},
            {"178956971 years","22008"},{"9223372036854775808 microseconds","22015"},
            {"-9223372036854775809 microseconds","22015"},
            {"1 fortnight","22007"},{"1 year bogus","22007"},{"","22007"},
            {std::string(256,'9')+" days","22007"}}) {
        // Direct storage keeps its DBStatus contract. SQL uses DbError,
        // including zero-match UPDATE validation, without rendering/parsing it.
        assert(g_engine.insert(db,"rows_table",{{"id","3"},{"v",input.first}})==DBStatus::INVALID_VALUE);
        for (const std::string& sql : {
            "INSERT INTO rows_table VALUES(3,'"+input.first+"')",
            "UPDATE rows_table SET v='"+input.first+"' WHERE false",
            "UPDATE rows_table SET v='"+input.first+"' WHERE id=1"}) {
            bool handled=false, precise=false;
            try {
                (void)tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql);
            } catch (const DbError& failure) {
                precise=failure.sqlState()==input.second;
                if (!precise) std::cerr<<sql<<": actual "<<failure.sqlState()<<" expected "<<input.second<<'\n';
            }
            if (!precise) std::cerr<<"missing structured interval error: "<<sql<<'\n';
            assert(precise);
            assert(rows()==before);
            assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
        }
    }
    for (const auto& input : std::vector<std::pair<std::string,std::string>>{
             {"missing_interval_function(1)","42883"},{"missing_interval_column","42703"}}) {
        const std::string sql="INSERT INTO rows_table VALUES("+input.first+",'2147483648 months')";
        bool handled=false, precise=false;
        try {
            (void)tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql);
        } catch (const DbError& failure) {
            precise=failure.sqlState()==input.second;
        }
        assert(precise && rows()==before);
    }
    const auto run = [&](const std::string& sql) {
        bool handled=false;
        assert(!tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql));
        assert(handled);
    };
    run("UPDATE rows_table SET v='-9223372036854775808 microseconds' WHERE id=1");
    assert(g_engine.query(db,"rows_table",{"=id 1"},{"v"})==
           std::vector<std::string>{"-2562047788:00:54.775808 "});
    run("UPDATE rows_table SET v=NULL WHERE id=1");
    assert(g_engine.query(db,"rows_table",{"isnull v"},{"id"}).size()==2);
    assert(!ddl.executeSql("CREATE TABLE quoted_table(\"I\" INTERVAL,t TEXT)",session));
    run("INSERT INTO quoted_table(t,\"I\") VALUES('','1 us'),(NULL,NULL)");
    const auto quotedValues = [&] {
        // Physical iteration has no SQL ORDER BY contract: an MVCC UPDATE
        // replaces the changed occurrence after the surviving NULL row.
        std::multiset<std::pair<SqlCell,SqlCell>> values;
        const auto table=g_engine.getTableSchema(db,"quoted_table");
        assert(table.cols[0].dataName=="I" && table.cols[1].dataName=="t");
        assert(g_engine.forEachVisibleRow(db,"quoted_table","SELECT",
            [&](uint32_t page,uint16_t slot,const char* data,size_t length) {
                const std::string buffer(data,length);
                const auto rid=StorageEngine::encodeRid(page,slot);
                const auto cell = [&](size_t column) -> SqlCell {
                    if (g_engine.isColumnNullByRid(db,"quoted_table",rid,column)) return {};
                    return g_engine.extractColumnValue(buffer,table,column,db);
                };
                values.emplace(cell(0),cell(1));
            }));
        return values;
    };
    assert((quotedValues()==std::multiset<std::pair<SqlCell,SqlCell>>({{"00:00:00.000001",""},{std::nullopt,std::nullopt}})));
    run("UPDATE quoted_table SET \"I\"='2 ms' WHERE t=''");
    assert((quotedValues()==std::multiset<std::pair<SqlCell,SqlCell>>({{"00:00:00.002",""},{std::nullopt,std::nullopt}})));
    for (const std::string value : {"NULL", "1 day (SQLSTATE 99999)"}) {
        const std::string sql="UPDATE rows_table SET v='"+value+"' WHERE false";
        bool handled=false, precise=false;
        try {
            (void)tryDmlBridge(sql,SQLParser::classify(sql),session,handled,sql);
        } catch (const DbError& failure) {
            precise=failure.sqlState()=="22007";
        }
        assert(precise);
    }
    assert(g_engine.dropDatabase(db)==DBStatus::OK);
    cleanupTestDb(name);
    std::cout<<"[INTERVAL INPUT SQLSTATE] structured input errors, NULL and primitive contract passed\n";
}
