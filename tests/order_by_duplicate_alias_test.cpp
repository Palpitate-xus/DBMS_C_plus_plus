#include "commands/TableManage.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("order_by_duplicate_alias");
    cleanupTestDb("order_by_duplicate_alias");
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    TableSchema schema;schema.len=3;
    for(size_t i=0;i<3;++i) {
        schema.cols[i].dataName=i==0?"id":i==1?"v":"word";
        schema.cols[i].dataType=i==2?"text":"int";
        schema.cols[i].dsize=i==2?64:4;
    }
    assert(g_engine.createTable(database,"alias_rows",schema)==DBStatus::OK);
    size_t checks=0,failures=0;
    const auto check=[&](bool value,const std::string& label) {
        ++checks;if(!value){++failures;std::cerr<<"FAIL "<<label<<'\n';}
    };
    for(const auto* target:{"id AS x,v AS x","id AS x,id+0 AS x","id AS x,id::bigint AS x",
            "1 AS x,2 AS x","1.0 AS x,1.00 AS x","sum(id) AS x,sum(v) AS x",
            "(SELECT 1) AS x,(SELECT 2) AS x",
            "row_number() OVER(ORDER BY id) AS x,row_number() OVER(ORDER BY v) AS x",
            "row_number() OVER(ORDER BY id) AS x,row_number() OVER(ORDER BY t.id) AS x",
            "'a' AS x,'a'::text AS x","NULL AS x,NULL::text AS x","'NULL' AS x,NULL AS x",
            "(SELECT 1 AS one) AS x,(SELECT 1 AS two) AS x",
            "(SELECT 1) AS x,(SELECT 1::integer) AS x","1 AS x,1::numeric AS x",
            "1+1 AS x,2 AS x","1e0 AS x,1.0 AS x"})
        for(const auto* suffix:{""," WHERE FALSE"," LIMIT 0"}) {
            const std::string tail=suffix;
            const auto sql=std::string("SELECT ")+target+" FROM alias_rows t"+
                (tail==" WHERE FALSE"?tail:"")+" ORDER BY x"+(tail==" LIMIT 0"?tail:"");
            std::string state;
            try{(void)g_engine.prepareBoundQuery(database,sql);}
            catch(const DbError& error){state=error.sqlState();}
            check(state=="42702",sql+" got "+state);
        }
    for(const auto* target:{"id AS x,t.id AS x","id AS x,id::integer AS x","1 AS x,01 AS x",
            "1 AS x,'01'::integer AS x","1.00 AS x,01.00 AS x",
            "lower(word) AS x,pg_catalog.lower(word) AS x","(SELECT 1) AS x,(SELECT 01) AS x",
            "sum(id) AS x,sum(t.id) AS x","id+v AS x,t.id+t.v AS x",
            "row_number() OVER(ORDER BY id) AS x,row_number() OVER(ORDER BY id) AS x"}) {
        const auto sql=std::string("SELECT ")+target+" FROM alias_rows t ORDER BY x";
        try {
            auto query=g_engine.prepareBoundQuery(database,sql);
            const auto* select=dynamic_cast<const SelectStmt*>(query.ast.get());
            check(select && query.sortOutputOwners.at(select)==std::vector<size_t>({0,0}),sql);
        } catch(const std::exception& error){check(false,sql+" got "+error.what());}
    }
    auto query=g_engine.prepareBoundQuery(database,"SELECT t.*,t.id AS id FROM alias_rows t ORDER BY id");
    auto* select=dynamic_cast<const SelectStmt*>(query.ast.get());
    check(query.sortOutputOwners.at(select)==std::vector<size_t>({0,1,2,0}),"expanded star source identity");
    query=g_engine.prepareBoundQuery(database,"SELECT id AS x,v AS x FROM alias_rows ORDER BY 2");
    check(query.sortOutputOwners.empty(),"ordinal does not merge independent target sites");
    query=g_engine.prepareBoundQuery(database,"SELECT id AS x,id AS x FROM alias_rows");
    check(query.sortOutputOwners.empty(),"unreferenced duplicate names do not merge sites");
    query=g_engine.prepareBoundQuery(database,"SELECT row_number() OVER w AS x,row_number() OVER(ORDER BY id) AS x FROM alias_rows WINDOW w AS(ORDER BY id) ORDER BY x");
    select=dynamic_cast<const SelectStmt*>(query.ast.get());
    check(query.sortOutputOwners.at(select)==std::vector<size_t>({0,0}),"named/inline same parsed window");
    for(const auto* offset:{"(WITH q AS(SELECT t.id AS n) SELECT n FROM q)",
                           "(SELECT q.n FROM(SELECT t.id AS n) q)"})
        for(const auto* suffix:{""," WHERE FALSE"," LIMIT 0"}) {
            const std::string tail=suffix;
            const auto sql=std::string("SELECT sum(v) OVER w AS x,sum(v) OVER w AS x FROM alias_rows t")+
                (tail==" WHERE FALSE"?tail:"")+" WINDOW w AS(ORDER BY id ROWS "+offset+
                " PRECEDING) ORDER BY x"+(tail==" LIMIT 0"?tail:"");
            std::string state;
            try{(void)g_engine.prepareBoundQuery(database,sql);}
            catch(const DbError& error){state=error.sqlState();}
            check(state=="42P10",sql+" got "+state);
        }
    for(const auto& [sql,state]:std::vector<std::pair<std::string,std::string>>{
            {"SELECT row_number() OVER absent FROM alias_rows","42704"},
            {"SELECT row_number() OVER w FROM alias_rows WINDOW w AS(),w AS()","42P20"}}) {
        std::string actual;
        try{(void)g_engine.prepareBoundQuery(database,sql);}
        catch(const DbError& error){actual=error.sqlState();}
        check(actual==state,sql+" got "+actual);
    }
    size_t rows=0;
    assert(g_engine.forEachRow(database,"alias_rows",[&](uint32_t,uint16_t,const char*,size_t){++rows;}));
    check(rows==0,"pure preparation never writes source rows");
    g_engine.endBackendSession();assert(g_engine.dropDatabase(database)==DBStatus::OK);
    cleanupTestDb("order_by_duplicate_alias");
    std::cout<<"ORDER_ALIAS_NATIVE_COMPLETE "<<checks<<" FAILURES "<<failures<<std::endl;
    return failures?1:0;
}
