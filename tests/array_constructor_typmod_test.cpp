#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "test_utils.h"
#include <iostream>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;
using namespace dbms;
static int checks=0,failures=0;
static void check(bool pass,const std::string& label) {
    ++checks;if(!pass){++failures;std::cerr<<"FAIL "<<label<<'\n';}
}
int main() {
    TypeRegistry::instance().bootstrap();const std::string name="array_constructor_typmod";
    const auto database=testDbPath(name);
    check(g_engine.createDatabase(database)==DBStatus::OK,"create isolated database");
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    setCurrentSession(&session);DdlExecutor ddl;
    check(!ddl.executeSql("CREATE TABLE r324_mods(id integer,c char(4),v varchar(6),b bit(4),bv varbit(4),n numeric(6,2),ts timestamp(3),tm time(3))",session),"create declared column metadata");
    struct Case {std::string sql;uint32_t oid;int32_t modifier;};
    // Physical output metadata independently frozen from actual PostgreSQL
    // 18.6 before the binder change. Rows are not executed to discover types.
    const std::vector<Case> cases={
        {"SELECT ARRAY[CAST('abc' AS character(4))] AS v",1014,8},
        {"SELECT ARRAY[CAST('abc' AS character(4)),CAST('xy' AS character(4))] AS v",1014,8},
        {"SELECT ARRAY[CAST('abc' AS character(4)),CAST('xy' AS character(3))] AS v",1014,-1},
        {"SELECT ARRAY[CAST('abc' AS character(4)),CAST(NULL AS character(4))] AS v",1014,8},
        {"SELECT ARRAY[CAST('abc' AS character(4)),NULL] AS v",1014,-1},
        {"SELECT ARRAY[ARRAY[CAST('abc' AS character(4))],ARRAY[CAST('xy' AS character(4))]] AS v",1014,8},
        {"SELECT ARRAY[ARRAY[CAST('abc' AS character(4))],ARRAY[CAST('xy' AS character(3))]] AS v",1014,-1},
        {"SELECT ARRAY[CAST(COALESCE(CAST('abc' AS character(4)),CAST(NULL AS character(4))) AS character(4))] AS v",1014,8},
        {"SELECT CAST('abc' AS character(4)) AS v",1042,8},
        {"SELECT ARRAY[CAST('abcd' AS varchar(4))] AS v",1015,8},
        {"SELECT ARRAY[CAST('abcd' AS varchar(4)),CAST('xy' AS varchar(4))] AS v",1015,8},
        {"SELECT ARRAY[CAST('abcd' AS varchar(4)),CAST('xy' AS varchar(3))] AS v",1015,-1},
        {"SELECT ARRAY[CAST('abcd' AS varchar(4)),CAST(NULL AS varchar(4))] AS v",1015,8},
        {"SELECT ARRAY[CAST('abcd' AS varchar(4)),NULL] AS v",1015,-1},
        {"SELECT ARRAY[ARRAY[CAST('abcd' AS varchar(4))],ARRAY[CAST('xy' AS varchar(4))]] AS v",1015,8},
        {"SELECT ARRAY[ARRAY[CAST('abcd' AS varchar(4))],ARRAY[CAST('xy' AS varchar(3))]] AS v",1015,-1},
        {"SELECT ARRAY[CAST(COALESCE(CAST('abcd' AS varchar(4)),CAST(NULL AS varchar(4))) AS varchar(4))] AS v",1015,8},
        {"SELECT CAST('abcd' AS varchar(4)) AS v",1043,8},
        {"SELECT ARRAY[CAST(B'1010' AS bit(4))] AS v",1561,4},
        {"SELECT ARRAY[CAST(B'1010' AS bit(4)),CAST(B'0101' AS bit(4))] AS v",1561,4},
        {"SELECT ARRAY[CAST(B'1010' AS bit(4)),CAST(B'0101' AS bit(3))] AS v",1561,-1},
        {"SELECT ARRAY[CAST(B'1010' AS bit(4)),CAST(NULL AS bit(4))] AS v",1561,4},
        {"SELECT ARRAY[CAST(B'1010' AS bit(4)),NULL] AS v",1561,-1},
        {"SELECT ARRAY[ARRAY[CAST(B'1010' AS bit(4))],ARRAY[CAST(B'0101' AS bit(4))]] AS v",1561,4},
        {"SELECT ARRAY[ARRAY[CAST(B'1010' AS bit(4))],ARRAY[CAST(B'0101' AS bit(3))]] AS v",1561,-1},
        {"SELECT ARRAY[CAST(COALESCE(CAST(B'1010' AS bit(4)),CAST(NULL AS bit(4))) AS bit(4))] AS v",1561,4},
        {"SELECT CAST(B'1010' AS bit(4)) AS v",1560,4},
        {"SELECT ARRAY[CAST(B'1010' AS varbit(4))] AS v",1563,4},
        {"SELECT ARRAY[CAST(B'1010' AS varbit(4)),CAST(B'01' AS varbit(4))] AS v",1563,4},
        {"SELECT ARRAY[CAST(B'1010' AS varbit(4)),CAST(B'01' AS varbit(3))] AS v",1563,-1},
        {"SELECT ARRAY[CAST(B'1010' AS varbit(4)),CAST(NULL AS varbit(4))] AS v",1563,4},
        {"SELECT ARRAY[CAST(B'1010' AS varbit(4)),NULL] AS v",1563,-1},
        {"SELECT ARRAY[ARRAY[CAST(B'1010' AS varbit(4))],ARRAY[CAST(B'01' AS varbit(4))]] AS v",1563,4},
        {"SELECT ARRAY[ARRAY[CAST(B'1010' AS varbit(4))],ARRAY[CAST(B'01' AS varbit(3))]] AS v",1563,-1},
        {"SELECT ARRAY[CAST(COALESCE(CAST(B'1010' AS varbit(4)),CAST(NULL AS varbit(4))) AS varbit(4))] AS v",1563,4},
        {"SELECT CAST(B'1010' AS varbit(4)) AS v",1562,4},
        {"SELECT ARRAY[CAST('12.345' AS numeric(6,2))] AS v",1231,393222},
        {"SELECT ARRAY[CAST('12.345' AS numeric(6,2)),CAST('-3.14159' AS numeric(6,2))] AS v",1231,393222},
        {"SELECT ARRAY[CAST('12.345' AS numeric(6,2)),CAST('-3.14159' AS numeric(7,3))] AS v",1231,-1},
        {"SELECT ARRAY[CAST('12.345' AS numeric(6,2)),CAST(NULL AS numeric(6,2))] AS v",1231,393222},
        {"SELECT ARRAY[CAST('12.345' AS numeric(6,2)),NULL] AS v",1231,-1},
        {"SELECT ARRAY[ARRAY[CAST('12.345' AS numeric(6,2))],ARRAY[CAST('-3.14159' AS numeric(6,2))]] AS v",1231,393222},
        {"SELECT ARRAY[ARRAY[CAST('12.345' AS numeric(6,2))],ARRAY[CAST('-3.14159' AS numeric(7,3))]] AS v",1231,-1},
        {"SELECT ARRAY[CAST(COALESCE(CAST('12.345' AS numeric(6,2)),CAST(NULL AS numeric(6,2))) AS numeric(6,2))] AS v",1231,393222},
        {"SELECT CAST('12.345' AS numeric(6,2)) AS v",1700,393222},
        {"SELECT ARRAY[CAST('03:04:05.123456' AS time(3))] AS v",1183,3},
        {"SELECT ARRAY[CAST('03:04:05.123456' AS time(3)),CAST('23:59:58.654321' AS time(3))] AS v",1183,3},
        {"SELECT ARRAY[CAST('03:04:05.123456' AS time(3)),CAST('23:59:58.654321' AS time(1))] AS v",1183,-1},
        {"SELECT ARRAY[CAST('03:04:05.123456' AS time(3)),CAST(NULL AS time(3))] AS v",1183,3},
        {"SELECT ARRAY[CAST('03:04:05.123456' AS time(3)),NULL] AS v",1183,-1},
        {"SELECT ARRAY[ARRAY[CAST('03:04:05.123456' AS time(3))],ARRAY[CAST('23:59:58.654321' AS time(3))]] AS v",1183,3},
        {"SELECT ARRAY[ARRAY[CAST('03:04:05.123456' AS time(3))],ARRAY[CAST('23:59:58.654321' AS time(1))]] AS v",1183,-1},
        {"SELECT ARRAY[CAST(COALESCE(CAST('03:04:05.123456' AS time(3)),CAST(NULL AS time(3))) AS time(3))] AS v",1183,3},
        {"SELECT CAST('03:04:05.123456' AS time(3)) AS v",1083,3},
        {"SELECT ARRAY[CAST('03:04:05.123456+02' AS timetz(3))] AS v",1270,3},
        {"SELECT ARRAY[CAST('03:04:05.123456+02' AS timetz(3)),CAST('23:59:58.654321-03' AS timetz(3))] AS v",1270,3},
        {"SELECT ARRAY[CAST('03:04:05.123456+02' AS timetz(3)),CAST('23:59:58.654321-03' AS timetz(1))] AS v",1270,-1},
        {"SELECT ARRAY[CAST('03:04:05.123456+02' AS timetz(3)),CAST(NULL AS timetz(3))] AS v",1270,3},
        {"SELECT ARRAY[CAST('03:04:05.123456+02' AS timetz(3)),NULL] AS v",1270,-1},
        {"SELECT ARRAY[ARRAY[CAST('03:04:05.123456+02' AS timetz(3))],ARRAY[CAST('23:59:58.654321-03' AS timetz(3))]] AS v",1270,3},
        {"SELECT ARRAY[ARRAY[CAST('03:04:05.123456+02' AS timetz(3))],ARRAY[CAST('23:59:58.654321-03' AS timetz(1))]] AS v",1270,-1},
        {"SELECT ARRAY[CAST(COALESCE(CAST('03:04:05.123456+02' AS timetz(3)),CAST(NULL AS timetz(3))) AS timetz(3))] AS v",1270,3},
        {"SELECT CAST('03:04:05.123456+02' AS timetz(3)) AS v",1266,3},
        {"SELECT ARRAY[CAST('2020-01-02 03:04:05.123456' AS timestamp(3))] AS v",1115,3},
        {"SELECT ARRAY[CAST('2020-01-02 03:04:05.123456' AS timestamp(3)),CAST('2021-02-03 23:59:58.654321' AS timestamp(3))] AS v",1115,3},
        {"SELECT ARRAY[CAST('2020-01-02 03:04:05.123456' AS timestamp(3)),CAST('2021-02-03 23:59:58.654321' AS timestamp(1))] AS v",1115,-1},
        {"SELECT ARRAY[CAST('2020-01-02 03:04:05.123456' AS timestamp(3)),CAST(NULL AS timestamp(3))] AS v",1115,3},
        {"SELECT ARRAY[CAST('2020-01-02 03:04:05.123456' AS timestamp(3)),NULL] AS v",1115,-1},
        {"SELECT ARRAY[ARRAY[CAST('2020-01-02 03:04:05.123456' AS timestamp(3))],ARRAY[CAST('2021-02-03 23:59:58.654321' AS timestamp(3))]] AS v",1115,3},
        {"SELECT ARRAY[ARRAY[CAST('2020-01-02 03:04:05.123456' AS timestamp(3))],ARRAY[CAST('2021-02-03 23:59:58.654321' AS timestamp(1))]] AS v",1115,-1},
        {"SELECT ARRAY[CAST(COALESCE(CAST('2020-01-02 03:04:05.123456' AS timestamp(3)),CAST(NULL AS timestamp(3))) AS timestamp(3))] AS v",1115,3},
        {"SELECT CAST('2020-01-02 03:04:05.123456' AS timestamp(3)) AS v",1114,3},
        {"SELECT ARRAY[CAST('2020-01-02 03:04:05.123456+02' AS timestamptz(3))] AS v",1185,3},
        {"SELECT ARRAY[CAST('2020-01-02 03:04:05.123456+02' AS timestamptz(3)),CAST('2021-02-03 23:59:58.654321-03' AS timestamptz(3))] AS v",1185,3},
        {"SELECT ARRAY[CAST('2020-01-02 03:04:05.123456+02' AS timestamptz(3)),CAST('2021-02-03 23:59:58.654321-03' AS timestamptz(1))] AS v",1185,-1},
        {"SELECT ARRAY[CAST('2020-01-02 03:04:05.123456+02' AS timestamptz(3)),CAST(NULL AS timestamptz(3))] AS v",1185,3},
        {"SELECT ARRAY[CAST('2020-01-02 03:04:05.123456+02' AS timestamptz(3)),NULL] AS v",1185,-1},
        {"SELECT ARRAY[ARRAY[CAST('2020-01-02 03:04:05.123456+02' AS timestamptz(3))],ARRAY[CAST('2021-02-03 23:59:58.654321-03' AS timestamptz(3))]] AS v",1185,3},
        {"SELECT ARRAY[ARRAY[CAST('2020-01-02 03:04:05.123456+02' AS timestamptz(3))],ARRAY[CAST('2021-02-03 23:59:58.654321-03' AS timestamptz(1))]] AS v",1185,-1},
        {"SELECT ARRAY[CAST(COALESCE(CAST('2020-01-02 03:04:05.123456+02' AS timestamptz(3)),CAST(NULL AS timestamptz(3))) AS timestamptz(3))] AS v",1185,3},
        {"SELECT CAST('2020-01-02 03:04:05.123456+02' AS timestamptz(3)) AS v",1184,3},
        {"SELECT ARRAY[CAST('12.123456 seconds' AS interval second(3))] AS v",1187,268435459},
        {"SELECT ARRAY[CAST('12.123456 seconds' AS interval second(3)),CAST('-3.654321 seconds' AS interval second(3))] AS v",1187,268435459},
        {"SELECT ARRAY[CAST('12.123456 seconds' AS interval second(3)),CAST('-3.654321 seconds' AS interval second(1))] AS v",1187,-1},
        {"SELECT ARRAY[CAST('12.123456 seconds' AS interval second(3)),CAST(NULL AS interval second(3))] AS v",1187,268435459},
        {"SELECT ARRAY[CAST('12.123456 seconds' AS interval second(3)),NULL] AS v",1187,-1},
        {"SELECT ARRAY[ARRAY[CAST('12.123456 seconds' AS interval second(3))],ARRAY[CAST('-3.654321 seconds' AS interval second(3))]] AS v",1187,268435459},
        {"SELECT ARRAY[ARRAY[CAST('12.123456 seconds' AS interval second(3))],ARRAY[CAST('-3.654321 seconds' AS interval second(1))]] AS v",1187,-1},
        {"SELECT ARRAY[CAST(COALESCE(CAST('12.123456 seconds' AS interval second(3)),CAST(NULL AS interval second(3))) AS interval second(3))] AS v",1187,268435459},
        {"SELECT CAST('12.123456 seconds' AS interval second(3)) AS v",1186,268435459},
        {"SELECT ARRAY[CAST('1 day 03:04:05.123456' AS interval day to second(3))] AS v",1187,470286339},
        {"SELECT ARRAY[CAST('1 day 03:04:05.123456' AS interval day to second(3)),CAST('2 days 04:05:06.654321' AS interval day to second(3))] AS v",1187,470286339},
        {"SELECT ARRAY[CAST('1 day 03:04:05.123456' AS interval day to second(3)),CAST('2 days 04:05:06.654321' AS interval day to second(1))] AS v",1187,-1},
        {"SELECT ARRAY[CAST('1 day 03:04:05.123456' AS interval day to second(3)),CAST(NULL AS interval day to second(3))] AS v",1187,470286339},
        {"SELECT ARRAY[CAST('1 day 03:04:05.123456' AS interval day to second(3)),NULL] AS v",1187,-1},
        {"SELECT ARRAY[ARRAY[CAST('1 day 03:04:05.123456' AS interval day to second(3))],ARRAY[CAST('2 days 04:05:06.654321' AS interval day to second(3))]] AS v",1187,470286339},
        {"SELECT ARRAY[ARRAY[CAST('1 day 03:04:05.123456' AS interval day to second(3))],ARRAY[CAST('2 days 04:05:06.654321' AS interval day to second(1))]] AS v",1187,-1},
        {"SELECT ARRAY[CAST(COALESCE(CAST('1 day 03:04:05.123456' AS interval day to second(3)),CAST(NULL AS interval day to second(3))) AS interval day to second(3))] AS v",1187,470286339},
        {"SELECT CAST('1 day 03:04:05.123456' AS interval day to second(3)) AS v",1186,470286339},
        {"SELECT ARRAY[c] AS v FROM r324_mods ORDER BY id",1014,8},
        {"SELECT ARRAY[ARRAY[c],ARRAY[c]] AS v FROM r324_mods ORDER BY id",1014,8},
        {"SELECT ARRAY[c,NULL] AS v FROM r324_mods ORDER BY id",1014,-1},
        {"SELECT ARRAY[v] AS v FROM r324_mods ORDER BY id",1015,10},
        {"SELECT ARRAY[ARRAY[v],ARRAY[v]] AS v FROM r324_mods ORDER BY id",1015,10},
        {"SELECT ARRAY[v,NULL] AS v FROM r324_mods ORDER BY id",1015,-1},
        {"SELECT ARRAY[b] AS v FROM r324_mods ORDER BY id",1561,4},
        {"SELECT ARRAY[ARRAY[b],ARRAY[b]] AS v FROM r324_mods ORDER BY id",1561,4},
        {"SELECT ARRAY[b,NULL] AS v FROM r324_mods ORDER BY id",1561,-1},
        {"SELECT ARRAY[bv] AS v FROM r324_mods ORDER BY id",1563,4},
        {"SELECT ARRAY[ARRAY[bv],ARRAY[bv]] AS v FROM r324_mods ORDER BY id",1563,4},
        {"SELECT ARRAY[bv,NULL] AS v FROM r324_mods ORDER BY id",1563,-1},
        {"SELECT ARRAY[n] AS v FROM r324_mods ORDER BY id",1231,393222},
        {"SELECT ARRAY[ARRAY[n],ARRAY[n]] AS v FROM r324_mods ORDER BY id",1231,393222},
        {"SELECT ARRAY[n,NULL] AS v FROM r324_mods ORDER BY id",1231,-1},
        {"SELECT ARRAY[ts] AS v FROM r324_mods ORDER BY id",1115,3},
        {"SELECT ARRAY[ARRAY[ts],ARRAY[ts]] AS v FROM r324_mods ORDER BY id",1115,3},
        {"SELECT ARRAY[ts,NULL] AS v FROM r324_mods ORDER BY id",1115,-1},
        {"SELECT ARRAY[tm] AS v FROM r324_mods ORDER BY id",1183,3},
        {"SELECT ARRAY[ARRAY[tm],ARRAY[tm]] AS v FROM r324_mods ORDER BY id",1183,3},
        {"SELECT ARRAY[tm,NULL] AS v FROM r324_mods ORDER BY id",1183,-1},
        {"SELECT CAST(ARRAY[] AS character(4)[]) AS v",1014,8},
        {"SELECT CAST(ARRAY[] AS numeric(6,2)[]) AS v",1231,393222},
        {"SELECT ARRAY[CHAR 'abc '] AS v",1014,-1},
        {"SELECT ARRAY[CHARACTER 'abc '] AS v",1014,-1},
        {"SELECT ARRAY[BIT '1010'] AS v",1561,-1},
        {"SELECT ARRAY[B'1010'] AS v",1561,-1},
        {"SELECT ARRAY[CHARACTER(3) 'abc'] AS v",1014,7},
        {"SELECT ARRAY[BIT(3) '101010'] AS v",1561,3},
        {"SELECT ARRAY[ARRAY[CHARACTER 'abc '],ARRAY[CHARACTER 'xy']] AS v",1014,-1},
        {"SELECT ARRAY[ARRAY[BIT '1010'],ARRAY[BIT '11']] AS v",1561,-1},
        {"SELECT ARRAY[CHARACTER 'abc ',CAST('x' AS character(4))] AS v",1014,-1},
        {"SELECT ARRAY[BIT '1010',B'11'] AS v",1561,-1},
        {"SELECT ARRAY[CAST('abc' AS character)] AS v",1014,5},
        {"SELECT ARRAY[B'1010'::bit] AS v",1561,1},
    };
    for(const auto& item:cases) {
        try {
            const auto query=g_engine.prepareBoundQuery(database,item.sql);
            check(query.output.size()==1 && query.output[0].name=="v",item.sql+" output count/name");
            const auto oid=query.output.empty()?0:query.output[0].typeOid;
            const auto modifier=query.output.empty()?-999:query.output[0].typeMod;
            check(oid==item.oid,item.sql+" physical OID="+std::to_string(oid));
            check(modifier==item.modifier,item.sql+" typmod="+std::to_string(modifier)+" expected="+std::to_string(item.modifier));
        } catch(const std::exception& error) {
            check(false,item.sql+" exception="+error.what());check(false,item.sql+" OID unavailable");check(false,item.sql+" typmod unavailable");
        }
    }
    // Descriptor inference must not run a value expression. Execution of
    // this deliberately invalid division is outside this metadata test.
    try {
        const auto query=g_engine.prepareBoundQuery(database,"SELECT ARRAY[CAST(1/0 AS numeric(6,2))] AS v");
        check(query.output.size()==1,"metadata-only division output count");
        check(query.output.size()==1 && query.output[0].typeOid==1231,"metadata-only division array OID");
        check(query.output.size()==1 && query.output[0].typeMod==393222,"metadata-only division declared modifier");
    } catch(const std::exception& error) {
        check(false,std::string("metadata-only division executed: ")+error.what());check(false,"metadata-only OID unavailable");check(false,"metadata-only modifier unavailable");
    }
    setCurrentSession(nullptr);cleanupTestDb(name);
    std::cout<<"ARRAY_CONSTRUCTOR_TYPMOD_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
