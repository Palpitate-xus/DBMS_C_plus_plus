#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("aggregate_argument_expression");
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="testuser";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    setCurrentSession(&session);DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE args (id INT,n INT,t TEXT,b BOOLEAN)",session));
    assert(!ddl.executeSql("CREATE TABLE empty_args (id INT,n INT,t TEXT,b BOOLEAN)",session));
    for (const auto& values:std::vector<std::vector<std::optional<std::string>>>{
        {"1","1","zeta","true"},{"2","2","","false"},
        {"3",std::nullopt,std::nullopt,std::nullopt},{"4","-1","NULL","true"}}) {
        assert(g_engine.insertRow(db,"args",{{"id",values[0]},{"n",values[1]},{"t",values[2]},{"b",values[3]}})==DBStatus::OK);
    }
    const auto schema=g_engine.getTableSchema(db,"args");
    assert(schema.len==4);
    std::cout<<"AGGREGATE_DEFAULT_TEXT_COLUMN_COLLATION="<<schema.cols[2].collation
             <<" RESOLVED="<<schema.cols[2].resolvedCollation<<std::endl;
    StorageEngine::SelectExpr textColumn;textColumn.colName="t";
    std::vector<std::vector<std::string>> textValues;std::vector<std::vector<bool>> textMissing;
    g_engine.queryExpr(db,"args",{}, {textColumn},{},&textValues,&textMissing);
    assert(textValues.size()==4 && textMissing.size()==4);
    assert(textValues[1][0].empty() && !textMissing[1][0] && textMissing[2][0]);
    assert(textValues[3][0]=="NULL" && !textMissing[3][0]);
    const auto projection=[](const std::string& sql) {
        StorageEngine::SelectExpr value;value.isScalar=true;value.funcName="expreval";value.funcArgs={sql};return value;
    };
    const auto expect=[&](const std::string& source,const std::string& sql,
                          const std::optional<std::string>& value) {
        std::vector<std::vector<std::string>> rows;std::vector<std::vector<bool>> nulls;
        g_engine.queryExpr(db,source,{}, {projection(sql)}, {},&rows,&nulls);
        assert(rows.size()==1 && rows[0].size()==1 && nulls.size()==1 && nulls[0].size()==1);
        assert(nulls[0][0]==!value && (!value || rows[0][0]==*value));
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    };
    for (const auto& control:std::vector<std::pair<std::string,std::string>>{
        {"SUM(CASE WHEN n > 0 THEN 1 ELSE 0 END)","2"},
        // Actual engine default is en_US; an owned 180006 database with the
        // same default proves 1 for this unchanged SQL and original datum set.
        {"SUM(CASE WHEN t < 'alpha' THEN 1 ELSE 0 END)","1"},
        {"SUM(CASE WHEN t COLLATE \"C\" < 'alpha' THEN 1 ELSE 0 END)","2"},
        {"sum(CASE WHEN n IS NULL THEN NULL ELSE n END)","2"},
        {"count(CASE WHEN n IS NULL THEN 1 END)","1"},
        {"sum(n * 2)","4"},{"sum(CAST(n AS BIGINT))","2"},
        {"bool_and(t < 'alpha')","f"},{"every(t <> '')","f"},
        {"sum(DISTINCT CASE WHEN n > 0 THEN 1 ELSE 0 END)","1"},
        {"sum(CASE WHEN n > 0 THEN 1 ELSE 0 END) FILTER (WHERE id <= 2)","2"}})
        expect("args",control.first,control.second);
    expect("empty_args","SUM(CASE WHEN n > 0 THEN 1 ELSE 0 END)",std::nullopt);
    expect("empty_args","bool_and(t < 'alpha')",std::nullopt);
    expect("empty_args","count(CASE WHEN n IS NULL THEN 1 END)","0");
    assert(!ddl.executeSql("CREATE INDEX args_id_idx ON args(id)",session));
    const auto expectBranches=[&](const std::string& source,
            const std::vector<std::vector<std::string>>& branches,
            const std::optional<std::string>& expected) {
        StorageEngine::QueryExprExecutionOptions options;options.conditionAlternatives=branches;
        std::vector<std::vector<std::string>> rows;std::vector<std::vector<bool>> nulls;
        g_engine.queryExpr(db,source,branches.front(),{projection("sum(n+0)")},{},&rows,&nulls,nullptr,options);
        assert(rows.size()==1 && nulls.size()==1 && rows[0].size()==1 && nulls[0].size()==1);
        assert(nulls[0][0]==!expected && (!expected || rows[0][0]==*expected));
        assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    };
    expectBranches("args",{{"=id 1"},{"=id 2"}},"3");
    expectBranches("args",{{"=id 1"},{"=id 1"}},"1");
    expectBranches("args",{{"typedexpr false"},{"=id 99"}},std::nullopt);
    expectBranches("args",{{"typedexpr NULL"},{"=id 1"}},"1");
    expectBranches("args",{{"typedexpr true"},{"=id 99"}},"2");
    expectBranches("args",{{"typedexpr NOT false"}},"2");
    expectBranches("args",{{"typedexpr NOT true"}},std::nullopt);
    expectBranches("args",{{"typedexpr NOT NULL"}},std::nullopt);
    expectBranches("args",{{"typedexpr NOT (false OR id=1)"}},"1");
    expectBranches("args",{{"typedexpr b"}},"0");
    expectBranches("args",{{"typedexpr NOT b"}},"2");
    expectBranches("empty_args",{{"typedexpr true"},{"=id 99"}},std::nullopt);
    assert(!ddl.executeSql("CREATE TABLE argument_effect (v INT)",session));
    assert(g_engine.createUDF(db,"argument_writer",{"i"},{"int"},
        "BEGIN INSERT INTO argument_effect(v) VALUES(i); RETURN i; END;",'v',"plpgsql","int")==DBStatus::OK);
    assert(g_engine.beginTransaction(db)==DBStatus::OK && g_engine.beginSqlCommand());
    expect("args","bool_and(argument_writer(n) < 0)","f");
    assert(g_engine.finishSqlCommand() && g_engine.beginSqlCommand());
    std::vector<std::vector<std::string>> effects;std::vector<std::vector<bool>> missing;
    StorageEngine::SelectExpr column;column.colName="v";
    g_engine.queryExpr(db,"argument_effect",{}, {column},{},&effects,&missing);
    assert(effects.size()==4 && missing.size()==4);
    assert(effects[0][0]=="1" && effects[1][0]=="2" && missing[2][0] && effects[3][0]=="-1");
    assert(g_engine.finishSqlCommand() && g_engine.beginSqlCommand());
    StorageEngine::QueryExprExecutionOptions overlap;
    overlap.conditionAlternatives={{"<=id 2"},{">=id 2"}};
    std::vector<std::vector<std::string>> reduced;std::vector<std::vector<bool>> reducedNulls;
    g_engine.queryExpr(db,"args",overlap.conditionAlternatives.front(),
        {projection("sum(argument_writer(n))")},{},&reduced,&reducedNulls,nullptr,overlap);
    assert(reduced==std::vector<std::vector<std::string>>{{"2"}} && !reducedNulls[0][0]);
    assert(g_engine.finishSqlCommand() && g_engine.beginSqlCommand());
    g_engine.queryExpr(db,"argument_effect",{}, {column},{},&effects,&missing);
    assert(effects.size()==8 && missing.size()==8 && effects[4][0]=="1" &&
        effects[5][0]=="2" && missing[6][0] && effects[7][0]=="-1");
    assert(g_engine.rollbackTransaction()==DBStatus::OK);
    g_engine.queryExpr(db,"argument_effect",{}, {column},{},&effects,&missing);assert(effects.empty());
    std::cout<<"[AGGREGATE ARGUMENT EXPRESSION] real typed reduction/NULL/empty/filter/CASE/volatile no early-stop and rollback passed\n";
}
