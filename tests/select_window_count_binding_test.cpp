#include "Session.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;

// PG18.6 Parse/Describe rejects the current-query OVER cases below before
// ParseComplete or RowDescription. Other SELECT dynamic counts retain their
// existing unsupported parser contract; their legal PG behavior is not
// claimed as implemented. No query graph or source is executed by this test.
int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string fixture="select_window_count_binding";
    const auto database=testDbPath(fixture);cleanupTestDb(fixture);
    if(g_engine.createDatabase(database)!=DBStatus::OK)return 2;
    Session session;session.username="testuser";session.permission=1;session.currentDB=database;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    DdlExecutor ddl;
    if(ddl.executeSql("CREATE TABLE wh_source(id INTEGER,t TEXT)",session))return 2;
    enum class Policy { ErrorAnalysis, UnsupportedDynamic, Primitive };
    struct Case {const char* label;const char* sql;const char* state;Policy policy;};
    const std::vector<Case> cases={
        {"limit-window","SELECT id FROM wh_source LIMIT row_number() OVER()","42P20",Policy::ErrorAnalysis},
        {"limit-actual-scalar-role","SELECT id FROM wh_source LIMIT upper(t) OVER()","42809",Policy::ErrorAnalysis},
        {"limit-nested-scalar-wrapper","SELECT id FROM wh_source LIMIT abs(row_number() OVER())","42P20",Policy::ErrorAnalysis},
        {"limit-dead-case","SELECT id FROM wh_source LIMIT CASE WHEN false THEN row_number() OVER() ELSE 1 END","42P20",Policy::ErrorAnalysis},
        {"limit-integer-prefix","SELECT id FROM wh_source LIMIT 1+row_number() OVER()","42P20",Policy::ErrorAnalysis},
        {"limit-null-prefix","SELECT id FROM wh_source LIMIT NULL+row_number() OVER()","42P20",Policy::ErrorAnalysis},
        {"limit-cast","SELECT id FROM wh_source LIMIT (row_number() OVER())::bigint","42P20",Policy::ErrorAnalysis},
        {"limit-array-subscript","SELECT id FROM wh_source LIMIT (ARRAY[row_number() OVER()])[1]","42P20",Policy::ErrorAnalysis},
        {"limit-record","SELECT id FROM wh_source LIMIT ROW(row_number() OVER())","42P20",Policy::ErrorAnalysis},
        {"limit-quantified","SELECT id FROM wh_source LIMIT (row_number() OVER()=ANY(ARRAY[1]))","42P20",Policy::ErrorAnalysis},
        {"offset-window","SELECT id FROM wh_source OFFSET row_number() OVER()","42P20",Policy::ErrorAnalysis},
        {"offset-scalar-role","SELECT id FROM wh_source OFFSET upper(t) OVER()","42809",Policy::ErrorAnalysis},
        {"offset-wrapper-rows","SELECT id FROM wh_source OFFSET abs(row_number() OVER()) ROWS","42P20",Policy::ErrorAnalysis},
        {"offset-dead-case","SELECT id FROM wh_source OFFSET CASE WHEN false THEN row_number() OVER() ELSE 1 END","42P20",Policy::ErrorAnalysis},
        {"fetch-window","SELECT id FROM wh_source FETCH FIRST row_number() OVER() ROWS ONLY","42P20",Policy::ErrorAnalysis},
        {"fetch-parenthesized-wrapper","SELECT id FROM wh_source FETCH FIRST (abs(row_number() OVER())) ROWS ONLY","42P20",Policy::ErrorAnalysis},
        {"fetch-scalar-role","SELECT id FROM wh_source FETCH NEXT (upper(t) OVER()) ROWS ONLY","42809",Policy::ErrorAnalysis},
        {"fetch-dead-case","SELECT id FROM wh_source FETCH FIRST (CASE WHEN false THEN row_number() OVER() ELSE 1 END) ROWS ONLY","42P20",Policy::ErrorAnalysis},
        {"limit-pure-expression","SELECT id FROM wh_source LIMIT (1+0)","",Policy::UnsupportedDynamic},
        {"limit-scalar-without-over","SELECT id FROM wh_source LIMIT abs(1)","",Policy::UnsupportedDynamic},
        {"limit-string-no-false-positive","SELECT id FROM wh_source LIMIT length('row_number() OVER()')","",Policy::UnsupportedDynamic},
        {"limit-window-child","SELECT id FROM wh_source LIMIT (SELECT row_number() OVER())","",Policy::UnsupportedDynamic},
        {"offset-window-child","SELECT id FROM wh_source OFFSET (SELECT row_number() OVER())","",Policy::UnsupportedDynamic},
        {"fetch-window-child","SELECT id FROM wh_source FETCH FIRST (SELECT row_number() OVER()) ROWS ONLY","",Policy::UnsupportedDynamic},
        {"limit-literal","SELECT id FROM wh_source LIMIT 2","",Policy::Primitive},
        {"offset-literal","SELECT id FROM wh_source OFFSET 1","",Policy::Primitive},
        {"fetch-literal","SELECT id FROM wh_source FETCH FIRST 2 ROWS ONLY","",Policy::Primitive},
        {"parenthesized-select-current-window","(SELECT id FROM wh_source) LIMIT row_number() OVER()","42P20",Policy::ErrorAnalysis},
        {"parenthesized-select-current-offset","(SELECT id FROM wh_source) OFFSET upper(t) OVER()","42809",Policy::ErrorAnalysis},
        {"parenthesized-select-current-fetch","(SELECT id FROM wh_source) FETCH FIRST (row_number() OVER()) ROWS ONLY","42P20",Policy::ErrorAnalysis},
    };
    size_t controls=0,failures=0;
    SQLParser parser;
    for(const auto& item:cases) {
        ++controls;
        bool good=true,bound=false;
        std::string state,error;
        const auto plain=parser.parse(item.sql);
        const auto binding=parser.parseForBinding(item.sql);
        const auto* ordinary=dynamic_cast<const SelectStmt*>(plain.stmt.get());
        const auto* select=dynamic_cast<const SelectStmt*>(binding.stmt.get());
        const Expr* count=select?(select->limitExpr?select->limitExpr.get():select->offsetExpr.get()):nullptr;
        const Expr* ordinaryCount=ordinary?(ordinary->limitExpr?ordinary->limitExpr.get():ordinary->offsetExpr.get()):nullptr;
        if(item.policy==Policy::ErrorAnalysis) {
            good=plain.isValid() && binding.isValid() && ordinary && select && count && ordinaryCount &&
                select->hasRowCountClause && ordinary->hasRowCountClause &&
                count->sourceBegin!=std::string::npos && count->sourceBegin<count->sourceEnd &&
                count->sourceEnd<=std::string(item.sql).size();
            const std::string sql=item.sql;
            const size_t clause=sql.find("LIMIT")!=std::string::npos?sql.find("LIMIT"):
                sql.find("OFFSET")!=std::string::npos?sql.find("OFFSET"):sql.find("FETCH");
            good=good && clause!=std::string::npos && count && count->sourceBegin>clause;
        } else if(item.policy==Policy::UnsupportedDynamic) {
            good=!plain.isValid() && !binding.isValid() && !ordinaryCount && !count;
        } else {
            good=plain.isValid() && binding.isValid() && ordinary && select && !ordinaryCount && !count;
        }
        try {
            const auto query=g_engine.prepareBoundQuery(database,item.sql,{},true);bound=true;
            good=good && item.policy==Policy::Primitive && query.output.size()==1 &&
                query.output.front().type=="integer" && query.output.front().typeOid==23;
        } catch(const DbError& failure) {state=failure.sqlState();error=failure.what();}
        if(item.policy==Policy::ErrorAnalysis)good=good && !bound && state==item.state;
        else if(item.policy==Policy::UnsupportedDynamic)good=good && !bound && state=="42601";
        else good=good && bound && state.empty();
        if(!good)++failures;
        std::cout<<"SELECT_WINDOW_COUNT_BINDING "<<controls<<' '<<item.label<<" good="<<good
            <<" plain_valid="<<plain.isValid()<<" binding_valid="<<binding.isValid()
            <<" count="<<(count!=nullptr)<<" bound="<<bound<<" state="<<std::quoted(state)
            <<" parse_error="<<std::quoted(binding.error)<<" bind_error="<<std::quoted(error)<<'\n';
    }
    cleanupTestDb(fixture);finalCleanupTestData();
    constexpr size_t expectedControls=30;
    if(controls!=expectedControls)++failures;
    std::cout<<"SELECT_WINDOW_COUNT_BINDING_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
