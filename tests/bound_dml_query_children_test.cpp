#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/DmlExecutor.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    using SqlCell = StorageEngine::SqlCell;
    TypeRegistry::instance().bootstrap();
    const std::string name="bound_dml_query_children",db=testDbPath(name);
    cleanupTestDb(name);
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    Session session;session.username="admin";session.permission=1;session.currentDB=db;
    auto* previous=currentSession();setCurrentSession(&session);
    struct Restore {Session* value;~Restore(){setCurrentSession(value);}} restore{previous};
    DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE target(id INT PRIMARY KEY,v INT,t TEXT)",session));
    assert(!ddl.executeSql("CREATE TABLE source(id INT)",session));
    for(int n:{1,2,3})assert(g_engine.insertRow(db,"target",{{"id",std::to_string(n)},{"v",std::to_string(n*10)},{"t",n==1?SqlCell{}:SqlCell{n==2?"":"NULL"}}})==DBStatus::OK);
    for(int n:{2,3})assert(g_engine.insertRow(db,"source",{{"id",std::to_string(n)}})==DBStatus::OK);
    size_t failures=0,creates=0,reads=0,closes=0,legacy=0;
    const auto execute=[&](const std::string& sql) {
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,sql));
        auto* envelope=dynamic_cast<WithStmt*>(query->ast.get());
        Stmt* statement=envelope?envelope->statement.get():query->ast.get();
        PreparedChildCursorFactory factory=[&,query](const Stmt* child,const RowContext& row) {
            ++creates;
            class Cursor final:public PreparedQueryCursor {
                std::unique_ptr<PreparedQueryCursor> cursor_;
                size_t &reads_,&closes_;
            public:
                Cursor(std::unique_ptr<PreparedQueryCursor> cursor,size_t& reads,size_t& closes)
                    :cursor_(std::move(cursor)),reads_(reads),closes_(closes){}
                const QueryRowDescriptor& descriptor()const override{return cursor_->descriptor();}
                bool next(std::vector<ExprValue>& values)override{++reads_;return cursor_->next(values);}
                void close()override{++closes_;cursor_->close();}
                bool supportsRestart()const override{return cursor_->supportsRestart();}
                void restart(const RowContext& row)override{cursor_->restart(row);}
                Operator* plan()const override{return cursor_->plan();}
            };
            auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,child,row);
            return std::unique_ptr<PreparedQueryCursor>(std::make_unique<Cursor>(
                QueryPlanner::makePreparedCursor(std::move(plan),query->statementOutputs.at(child)),reads,closes));
        };
        PreparedChildExecutor forbidden=[&](const Stmt*,const RowContext&,size_t)->PreparedQueryRows{
            ++legacy;throw DbError("XX000","paired cursor was bypassed by fullRows reader");
        };
        prepareBoundDml(statement,session,query,{},factory,true);
        return executeAtomicDmlUnit(session,[&]{return executeBoundDml(statement,session,query,forbidden,{},factory,true);});
    };
    for(const auto& sql:std::vector<std::string>{
        "UPDATE target SET v=v WHERE id=ANY(SELECT id FROM source) RETURNING id,t",
        "WITH hint AS(SELECT 1) UPDATE target AS d SET v=(SELECT d.v) WHERE d.id=ANY(SELECT r.id FROM source AS r WHERE r.id=d.id) RETURNING id,t",
        "DELETE FROM target AS d WHERE d.id=ALL(SELECT id FROM source) RETURNING id",
        "WITH hint AS(SELECT 1) DELETE FROM target WHERE id=ANY(SELECT NULL::INT) RETURNING id",
    }) {
        std::string state;
        try {
            auto result=execute(sql);
            if(sql.find("UPDATE")!=std::string::npos) {
                assert(result.commandTag=="UPDATE 2");
                assert((result.rows==std::vector<std::vector<std::string>>{{"2",""},{"3","NULL"}}));
                assert((result.nulls==std::vector<std::vector<bool>>{{false,false},{false,false}}));
            } else assert(result.commandTag=="DELETE 0" && result.rows.empty());
        } catch(const DbError& error){state=error.sqlState();}
        std::cout<<"BOUND CHILD NATIVE "<<sql<<" state="<<state<<std::endl;
        if(!state.empty())++failures;
    }
    for(const auto& control:std::vector<std::pair<std::string,std::string>>{
        {"UPDATE target SET v=1/0 WHERE false","22012"},
        {"UPDATE target SET v=(SELECT 1/0) WHERE false","22012"},
        {"DELETE FROM target WHERE id=ANY(SELECT CAST('bad' AS INT))","22P02"},
        {"DELETE FROM target WHERE false AND id=ANY(SELECT missing_column)","42703"},
        {"UPDATE target SET v=(SELECT id FROM source) WHERE id=ANY(SELECT 2)","21000"},
        {"UPDATE target SET v=1/0,v=2 WHERE id=ANY(SELECT 2)","42601"},
    }) {
        std::string state;try{(void)execute(control.first);}catch(const DbError& error){state=error.sqlState();}
        std::cout<<"BOUND CHILD NATIVE ERROR "<<control.first<<" actual="<<state<<" expected="<<control.second<<std::endl;
        if(state!=control.second)++failures;
    }
    assert(!g_engine.inTransaction());
    // Legacy source-compatible callers without a paired provider retain
    // lazy default preparation: a genuinely false constant qualification
    // cannot acquire an unsupported child it never evaluates.
    {
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,
            "DELETE FROM target WHERE false AND 1=ANY(SELECT 1)"));
        auto result=executeAtomicDmlUnit(session,[&]{return executeBoundDml(query->ast.get(),session,query,{});});
        assert(result.commandTag=="DELETE 0" && result.rows.empty());
    }
    std::cout<<"BOUND CHILD NATIVE failures="<<failures<<" creates="<<creates<<" reads="<<reads<<" closes="<<closes<<" legacy="<<legacy<<std::endl;
    assert(failures==0 && creates>0 && reads>0 && closes>0 && legacy==0);
    assert(g_engine.getLockManager().captureCheckpoint().tableCounts.empty());
    assert(g_engine.dropDatabase(db)==DBStatus::OK);cleanupTestDb(name);
    std::cout<<"[BOUND DML QUERY CHILDREN] passed\n";
}
