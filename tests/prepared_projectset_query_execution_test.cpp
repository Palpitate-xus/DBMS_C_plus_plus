#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    StorageEngine engine;
    const std::string name="prepared_projectset_query_execution",db=testDbPath(name);
    cleanupTestDb(name);
    assert(engine.createDatabase(db)==DBStatus::OK);
    TableSchema schema;schema.len=1;schema.cols[0].dataName="id";schema.cols[0].dataType="int";
    assert(engine.createTable(db,"source",schema)==DBStatus::OK);
    assert(engine.insertRow(db,"source",{{"id","1"}})==DBStatus::OK);
    assert(engine.insertRow(db,"source",{{"id","2"}})==DBStatus::OK);
    assert(engine.createSequence(db,"effects")==DBStatus::OK);
    size_t childNext=0;
    const auto plan=[&](const std::string& sql,bool root=true) {
        auto query=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,sql));
        auto* select=dynamic_cast<SelectStmt*>(query->ast.get());assert(select);
        PreparedChildCursorFactory factory=[&,query](const Stmt* child,const RowContext& outer) {
            auto actual=QueryPlanner::buildPreparedQueryPlan(&engine,db,query,child,outer);
            class Cursor final : public PreparedQueryCursor {
                std::unique_ptr<PreparedQueryCursor> actual_;
                size_t& next_;
            public:
                Cursor(std::unique_ptr<PreparedQueryCursor> actual,size_t& next):actual_(std::move(actual)),next_(next){}
                const QueryRowDescriptor& descriptor() const override {return actual_->descriptor();}
                bool next(std::vector<ExprValue>& row) override {++next_;return actual_->next(row);}
                void close() override {actual_->close();}
                Operator* plan() const override {return actual_->plan();}
                bool supportsRestart() const override {return actual_->supportsRestart();}
                void restart(const RowContext& row) override {actual_->restart(row);}
            };
            return std::unique_ptr<PreparedQueryCursor>(std::make_unique<Cursor>(
                QueryPlanner::makePreparedCursor(std::move(actual),query->statementOutputs.at(child)),childNext));
        };
        return QueryPlanner::buildPreparedSetReturningPlan(&engine,db,query,select,{}, {},factory,root);
    };
    const auto execute=[&](const std::string& sql,std::vector<std::vector<std::string>> rows,
                          std::vector<std::vector<bool>> nulls) {
        auto tree=plan(sql);assert(tree);
        auto result=QueryPlanner::executePlanChecked(std::move(tree));result.throwIfFailed();
        std::cout<<"PROJECTSET NATIVE "<<sql<<" structured "<<result.structuredRowsAvailable<<std::endl;
        for(size_t row=0;row<result.structuredRows.size();++row)
            for(size_t column=0;column<result.structuredRows[row].size();++column)
                std::cout<<" cell["<<row<<","<<column<<"]="<<result.structuredRows[row][column]
                         <<" null="<<result.structuredNulls[row][column]<<std::endl;
        assert(result.structuredRowsAvailable && result.structuredRows==rows && result.structuredNulls==nulls);
    };
    execute("SELECT unnest(ARRAY[1,NULL,2]) WHERE 1=ANY(SELECT 1)",
            {{"1"},{"NULL"},{"2"}},{{false},{true},{false}});
    assert(childNext>0);
    childNext=0;
    execute("SELECT unnest(ARRAY[nextval('effects')]) WHERE 1=ANY(SELECT 1) LIMIT 0",{},{});
    assert(childNext==0 && engine.nextval(db,"effects")==1);
    execute("SELECT unnest(ARRAY[nextval('effects')]) WHERE 1=ANY(SELECT 0)",{},{});
    assert(engine.nextval(db,"effects")==2);
    execute("SELECT unnest(ARRAY[(SELECT 3)]) WHERE 1=ANY(SELECT 1)",{{"3"}},{{false}});
    execute("SELECT unnest(ARRAY['NULL',NULL,'',' a b ']) WHERE 1=ANY(SELECT 1)",
            {{"NULL"},{"NULL"},{""},{" a b "}},{{false},{true},{false},{false}});
    for(const auto& control:std::vector<std::pair<std::string,std::string>>{
        {"SELECT unnest(ARRAY[-INTERVAL '-2147483648 months']) WHERE 1=ANY(SELECT 1) LIMIT 0","22008"},
        {"SELECT unnest(ARRAY[1/0]) WHERE 1=ANY(SELECT nextval('effects'))","22012"},
        {"SELECT unnest(ARRAY[(SELECT id FROM source)]) WHERE 1=ANY(SELECT 1)","21000"},
    }) {
        std::string state;
        try {auto tree=plan(control.first);auto result=QueryPlanner::executePlanChecked(std::move(tree));
             assert(result.rows.empty());result.throwIfFailed();}
        catch(const DbError& error){state=error.sqlState();}
        std::cout<<control.first<<" actual "<<state<<" expected "<<control.second<<std::endl;
        assert(state==control.second);
    }
    assert(engine.nextval(db,"effects")==3);
    assert(engine.dropDatabase(db)==DBStatus::OK);
    cleanupTestDb(name);
    std::cout<<"[PREPARED PROJECTSET] passed\n";
}
