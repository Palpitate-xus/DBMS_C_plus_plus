#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto db=testDbPath("integer_unknown_preparation");
    assert(g_engine.createDatabase(db)==DBStatus::OK);
    auto* previous=currentSession();
    struct Restore {Session* previous;~Restore(){setCurrentSession(previous);}} restore{previous};
    Session session;session.username="testuser";session.permission=1;session.currentDB=db;
    setCurrentSession(&session);DdlExecutor ddl;
    assert(!ddl.executeSql("CREATE TABLE bits (id INT PRIMARY KEY,i INT,j INT,b BIGINT,\"odd\"\"i\" INT)",session));
    assert(!ddl.executeSql("CREATE INDEX bits_i ON bits(i)",session));
    assert(!ddl.executeSql("CREATE SEQUENCE effects",session));
    const std::vector<std::string> columns={"id","i","j","b","\"odd\"\"i\""};
    const std::vector<std::string> operators={"=","<>","<",">","<=",">="};
    size_t controls=0;
    const auto error=[&](const std::string& sql,const std::string& expected) {
        ++controls;std::string state;
        try {(void)g_engine.prepareBoundQuery(db,sql);}
        catch(const DbError& exception){state=exception.sqlState();}
        std::cout<<"INTEGER_UNKNOWN_NATIVE "<<sql<<" actual="<<state<<" expected="<<expected<<'\n';
        assert(state==expected);
    };
    for(bool populated:{false,true}) {
        if(populated) {
            assert(g_engine.insertRow(db,"bits",{{"id","1"},{"i","1"},{"j","1"},{"b","1"},{"odd\"i","1"}})==DBStatus::OK);
            assert(g_engine.insertRow(db,"bits",{{"id","2"},{"i","2"},{"j","2"},{"b","2"},{"odd\"i","2"}})==DBStatus::OK);
            assert(g_engine.insertRow(db,"bits",{{"id","3"},{"i",std::nullopt},{"j",std::nullopt},{"b",std::nullopt},{"odd\"i",std::nullopt}})==DBStatus::OK);
        }
        for(const auto& column:columns)for(const auto& op:operators)for(bool reversed:{false,true}) {
            const auto predicate=reversed?"''"+op+column:column+op+"''";
            error("SELECT id FROM bits WHERE "+predicate+" ORDER BY id","22P02");
            // Output and lazy/demand boundaries share the same immutable
            // source descriptor and input transformation, not sample rows.
            error("SELECT "+predicate+" FROM bits WHERE FALSE LIMIT 0","22P02");
        }
        for(const auto& input:std::vector<std::pair<std::string,std::string>>{
            {"i='x'","22P02"},{"i='NULL'","22P02"},{"i='1.0'","22P02"},{"b='9e2'","22P02"},{"i='it''s'","22P02"},
            {"i='2147483648'","22003"},{"b='9223372036854775808'","22003"}})
            error("SELECT id FROM bits WHERE "+input.first+" LIMIT 0",input.second);
        for(const auto& column:columns) {
            ++controls;
            auto prepared=g_engine.prepareBoundQuery(db,"SELECT id FROM bits WHERE "+column+"='1' ORDER BY id");
            const auto* select=static_cast<const SelectStmt*>(prepared.ast.get());
            const auto* binary=dynamic_cast<const BinaryOpExpr*>(select->whereClause.get());
            const auto* constant=binary?dynamic_cast<const LiteralExpr*>(binary->right.get()):nullptr;
            assert(constant && constant->value=="1" && constant->typeName==(column=="b"?"bigint":"integer"));
            assert(constant->sourceBegin!=std::string::npos && constant->sourceEnd>constant->sourceBegin);
            assert(prepared.source.substr(constant->sourceBegin,constant->sourceEnd-constant->sourceBegin)=="'1'");
            const auto* source=dynamic_cast<const ColumnRefExpr*>(binary->left.get());
            assert(source && source->binding && source->binding->sourceOrdinal==prepared.sourceRanges.front().ordinal);
            assert(source->binding->typeOid==(column=="b"?20u:23u));
            assert(prepared.output.size()==1 && prepared.output[0].name=="id" && prepared.output[0].type=="integer");
            auto rows=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(&g_engine,db,"bits",std::move(prepared)));
            rows.throwIfFailed();assert(rows.structuredRowsAvailable);
            assert(rows.structuredRows==(populated?std::vector<std::vector<std::string>>{{"1"}}:std::vector<std::vector<std::string>>{}));
            ++controls;
            auto null=g_engine.prepareBoundQuery(db,"SELECT id FROM bits WHERE "+column+"=NULL ORDER BY id");
            auto none=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(&g_engine,db,"bits",std::move(null)));
            none.throwIfFailed();assert(none.structuredRows.empty());
        }
    }
    error("SELECT nextval('effects') FROM bits WHERE i='' LIMIT 0","22P02");
    assert(g_engine.nextval(db,"effects")==1);
    // A true bound UNKNOWN parameter is not an SQL string literal. Its bad
    // current value cannot be evaluated to discover/pre-transform metadata.
    auto dynamic=g_engine.prepareBoundQuery(db,"SELECT id FROM bits WHERE i=$1",
        {{"integer-input-param","","unknown",{},true,std::string("bad"),1}});
    auto* select=static_cast<SelectStmt*>(dynamic.ast.get());
    auto* binary=dynamic_cast<BinaryOpExpr*>(select->whereClause.get());
    assert(binary && dynamic_cast<ParameterExpr*>(binary->right.get()));
    assert(dynamic.parameters.size()==1 && dynamic.parameters.front().value=="bad");
    // Typed expressions and actual volatile function occurrences remain
    // executable sites. Only the opposing genuine UNKNOWN literal converts.
    auto volatileQuery=g_engine.prepareBoundQuery(db,"SELECT id FROM bits WHERE nextval('effects')='1'");
    select=static_cast<SelectStmt*>(volatileQuery.ast.get());
    binary=dynamic_cast<BinaryOpExpr*>(select->whereClause.get());
    assert(binary && dynamic_cast<FunctionCallExpr*>(binary->left.get()) && dynamic_cast<LiteralExpr*>(binary->right.get()));
    assert(g_engine.nextval(db,"effects")==2);
    auto typed=g_engine.prepareBoundQuery(db,"SELECT id FROM bits WHERE i=''::TEXT LIMIT 0");
    select=static_cast<SelectStmt*>(typed.ast.get());
    binary=dynamic_cast<BinaryOpExpr*>(select->whereClause.get());
    assert(binary && dynamic_cast<BinaryOpExpr*>(binary->right.get())); // original explicit ::TEXT, not integer recast
    assert(!ddl.executeSql("CREATE TABLE exact_bigints(id INT,b BIGINT)",session));
    assert(g_engine.insertRow(db,"exact_bigints",{{"id","1"},{"b","9007199254740992"}})==DBStatus::OK);
    assert(g_engine.insertRow(db,"exact_bigints",{{"id","2"},{"b","9007199254740993"}})==DBStatus::OK);
    for(const auto& sql:{"SELECT id FROM exact_bigints WHERE b='9007199254740993'",
                        "SELECT id FROM exact_bigints WHERE '9007199254740993'=b"}) {
        auto prepared=g_engine.prepareBoundQuery(db,sql);
        auto rows=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(&g_engine,db,"exact_bigints",std::move(prepared)));
        rows.throwIfFailed();assert(rows.structuredRows==std::vector<std::vector<std::string>>{{"2"}});
    }
    std::cout<<"[INTEGER UNKNOWN PREPARATION] complete "<<controls<<" real source/PK/index/quoted/NULL/empty/public planner/parameter/no-effects controls passed\n";
}
