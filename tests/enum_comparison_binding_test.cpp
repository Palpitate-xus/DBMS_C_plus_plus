#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/prepared_query_execution.h"
#include "executor/ExecutionPlan.h"
#include "Session.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto dbA=testDbPath("enum_comparison_binding_a");
    const auto dbB=testDbPath("enum_comparison_binding_b");
    const std::vector<std::string> labelsA{"zeta","","alpha","NULL","it's","longlonglong"};
    const std::vector<std::string> labelsB{"alpha","","zeta","NULL","it's","longlonglong"};
    auto* previous=currentSession();
    struct Restore {Session* session;~Restore(){setCurrentSession(session);}} restore{previous};
    Session session;session.username="testuser";session.permission=1;
    DdlExecutor ddl;
    for(const auto& db:{dbA,dbB}) {
        assert(g_engine.createDatabase(db)==DBStatus::OK);
        session.currentDB=db;setCurrentSession(&session);
        const auto sql=db==dbA?"CREATE TYPE rank_type AS ENUM ('zeta','','alpha','NULL','it''s','longlonglong')":
                              "CREATE TYPE rank_type AS ENUM ('alpha','','zeta','NULL','it''s','longlonglong')";
        assert(!ddl.executeSql(sql,session));
        assert(!ddl.executeSql("CREATE TABLE ranks (id INT,r rank_type)",session));
        assert(g_engine.insertRow(db,"ranks",{{"id","3"},{"r","alpha"}})==DBStatus::OK);
        assert(g_engine.insertRow(db,"ranks",{{"id","1"},{"r","zeta"}})==DBStatus::OK);
        assert(g_engine.insertRow(db,"ranks",{{"id","2"},{"r",""}})==DBStatus::OK);
    }
    const auto bind=[&](StorageEngine& engine,const std::string& db) {
        auto prepared=std::make_shared<PreparedQuery>(engine.prepareBoundQuery(db,
            "SELECT r < 'alpha',CASE r WHEN 'zeta' THEN 1 WHEN '' THEN 2 WHEN NULL THEN 99 ELSE 3 END FROM ranks"));
        auto* select=dynamic_cast<SelectStmt*>(prepared->ast.get());assert(select);
        const auto* comparison=dynamic_cast<const BinaryOpExpr*>(select->selectList[0].expr.get());
        assert(comparison && comparison->comparison && comparison->comparison->enumTypeOid);
        assert(comparison->comparison->enumLabels==(db==dbA?labelsA:labelsB));
        const auto* conditional=dynamic_cast<const CaseExpr*>(select->selectList[1].expr.get());
        assert(conditional && conditional->simpleEnumComparisons.size()==3);
        for(const auto& binding:conditional->simpleEnumComparisons)
            assert(binding && binding->enumTypeOid==comparison->comparison->enumTypeOid &&
                   binding->enumLabels==comparison->comparison->enumLabels);
        return prepared;
    };
    // The ambient session points at B. A's explicit database plus physical
    // source OID must still produce A's metadata, never a global enum lookup.
    session.currentDB=dbB;
    auto preparedA=bind(g_engine,dbA),preparedB=bind(g_engine,dbB);
    const auto* selectA=dynamic_cast<const SelectStmt*>(preparedA->ast.get());
    const auto* selectB=dynamic_cast<const SelectStmt*>(preparedB->ast.get());
    const auto* binaryA=dynamic_cast<const BinaryOpExpr*>(selectA->selectList[0].expr.get());
    const auto* binaryB=dynamic_cast<const BinaryOpExpr*>(selectB->selectList[0].expr.get());
    assert(binaryA->comparison->identity!=binaryB->comparison->identity);
    const auto check=[&](StorageEngine& engine,const std::string& db,const std::shared_ptr<PreparedQuery>& prepared) {
        auto* select=dynamic_cast<SelectStmt*>(prepared->ast.get());assert(select);
        PreparedQueryExecution execution(prepared,&engine,db);
        for(auto& item:select->selectList)execution.prepareExpression(item.expr.get());
        execution.planStatementConstants(select);
        const auto& labels=db==dbA?labelsA:labelsB;
        const auto alpha=std::find(labels.begin(),labels.end(),"alpha")-labels.begin();
        for(size_t i=0;i<=labels.size();++i) {
            auto row=execution.context();
            const bool null=i==labels.size();
            execution.setSourceRow(row,prepared->sourceRanges.front().ordinal,
                {{"integer",std::to_string(i+1),false},{"rank_type",null?"":labels[i],null}});
            const auto less=execution.evaluate(select->selectList[0].expr.get(),row);
            assert(less.typeName=="boolean" && less.isNull==null);
            if(!null)assert(less.value==(static_cast<ptrdiff_t>(i)<alpha?"t":"f"));
            const auto simple=execution.evaluate(select->selectList[1].expr.get(),row);
            assert(!simple.isNull && simple.typeName=="integer");
            assert(simple.value==(!null && labels[i]=="zeta"?"1":!null && labels[i].empty()?"2":"3"));
        }
    };
    check(g_engine,dbA,preparedA);check(g_engine,dbB,preparedB);check(g_engine,dbA,preparedA);
    for(const auto& db:{dbA,dbB}) {
        auto sorted=g_engine.prepareBoundQuery(db,"SELECT id,r < 'alpha' FROM ranks ORDER BY r");
        auto* select=dynamic_cast<SelectStmt*>(sorted.ast.get());assert(select);
        assert(select->orderBy.front().enumComparison &&
               select->orderBy.front().enumComparison->enumLabels==(db==dbA?labelsA:labelsB));
        auto result=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(&g_engine,db,"ranks",std::move(sorted)));
        result.throwIfFailed();assert(result.structuredRowsAvailable);
        assert((result.structuredRows==(db==dbA?std::vector<std::vector<std::string>>{{"1","t"},{"2","t"},{"3","f"}}:
                                                         std::vector<std::vector<std::string>>{{"3","f"},{"2","f"},{"1","f"}})));
        // Output aliases and ordinals must bind to the real enum output
        // identity too, not resolve its basename under another search_path.
        for(const auto& sql:{"SELECT id,r AS x,r < 'alpha' FROM ranks ORDER BY x",
                             "SELECT id,r,r < 'alpha' FROM ranks ORDER BY 2"}) {
            auto alias=g_engine.prepareBoundQuery(db,sql);
            auto* statement=dynamic_cast<SelectStmt*>(alias.ast.get());assert(statement);
            assert(statement->orderBy.front().enumComparison &&
                   statement->orderBy.front().enumComparison->enumLabels==(db==dbA?labelsA:labelsB));
            auto rows=QueryPlanner::executePlanChecked(QueryPlanner::buildPreparedSelectPlan(&g_engine,db,"ranks",std::move(alias)));
            rows.throwIfFailed();assert(rows.structuredRows.size()==3);
            assert(rows.structuredRows.front()[0]==(db==dbA?"1":"3"));
        }
    }
    {
        StorageEngine cold;
        auto prepared=bind(cold,dbA);
        check(cold,dbA,prepared);
    }
    for(const auto& sql:{
        "SELECT r = 'absent' FROM ranks WHERE false",
        "SELECT CASE r WHEN 'absent' THEN 1 ELSE 0 END FROM ranks LIMIT 0",
        "SELECT 'absent'::rank_type < 'alpha'::rank_type"}) {
        std::string state;
        try{(void)g_engine.prepareBoundQuery(dbA,sql);}catch(const DbError& error){state=error.sqlState();}
        assert(state=="22P02");
    }
    auto constant=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(dbA,
        "SELECT CASE 'zeta'::rank_type WHEN 'alpha' THEN 7 WHEN 'zeta' THEN 8 ELSE 9 END"));
    auto* select=dynamic_cast<SelectStmt*>(constant->ast.get());assert(select);
    PreparedQueryExecution execution(constant,&g_engine,dbA);
    execution.prepareExpression(select->selectList[0].expr.get());
    execution.planStatementConstants(select);
    assert(execution.evaluate(select->selectList[0].expr.get(),execution.context()).value=="8");
    std::cout<<"[ENUM COMPARISON BINDING] real two-database identities/rank/NULL/CASE/copies/constant planning/cold metadata and pure invalid literal binding passed\n";
}
