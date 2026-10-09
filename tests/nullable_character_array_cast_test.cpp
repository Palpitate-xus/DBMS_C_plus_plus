#include "commands/TableManage.h"
#include "expression/expr_helper.h"
#include "executor/ExecutionPlan.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <iostream>
extern dbms::StorageEngine g_engine;
int main() {
    using namespace dbms;
    size_t checks=0,failures=0;
    const auto require=[&](bool pass,const std::string& role){++checks;failures+=!pass;std::cout<<"NULLABLE_CHARACTER_ARRAY "<<role<<" pass="<<pass<<'\n';};
    const auto db=testDbPath("nullable_character_array_cast");
    require(g_engine.createDatabase(db)==DBStatus::OK,"create isolated database");
    for(const auto& type:{"varchar(3)[]","character(3)[]","varchar(3)[][]","pg_catalog._varchar(3)"}) {
        const auto expression=std::string("CAST(NULL AS ")+type+")";
        SQLParser parser;auto parsed=parser.parseForBinding("SELECT "+expression);
        if(!parsed.success)throw DbError(parsed.sqlState,parsed.error);
        ExprEvaluator evaluator;evaluator.setCurrentDB(db);
        const auto* select=static_cast<const SelectStmt*>(parsed.stmt.get());
        require(evaluator.eval(select->selectList.at(0).expr.get(),RowContext{}).isNull,expression+" actual cast evaluator");
        const auto stored=ExprHelper::evalString(expression,{},{},db);
        require(stored.ok && stored.isNull,expression+" stored expression");
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(db,"SELECT "+expression+" AS value"));
        auto plan=QueryPlanner::buildPreparedQueryPlan(&g_engine,db,query,query->ast.get());
        auto result=QueryPlanner::executePlanChecked(std::move(plan));result.throwIfFailed();
        require(result.structuredNulls==std::vector<std::vector<bool>>{{true}} &&
            query->output.at(0).typeOid==(std::string(type).find("character")==0?1014:1015),expression+" typed execution and actual OID");
    }
    std::cout<<"NULLABLE_CHARACTER_ARRAY_CHECKED="<<checks<<" FAILED="<<failures<<'\n';
    return checks==13 && !failures?0:1;
}
