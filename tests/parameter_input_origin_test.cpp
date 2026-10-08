#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/expr_helper.h"
#include "expression/prepared_query_execution.h"
#include "parser/parser.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database=testDbPath("parameter_input_origin");
    if(g_engine.createDatabase(database,"utf8")!=DBStatus::OK)return 2;
    TableSchema calls;calls.tablename="calls";calls.append(makeIntColumn("id",false,4));
    if(g_engine.createTable(database,calls)!=DBStatus::OK || g_engine.createUDF(database,"origin_writer",{"p"},{"varbit"},
        "BEGIN INSERT INTO calls VALUES(1); RETURN p; END;",'v',"plpgsql","varbit")!=DBStatus::OK)return 2;
    size_t controls=0,failures=0;
    const auto require=[&](bool valid,const std::string& label){++controls;if(!valid){++failures;std::cerr<<"[PARAMETER ORIGIN FAIL] "<<label<<'\n';}};
    const auto count=[&]{return g_engine.query(database,"calls",{},{"id"},{}).size();};
    require(ParameterExpr{}.origin==ParameterOrigin::RuntimeCell,"default public execution cell is not a Bind constant");
    QueryBindingDatum compatible={"owned-origin-input","p","bit varying",{},true,{},1};
    require(compatible.origin==ParameterOrigin::StatementInput,"existing seven-field datum API retains statement input role");
    auto parser=SQLParser().parse("SELECT $1");const auto* select=dynamic_cast<const SelectStmt*>(parser.stmt.get());
    require(select && dynamic_cast<const ParameterExpr*>(select->selectList[0].expr.get())->origin==ParameterOrigin::StatementInput,"actual SQL $1 is explicitly a statement input");
    ParameterExpr runtime,statement,metadata;runtime.declaredType=statement.declaredType=metadata.declaredType="bit varying";
    statement.origin=ParameterOrigin::StatementInput;metadata.origin=ParameterOrigin::MetadataPlaceholder;
    const auto identity=[&](const ParameterExpr& value){return ExprHelper::scalarExpressionIdentity(&value,[](const ColumnRefExpr&){return "unused";});};
    require(identity(runtime)!=identity(statement) && identity(runtime)!=identity(metadata) && identity(statement)!=identity(metadata),
        "identical slot/type with different actual origin is not an interchangeable scalar identity");
    const ParameterExpr copied=statement;
    require(copied.origin==statement.origin && copied.slot==statement.slot && copied.declaredType==statement.declaredType,"genuine parameter fullcopy retains role and type");
    size_t cases=0;
    for(const auto origin:{ParameterOrigin::RuntimeCell,ParameterOrigin::StatementInput,ParameterOrigin::MetadataPlaceholder})
        for(const auto& value:std::vector<std::optional<std::string>>{{},"01"})
            for(const auto& operation:{" BETWEEN "," NOT BETWEEN "})
                for(size_t path=0;path<5;++path) {
                    ++cases;const bool frozenNull=origin==ParameterOrigin::StatementInput && !value;
                    const std::string op=operation;
                    std::string expression;size_t effects=2;
                    if(path==0)expression="$1"+op+"origin_writer(B'00') AND origin_writer(B'11')",effects=frozenNull?0:2;
                    if(path==1)expression="CAST($1 AS varbit)"+op+"origin_writer(B'00') AND origin_writer(B'11')",effects=frozenNull?0:2;
                    if(path==2)expression="$1::varbit"+op+"origin_writer(B'00') AND origin_writer(B'11')",effects=frozenNull?0:2;
                    if(path==3)expression="origin_writer(B'01')"+op+"$1 AND B'11'",effects=frozenNull?1:2;
                    if(path==4)expression="origin_writer(B'01')"+op+"B'00' AND $1",effects=frozenNull?1:2;
                    QueryBindingDatum datum=compatible;datum.value=value;datum.origin=origin;const auto before=count();
                    try {
                        auto prepared=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+expression+" AS value",{datum}));
                        // The real execution carrier copies the bound AST, including
                        // its fourth independent range-left occurrence.
                        PreparedQueryExecution execution(prepared,&g_engine,database);
                        auto* statementAst=dynamic_cast<SelectStmt*>(prepared->ast.get());auto* root=statementAst->selectList[0].expr.get();
                        execution.prepareExpression(root);const auto result=execution.evaluate(root,execution.context());
                        require(result.typeName=="boolean" && result.isNull==!value && (!value || result.asBool()==(op==" BETWEEN ")) && count()-before==effects,
                            "true compiled owner "+expression+" origin="+std::to_string(static_cast<unsigned>(origin))+" value="+value.value_or("NULL"));
                        require(prepared->parameters.size()==1 && prepared->parameters[0].isNull==!value && !prepared->uses.empty(),"typed datum/real parameter SQL provenance retained");
                    }catch(const DbError& error){require(false,expression+" "+error.sqlState());}
                }
    require(cases==60,"all 60 role/NULL/CAST/pair-demand cells reached");
    // Raw public AST callers manufacture runtime cells by default. Never
    // infer Bind ownership from their slot, spelling or current NULL datum.
    auto raw=SQLParser().parse("SELECT $1 BETWEEN origin_writer(B'00') AND origin_writer(B'11')");
    auto* rawSelect=dynamic_cast<SelectStmt*>(raw.stmt.get());auto* range=dynamic_cast<FunctionCallExpr*>(rawSelect->selectList[0].expr.get());
    auto rawCell=std::make_unique<ParameterExpr>();rawCell->declaredType="bit varying";range->args[0]=std::move(rawCell);
    ExprEvaluator evaluator;evaluator.setCurrentDB(database);evaluator.bindScalarFunctions(range,&g_engine);
    RowContext row;row.setParameters({ExprValue("bit varying","",true)});const auto before=count();
    const auto result=evaluator.eval(range,row);
    require(result.isNull && count()-before==2,"default raw runtime cell keeps both real writer demands");
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    std::cout<<"[PARAMETER INPUT ORIGIN] all "<<controls<<" actual parser/datum/runtime/metadata/copied-owner/NULL/pair-demand/identity controls; failures="<<failures<<'\n';
    return failures?1:0;
}
