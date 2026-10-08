#include "commands/TableManage.h"
#include "executor/ExecutionPlan.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();const auto database=testDbPath("origin_correlated_child");
    if(g_engine.createDatabase(database,"utf8")!=DBStatus::OK)return 2;
    TableSchema calls;calls.tablename="calls";calls.append(makeIntColumn("id",false,4));
    TableSchema input;input.tablename="rows";input.append(makeIntColumn("id",false,4,true));input.append(makeIntColumn("i",true,4));
    Column bit;bit.dataName="b";bit.isNull=true;TypeRegistry::instance().resolveColumnType(bit,"varbit",{},false);input.append(bit);
    if(g_engine.createTable(database,calls)!=DBStatus::OK || g_engine.createTable(database,input)!=DBStatus::OK)return 2;
    for(int id=1;id<=3;++id)if(g_engine.insertRow(database,"rows",{{"id",std::to_string(id)},
        {"i",id==1?std::optional<std::string>{}:std::optional<std::string>{std::to_string(id-2)}},
        {"b",id==1?std::optional<std::string>{}:std::optional<std::string>{id==2?"00":"01"}}})!=DBStatus::OK)return 2;
    if(g_engine.createUDF(database,"child_bit_writer",{"p"},{"varbit"},"BEGIN INSERT INTO calls VALUES(1); RETURN p; END;",'v',"plpgsql","varbit")!=DBStatus::OK ||
       g_engine.createUDF(database,"child_int_writer",{"p"},{"integer"},"BEGIN INSERT INTO calls VALUES(1); RETURN p; END;",'v',"plpgsql","integer")!=DBStatus::OK)return 2;
    size_t controls=0,failures=0;
    const auto require=[&](bool valid,const std::string& label){++controls;if(!valid){++failures;std::cerr<<"[CORRELATED ORIGIN FAIL] "<<label<<'\n';}};
    const auto count=[&]{return g_engine.query(database,"calls",{},{"id"},{}).size();};
    for(const bool bits:{true,false})for(int id=1;id<=3;++id)for(const auto& operation:{" BETWEEN "," NOT BETWEEN "}) {
        const auto expression=std::string(bits?"o.b":"o.i")+operation+(bits?"child_bit_writer(B'00') AND child_bit_writer(B'11')":"child_int_writer(0) AND child_int_writer(2)");
        const auto sql="SELECT (SELECT "+expression+") AS value FROM rows o WHERE o.id="+std::to_string(id);
        for(const bool fallback:{false,true}) {
            const auto before=count();bool valid=false;
            try {
                auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,sql));
                if(!fallback) {
                    auto cursor=QueryPlanner::makePreparedCursor(QueryPlanner::buildPreparedQueryPlan(&g_engine,database,query,query->ast.get()),query->output);
                    std::vector<ExprValue> row;valid=query->output.size()==1 && query->output[0].type=="boolean" && cursor->next(row) && row.size()==1 &&
                        row[0].typeName=="boolean" && row[0].isNull==(id==1) && (id==1 || row[0].asBool()==(std::string(operation)==" BETWEEN ")) && !cursor->next(row);cursor->close();
                }else {
                    // No fake provider/executor: this actual public execution
                    // owner takes its genuine scalar-child legacy fallback.
                    PreparedQueryExecution execution(query,&g_engine,database);auto* select=dynamic_cast<SelectStmt*>(query->ast.get());auto* child=select->selectList[0].expr.get();
                    execution.prepareExpression(child);auto row=execution.context();const auto source=std::find_if(query->sourceRanges.begin(),query->sourceRanges.end(),[&](const auto& source){return source.owner==query->ast.get();});
                    if(source==query->sourceRanges.end())return 2;
                    std::vector<ExprValue> cells;
                    for(const auto& column:source->columns) {
                        const auto value=column.name=="id"?std::to_string(id):column.name=="i"?id==1?"":std::to_string(id-2):id==1?"":id==2?"00":"01";
                        cells.emplace_back(column.type,value,column.name!="id" && id==1);
                    }
                    execution.setSourceRow(row,source->ordinal,cells);
                    const auto value=execution.evaluate(child,row);valid=value.typeName=="boolean" && value.isNull==(id==1) && (id==1 || value.asBool()==(std::string(operation)==" BETWEEN "));
                }
            }catch(const DbError& error){std::cerr<<"[CORRELATED ORIGIN ERROR] "<<error.sqlState()<<' '<<error.what()<<'\n';}
            require(valid && count()-before==2,sql+(fallback?" actual legacy scalar fallback":" actual prepared child cursor")+" writer calls="+std::to_string(count()-before));
        }
    }
    require(controls==24,"all 24 actual cursor/fallback/type/NULL/nonNULL/role controls reached");
    if(g_engine.dropDatabase(database)!=DBStatus::OK)return 2;
    std::cout<<"[PARAMETER ORIGIN CORRELATED CHILD] all "<<controls<<" complete actual typed rows/NULL/values/cursor/legacy fallback/effects controls; failures="<<failures<<'\n';
    return failures?1:0;
}
