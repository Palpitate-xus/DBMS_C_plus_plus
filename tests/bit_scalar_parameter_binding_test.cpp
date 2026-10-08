#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "executor/ExecutionPlan.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("bit_scalar_parameter_binding");
    assert(g_engine.createDatabase(database) == DBStatus::OK);
    TableSchema schema; schema.tablename = "bits";
    Column id; id.dataName="id"; id.isNull=false; id.isPrimaryKey=true;
    assert(TypeRegistry::instance().resolveColumnType(id,"integer",{},false).empty());
    assert(id.dataType=="integer" && id.dsize==4 && !id.isVariableLength); schema.append(id);
    Column bit; bit.dataName = "v"; bit.isNull = true;
    assert(TypeRegistry::instance().resolveColumnType(bit,"varbit",{},false).empty()); schema.append(bit);
    for (const auto& name : {"bits","empty_bits"}) {
        schema.tablename = name; assert(g_engine.createTable(database,schema) == DBStatus::OK);
    }
    assert(g_engine.insertRow(database,"bits",{{"id","1"},{"v","01"}}) == DBStatus::OK);
    assert(g_engine.insertRow(database,"bits",{{"id","2"},{"v",""}}) == DBStatus::OK);
    assert(g_engine.insertRow(database,"bits",{{"id","3"},{"v",std::nullopt}}) == DBStatus::OK);
    size_t controls=0, failures=0;
    const auto check = [&](bool okay,const std::string& label) {
        ++controls; if (!okay) { ++failures; std::cerr << "BIT_SCALAR_PARAMETER_FAILURE " << label << '\n'; }
    };
    for (const auto& table : {"bits","empty_bits"})
        for (const auto& comparison : {"v=$1","$1=v"}) {
            const auto sql = std::string("SELECT id FROM ")+table+" WHERE "+comparison+" ORDER BY id";
            for (const auto& type : {"text","integer"})
                for (const auto& value : std::vector<std::optional<std::string>>{std::string("b01"),std::nullopt}) {
                    QueryBindingDatum parameter; parameter.identity="wire-slot-1"; parameter.position=1;
                    parameter.type=type; parameter.value=value; std::string state;
                    try { (void)g_engine.prepareBoundQuery(database,sql,{parameter}); }
                    catch (const DbError& error) { state=error.sqlState(); }
                    check(state=="42883",sql+" declared "+type+" independent of actual NULL/value");
                }
            for (const auto& type : {"bit","bit varying"})
                for (const auto& value : std::vector<std::optional<std::string>>{std::string("01"),std::string(""),std::nullopt}) {
                    QueryBindingDatum parameter; parameter.identity="wire-slot-1"; parameter.position=1;
                    parameter.type=type; parameter.value=value;
                    auto prepared=g_engine.prepareBoundQuery(database,sql,{parameter});
                    check(prepared.parameters.size()==1 && prepared.parameters[0].typeName==type &&
                          prepared.parameters[0].isNull==!value.has_value(),sql+" genuine typed slot/cell");
                    check(prepared.uses.size()==1 && sql.substr(prepared.uses[0].begin,
                          prepared.uses[0].end-prepared.uses[0].begin)=="$1",sql+" exact parameter source occurrence");
                    check(prepared.output.size()==1 && TypeRegistry::instance().normalizeTypeName(prepared.output[0].type)=="integer",
                          sql+" descriptor independent of rows/value/NULL: "+(prepared.output.empty()?"missing":prepared.output[0].type));
                    auto plan=QueryPlanner::buildPreparedSelectPlan(&g_engine,database,table,std::move(prepared));
                    auto actual=QueryPlanner::executePlanChecked(std::move(plan));
                    std::vector<std::string> expected;
                    if (std::string(table)=="bits" && value) expected.push_back(*value=="01"?"1 ":"2 ");
                    check(actual.ok && actual.rows==expected,sql+" actual whole-bound public runtime");
                }
        }
    std::cout << "[BIT SCALAR PARAMETER BINDING] complete controls=" << controls << " failures=" << failures << '\n';
    return failures?1:0;
}
