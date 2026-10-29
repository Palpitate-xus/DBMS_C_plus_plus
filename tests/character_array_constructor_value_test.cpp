#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "executor/ExecutionPlan.h"
#include <iostream>
#include <memory>
#include <string>
#include <vector>

using namespace dbms;
static int checks=0,failures=0;
static void check(bool pass,const std::string& label) {
    ++checks;if(!pass){++failures;std::cerr<<"FAIL "<<label<<'\n';}
}
int main() {
    TypeRegistry::instance().bootstrap();ExprEvaluator evaluator;
    struct Case {std::string expression,type,value,state;};
    // Independently collected from PostgreSQL 18.6 before the production fix.
    const std::vector<Case> cases={
        {"ARRAY['abc '::character(4)]", "character[]", "{\"abc \"}", ""},
        {"ARRAY['abc'::character(5),'abcdef'::character(6),NULL]", "character[]", "{\"abc  \",abcdef,NULL}", ""},
        {"ARRAY['abc '::pg_catalog.bpchar]", "character[]", "{\"abc \"}", ""},
        {"ARRAY[CHARACTER 'abc ']", "character[]", "{\"abc \"}", ""},
        {"ARRAY['abc'::character]", "character[]", "{a}", ""},
        {"ARRAY['abc'::character(2)]", "character[]", "{ab}", ""},
        {"ARRAY[''::pg_catalog.bpchar,'NULL'::pg_catalog.bpchar,NULL::character]", "character[]", "{\"\",\"NULL\",NULL}", ""},
        {"ARRAY['你好吗'::character(4),'é'::character(2)]", "character[]", "{\"你好吗 \",\"é \"}", ""},
        {"ARRAY['a,b'::pg_catalog.bpchar,'a\"b'::pg_catalog.bpchar,'a\\b'::pg_catalog.bpchar,'{x}'::pg_catalog.bpchar]", "character[]", "{\"a,b\",\"a\\\"b\",\"a\\\\b\",\"{x}\"}", ""},
        {"ARRAY['abc'::character(4),'xyz']", "character[]", "{\"abc \",xyz}", ""},
        {"ARRAY['abc'::character(4),'xy'::varchar]", "character[]", "{\"abc \",xy}", ""},
        {"ARRAY['xy'::varchar,'abc'::character(4)]", "character varying[]", "{xy,abc}", ""},
        {"ARRAY['abc '::character(4),'x'::text]", "character[]", "{\"abc \",x}", ""},
        {"ARRAY['x'::text,'abc '::character(4)]", "text[]", "{x,abc}", ""},
        {"ARRAY[ARRAY['abc'::character(4)],ARRAY['xy'::character(3)]]", "character[]", "{{\"abc \"},{\"xy \"}}", ""},
        {"ARRAY[['abc'::character(4),'longer'::character(6)],['q'::character(4),'xy'::character(2)]]", "character[]", "{{\"abc \",longer},{\"q   \",xy}}", ""},
        {"ARRAY['[0:1]={\"abc \",\"xy\"}'::pg_catalog.bpchar[],'[0:1]={\"long  \",\"end\"}'::pg_catalog.bpchar[]]", "character[]", "[1:2][0:1]={{\"abc \",xy},{\"long  \",end}}", ""},
        {"ARRAY['{}'::pg_catalog.bpchar[],'{}'::pg_catalog.bpchar[]]", "character[]", "{}", ""},
        {"ARRAY[ARRAY[NULL::character(4)],ARRAY['abc'::character(5)]]", "character[]", "{{NULL},{\"abc  \"}}", ""},
        {"ARRAY[NULL::character(4),''::character(2)]", "character[]", "{NULL,\"  \"}", ""},
        {"CAST(ARRAY['abc'::character(4),'xy'::character(3)] AS character[])", "character[]", "{a,x}", ""},
        {"CAST(ARRAY['abc'::character(4)] AS character(2)[])", "character[]", "{ab}", ""},
        {"ARRAY[B'01',B'001',B'',NULL]", "bit[]", "{01,001,\"\",NULL}", ""},
        {"ARRAY[ARRAY[B'01',B'001'],ARRAY[B'10',B'101']]", "bit[]", "{{01,001},{10,101}}", ""},
        {"ARRAY[B'01'::bit]", "bit[]", "{0}", ""},
        {"ARRAY[ARRAY['abc'::character(4),'x'::character(2)],ARRAY['y'::character(3)]]", "", "", "2202E"},
        {"ARRAY[ARRAY['abc'::character(4)],NULL::pg_catalog.bpchar[]]", "", "", "2202E"},
    };
    for(const auto& item:cases) {
        try {
            auto prepared=prepareQuery("SELECT "+item.expression+" AS v",{},{});
            const auto* select=static_cast<const SelectStmt*>(prepared.ast.get());
            const auto value=evaluator.eval(select->selectList.at(0).expr.get(),RowContext{});
            check(item.state.empty(),item.expression+" expected error "+item.state);
            if(item.state.empty()) {
                check(ExprHelper::canonicalResultTypeName(ExprHelper::declaredTypeInput(value.typeName))==item.type,item.expression+" type="+value.typeName);
                check(!value.isNull,item.expression+" non-NULL array");
                check(value.value==item.value,item.expression+" value="+value.value+" expected="+item.value);
            }
        } catch(const DbError& error) {
            check(!item.state.empty() && error.sqlState()==item.state,item.expression+" SQLSTATE="+error.sqlState());
        } catch(const std::exception& error) {check(false,item.expression+" exception="+error.what());}
    }
    // A query-bound CHAR cell's complete bytes and true NULL state survive
    // constructor coercion; opaque payload of a NULL is never serialized.
    for(const std::string type:{"character","bpchar"}) {
        ArrayExpr array;array.elementType=type;RowContext row;
        for(size_t i=0;i<3;++i) {
            auto input=std::make_unique<ColumnRefExpr>();input->column="c"+std::to_string(i);
            QueryColumnBinding binding;binding.sourceOrdinal=0;binding.columnOrdinal=i;binding.declaredType="character";
            input->binding=binding;array.elements.push_back(std::move(input));
        }
        row.setBoundColumn(0,0,{"character","abc "});
        row.setBoundColumn(0,1,{"character","opaque NULL bytes",true});
        row.setBoundColumn(0,2,{"character",""});
        const auto value=evaluator.eval(&array,row);
        check(!value.isNull,type+" bound array is not NULL");
        check(ExprHelper::canonicalResultTypeName(value.typeName)=="character[]",type+" bound result type");
        check(value.value=="{\"abc \",NULL,\"\"}",type+" bound full value="+value.value);
        const auto cells=ExprEvaluator::arrayElements(value);
        check(cells.size()==3 && !cells[0].isNull && cells[0].value=="abc " && cells[1].isNull && !cells[2].isNull && cells[2].value.empty(),type+" cell identity");
    }
    int calls=0;
    evaluator.registerFunction("r323_tick",[&](const auto&){++calls;return ExprValue("character",calls==1?"abc ":"",calls==2);},'v');
    ArrayExpr array;array.elementType="character";
    for(int i=0;i<3;++i) {auto call=std::make_unique<FunctionCallExpr>();call->funcName="r323_tick";call->resolvedResultType="character";array.elements.push_back(std::move(call));}
    const auto value=evaluator.eval(&array,RowContext{});
    check(calls==3,"each constructor argument evaluates exactly once");
    check(value.value=="{\"abc \",NULL,\"\"}","volatile full value="+value.value);
    std::cout<<"CHARACTER_ARRAY_CONSTRUCTOR_COMPLETE "<<checks<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
