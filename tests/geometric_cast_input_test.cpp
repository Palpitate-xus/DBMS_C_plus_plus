#include "commands/TableManage.h"
#include "common/DbError.h"
#include "common/GeometryValue.h"
#include "expression/ExprEvaluator.h"
#include "parser/query_binding.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::vector<std::pair<std::string,std::string>> shapes={
        {"point","(1,2)"}, {"line","{1,2,3}"}, {"lseg","[(0,0),(1,2)]"},
        {"box","(2,3),(0,0)"}, {"path","[(0,0),(1,2)]"},
        {"polygon","((0,0),(1,0),(0,1))"}, {"circle","<(1,2),3>"}};
    size_t failures=0;
    const auto expect=[&](const std::string& label,const auto& action) {
        std::string state;
        try { action(); } catch(const DbError& error){state=error.sqlState();}
        std::cout<<"GEOMETRY CAST INPUT "<<label<<" actual="<<state<<" expected=22P02"<<std::endl;
        if(state!="22P02")++failures;
    };
    ExprEvaluator evaluator;
    for(const auto& shape:shapes) {
        for(const auto& spelling:std::vector<std::string>{shape.first,"pg_catalog.\""+shape.first+"\""}) {
            for(const auto& expression:std::vector<std::string>{
                "CAST('bad' AS "+spelling+")", "'bad'::"+spelling})
                expect("pure preparation "+expression,[&]{
                    (void)prepareQuery("SELECT "+expression+" WHERE false",{},{});
                });
            CastExpr cast;cast.typeName=spelling;
            auto parameter=std::make_unique<ParameterExpr>();parameter->slot=0;parameter->declaredType="text";
            cast.operand=std::move(parameter);
            RowContext row;row.setParameters({ExprValue("text","bad",false)});
            expect("dynamic TEXT parameter "+spelling,[&]{(void)evaluator.eval(&cast,row);});
            row.setParameters({ExprValue("text","NULL",false)});
            expect("actual text NULL "+spelling,[&]{(void)evaluator.eval(&cast,row);});
            row.setParameters({ExprValue("text",shape.second,false)});
            const auto valid=evaluator.eval(&cast,row);
            if(valid.isNull || valid.typeName!=shape.first || valid.value!=shape.second) {
                ++failures;std::cout<<"GEOMETRY CAST valid descriptor/value mismatch "<<spelling<<std::endl;
            }
            row.setParameters({ExprValue("text","",true)});
            const auto null=evaluator.eval(&cast,row);
            if(!null.isNull || null.typeName!=shape.first) {
                ++failures;std::cout<<"GEOMETRY CAST typed NULL mismatch "<<spelling<<std::endl;
            }
        }
    }
    for(const auto& sql:std::vector<std::string>{
        "SELECT CASE NULL::PATH WHEN CAST('bad' AS PATH) THEN 1 ELSE 2 END",
        "SELECT CASE NULL::PATH WHEN CAST('bad' AS PATH) THEN 1 ELSE 2 END WHERE false",
        "SELECT CASE WHEN false THEN CAST('bad' AS PATH) ELSE NULL::PATH END"})
        expect(sql,[&]{(void)prepareQuery(sql,{},{});});

    StorageEngine engine;
    const std::string name="geometric_cast_input",db=testDbPath(name);
    cleanupTestDb(name);assert(engine.createDatabase(db)==DBStatus::OK);
    TableSchema table;table.len=1;table.cols[0].dataName="id";
    table.cols[0].dataType="integer";table.cols[0].dsize=4;
    assert(engine.createTable(db,"geometry_cast_rows",table)==DBStatus::OK);
    assert(engine.insertRow(db,"geometry_cast_rows",{{"id","1"}})==DBStatus::OK);
    assert(engine.createSequence(db,"geometry_cast_effects",1,1)==DBStatus::OK);
    assert(engine.createUDF(db,"geometry_cast_writer",{"p"},{"integer"},
        "BEGIN PERFORM nextval('geometry_cast_effects'); RETURN p; END;",'v',"plpgsql","integer")==DBStatus::OK);
    expect("engine metadata before writing CTE",[&]{
        (void)engine.prepareBoundQuery(db,"WITH w AS(INSERT INTO geometry_cast_rows VALUES(geometry_cast_writer(2)) RETURNING id) "
            "SELECT CASE NULL::PATH WHEN CAST('bad' AS PATH) THEN 1 ELSE 2 END FROM w");
    });
    assert(engine.nextval(db,"geometry_cast_effects")==1);
    assert(engine.query(db,"geometry_cast_rows",{}, {"id"})==std::vector<std::string>{"1 "});
    assert(failures==0);
    cleanupTestDb(name);std::cout<<"[GEOMETRIC CAST INPUT] passed\n";
}
