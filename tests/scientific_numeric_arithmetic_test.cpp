#include "commands/TableManage.h"
#include "expression/expr_helper.h"
#include "expression/prepared_query_execution.h"
#include "test_utils.h"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto fraction=[](int scale,const std::string& digits) {
        return "0."+std::string(scale-static_cast<int>(digits.size()),'0')+digits;
    };
    const std::vector<std::pair<std::string,std::string>> cases={
        {"1e-40+1e-40",fraction(40,"2")},
        {"1e-40-1e-40",fraction(40,"0")},
        {"1e-20*1e-20",fraction(40,"1")},
        {"5e-40%2e-40",fraction(40,"1")},
        {"mod(5e-40,2e-40)",fraction(40,"1")},
        {"-5e-40%2e-40","-"+fraction(40,"1")},
        {"mod(-5e-40,2e-40)","-"+fraction(40,"1")},
        {"1.250e-2+2.50e-3","0.01500"},
        {"1.250e-2-2.50e-3","0.01000"},
        {"1.250e-2*2.50e-3","0.0000312500"},
        {"1.250e-2%2.50e-3","0.00000"},
        {"mod(1.250e-2,2.50e-3)","0.00000"},
        {"1.00e2+2.0e1","120"},
        {"1.00e2*2.0e1","2000"},
        {".5E-2+5.e-3","0.010"},
        {"-.5E-2+5.e-3","0.000"},
        {"0e-40+2e-40",fraction(40,"2")},
        {"-0e-40*2e-40",fraction(80,"0")},
        {"0e-40%2e-40",fraction(40,"0")},
        {"mod(0e-40,2e-40)",fraction(40,"0")},
        {"1e400+1","1"+std::string(399,'0')+"1"},
        {"1E-400*2e-100",fraction(500,"2")},
        {"1e-40/1e-40","1."+std::string(40,'0')},
        {"1e-40/2e0",fraction(41,"5")+std::string(19,'0')},
        {"0e-40/1e0",fraction(40,"0")},
        {"1.250e-2/2.50e-3","5.0000000000000000"},
        {"div(5e-40,2e-40)","2"},
        {"CAST(gcd(6e-40,4e-40) AS numeric)",fraction(40,"2")},
        {"CAST(lcm(6e-40,4e-40) AS numeric)",fraction(40,"12")},
        {"'1e-40'::numeric+1e-40",fraction(40,"2")},
        {"CAST(1.250e-2 AS NUMERIC(10,5))+2.50e-3","0.01500"},
        {"1.25+2.50","3.75"}, {"1.25*2.50","3.1250"},
        {"'NaN'::numeric+1e-40","NaN"},
        {"'Infinity'::numeric+1e-40","Infinity"},
        {"'-Infinity'::numeric*1e-40","-Infinity"},
        {"'Infinity'::numeric%1e-40","NaN"},
        {"mod('Infinity'::numeric,1e-40)","NaN"},
    };
    size_t controls=0, failures=0;
    const auto check=[&](const std::string& label,bool valid,const std::string& actual) {
        ++controls;
        if(!valid)++failures;
        std::cout<<"SCIENTIFIC_ARITHMETIC_NATIVE "<<label<<" actual="<<actual<<" match="<<valid<<std::endl;
    };
    const std::string name="scientific_numeric_arithmetic";
    cleanupTestDb(name);
    const auto database=testDbPath(name);
    assert(g_engine.createDatabase(database,"utf8")==DBStatus::OK);
    for(const auto& control:cases) {
        const auto result=ExprHelper::evalString(control.first,{});
        check("legacy "+control.first,result.ok && !result.isNull && result.value==control.second,result.value);
        auto query=std::make_shared<PreparedQuery>(g_engine.prepareBoundQuery(database,"SELECT "+control.first));
        check("metadata "+control.first,query->output.size()==1 && query->output[0].type=="numeric",query->output.at(0).type);
        auto* statement=dynamic_cast<SelectStmt*>(query->ast.get());
        assert(statement && statement->selectList.size()==1);
        PreparedQueryExecution execution(query,&g_engine,database);
        execution.prepareExpression(statement->selectList[0].expr.get());
        const auto cell=execution.evaluate(statement->selectList[0].expr.get(),execution.context());
        check("prepared "+control.first,!cell.isNull && cell.typeName=="numeric" && cell.value==control.second,cell.value);
        execution.closeChildCursors();
    }
    for(const auto& expression:std::vector<std::string>{
        "NULL::numeric+1e-40","NULL::numeric-1e-40","NULL::numeric*1e-40",
        "NULL::numeric/1e-40","NULL::numeric%1e-40","mod(NULL::numeric,1e-40)"}) {
        const auto result=ExprHelper::evalString(expression,{});
        check("NULL "+expression,result.ok && result.isNull,result.value);
    }
    assert(g_engine.dropDatabase(database)==DBStatus::OK);
    cleanupTestDb(name);
    finalCleanupTestData();
    assert(controls==120);
    std::cout<<"SCIENTIFIC_ARITHMETIC_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<std::endl;
    assert(failures==0);
}
