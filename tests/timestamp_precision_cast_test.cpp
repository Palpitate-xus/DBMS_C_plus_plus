#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"
#include <iostream>
#include <string>

int main() {
    dbms::TypeRegistry::instance().bootstrap();dbms::ExprEvaluator evaluator;
    struct Case {const char* expression;const char* type;const char* value;bool null;};
    // Frozen actual PG18.6 rows from the independent full1806 reference.
    const Case cases[]={
        {"CAST('2024-02-29 12:34:56.987654' AS timestamp(2))","timestamp","2024-02-29 12:34:56.99",false},
        {"CAST('2024-03-01 00:00:01.123456' AS timestamp(2))","timestamp","2024-03-01 00:00:01.12",false},
        {"CAST('2024-02-29 12:34:56.987654+08:30' AS timestamptz(2))","timestamptz","2024-02-29 04:04:56.99+00",false},
        {"CAST('2024-03-01 00:00:01.123456-04' AS timestamptz(2))","timestamptz","2024-03-01 04:00:01.12+00",false},
        {"CAST(NULL AS timestamp(2))","timestamp","",true},
        {"CAST(NULL AS timestamptz(2))","timestamptz","",true},
    };
    unsigned checked=0,failed=0;
    const auto check=[&](bool good,const std::string& label){++checked;if(!good){++failed;std::cerr<<"FAIL "<<label<<'\n';}};
    for(const auto& item:cases) {
        try {
            dbms::SQLParser parser;const auto result=parser.parse(std::string("SELECT ")+item.expression);
            const auto* select=result.success?dynamic_cast<const dbms::SelectStmt*>(result.stmt.get()):nullptr;
            if(!select || select->selectList.size()!=1)throw dbms::DbError("XX000","invalid native timestamp precision fixture");
            const auto value=evaluator.eval(select->selectList[0].expr.get(),dbms::RowContext{});
            check(value.typeName==item.type,std::string(item.expression)+" canonical datum type");
            check(value.isNull==item.null,std::string(item.expression)+" nullness");
            check(item.null || value.value==item.value,std::string(item.expression)+" complete PG value");
        } catch(const dbms::DbError& error){check(false,std::string(item.expression)+" SQLSTATE="+error.sqlState());}
    }
    std::cout<<"TIMESTAMP_PRECISION_CAST_COMPLETE CHECKED="<<checked<<" FAILURES="<<failed<<'\n';
    return failed || checked!=18?1:0;
}
