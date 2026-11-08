#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include <iostream>
#include <memory>
#include <string>
#include <vector>
using namespace dbms;
struct Case { const char* type; const char* value; const char* format; const char* expected; };
static ExprValue render(ExprEvaluator& evaluator,const Case& c) {
 RowContext row;row.set("number",ExprValue(c.type,c.value,false));row.set("picture",ExprValue("text",c.format,false));
 FunctionCallExpr call;call.funcName="to_char";for(const char* name:{"number","picture"}){auto ref=std::make_unique<ColumnRefExpr>();ref->column=name;call.args.push_back(std::move(ref));}return evaluator.eval(&call,row);
}
int main(){ExprEvaluator evaluator;const std::vector<Case> cases={
 {"numeric", "0", "EEEE", " 0e+00"},
 {"numeric", "-0", "9.9EEEE", " 0.0e+00"},
 {"numeric", "1e999", "9.9EEEE", " 1.0e+999"},
 {"numeric", "-1e999", "9.9EEEE", "-1.0e+999"},
 {"numeric", "1e-999", "9.9EEEE", " 1.0e-999"},
 {"numeric", "-1e-999", "9.9EEEE", "-1.0e-999"},
 {"numeric", "9.995e-1", "9.9EEEE", " 10.0e-01"},
 {"numeric", "9.995e-1", "9EEEE", " 10e-01"},
 {"numeric", "NaN", "9.9EEEE", " #.#####"},
 {"numeric", "Infinity", "9.9EEEE", " #.#####"},
 {"numeric", "-Infinity", "9.9EEEE", " #.#####"},
 {"numeric", "NaN", ".EEEE", " .####"},
 {"numeric", "1e999", ".9EEEE", " 1.0e+999"},
 {"numeric", "1e-999", "9.EEEE", " 1e-999"},
 {"numeric", "1e999", "99.9EEEE", " 1.0e+999"},
 {"numeric", "1e-999", "d99EEEE", " 1.00e-999"},
 {"numeric", "NaN", "9d99EEEE", " #.######"},
 {"numeric", "0", "9.99eeee", " 0.00e+00"},
 {"bigint", "125", "9.9EEEE", " 1.3e+02"},
 {"bigint", "-125", "9.9EEEE", "-1.3e+02"},
 {"bigint", "9223372036854775807", "9.0EEEE", " 9.2e+18"},
 {"smallint", "125", "9.9EEEE", " 1.2e+02"},
 {"smallint", "-125", "9.9EEEE", "-1.2e+02"},
 {"integer", "125", "9.9EEEE", " 1.2e+02"},
 {"integer", "-125", "9.9EEEE", "-1.2e+02"},
 {"bigint", "125", "9.9EEEE", " 1.3e+02"},
 {"bigint", "-125", "9.9EEEE", "-1.3e+02"},
 {"numeric", "125", "9.9EEEE", " 1.3e+02"},
 {"numeric", "-125", "9.9EEEE", "-1.3e+02"},
 {"real", "125", "9.9EEEE", " 1.2e+02"},
 {"real", "-125", "9.9EEEE", "-1.2e+02"},
 {"double precision", "125", "9.9EEEE", " 1.2e+02"},
 {"double precision", "-125", "9.9EEEE", "-1.2e+02"},
 {"numeric", "1234.5", "FM9999.99", "1234.5"},
 {"numeric", "-3.14", "FM99.99", "-3.14"},
 };int failed=0;for(const auto& c:cases){try{auto value=render(evaluator,c);if(value.isNull||value.value!=c.expected){++failed;std::cerr<<"FAIL "<<c.type<<" "<<c.value<<" "<<c.format<<" expected ["<<c.expected<<"] actual ["<<value.value<<"]\n";}}catch(const std::exception& e){++failed;std::cerr<<"FAIL exception "<<c.type<<" "<<c.value<<" "<<c.format<<" "<<e.what()<<"\n";}}
 std::cout<<"NUMERIC_TOCHAR_SCIENTIFIC_EXACT_CHECKED="<<cases.size()<<" FAILED="<<failed<<"\n";return failed?1:0;}
