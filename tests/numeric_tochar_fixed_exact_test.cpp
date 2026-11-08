#include "catalog/type_registry.h"
#include "expression/expr_helper.h"
#include <iostream>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;
using namespace dbms;
int main() {
    TypeRegistry::instance().bootstrap();
    unsigned checked=0;
    std::vector<std::string> failed;
    const auto eval=[&](const std::string& s){ return ExprHelper::evalString(s,{}, {},"", "", &g_engine); };
    const auto value=[&](const std::string& expr,const std::string& expected) {
        const auto r=eval(expr); ++checked;
        if (!r.ok || r.isNull || r.value!=expected) failed.push_back(expr);
    };
    const auto error=[&](const std::string& expr,const std::string& state) {
        const auto r=eval(expr); ++checked;
        if (r.ok || r.sqlState!=state) failed.push_back(expr);
    };
    const auto formatted=[&](const std::string& n,const std::string& picture) {
        return "to_char('"+n+"'::numeric,'"+picture+"')";
    };
    value(formatted("1e999","9.9")," #.#");
    value(formatted("-1e999","9.9"),"-#.#");
    value(formatted("1e999","FM9.9"),"#.#");
    value(formatted("1e998","9V99")," ###");
    value(formatted("-1e998","9V99"),"-###");
    value(formatted("1e999","99.9S"),"##.#+");
    value(formatted("-1e999","99.9S"),"##.#-");
    value(formatted("1e999","99.9MI"),"##.# ");
    value(formatted("-1e999","99.9MI"),"##.#-");
    value(formatted("1e999","99.9PR")," ##.# ");
    value(formatted("-1e999","99.9PR"),"<##.#>");
    value(formatted("1e999","\"prefix\"99.9"),"prefix ##.#");
    value(formatted("-1e999","99.9\"suffix\""),"-##.#suffix");
    value(formatted("-1e999","FMPL9.9"),"-#.#");
    value(formatted("-1e999","PL9.9")," -#.#");
    value(formatted("1e999","FMPL9.9"),"+#.#");
    value(formatted("-1e998","FMPL9V99"),"-###");
    value(formatted("-1e999","\"X\"FMPL9.9"),"X-#.#");
    value(formatted("1e999","9V99S"),"+###");
    value(formatted("1e999","FM9V99S"),"+###");
    value(formatted("-1e999","9V99S"),"-###");
    value(formatted("-1e999","FM9V99S"),"-###");
    const std::string zeros(1000,'0');
    value(formatted("-1","99."+zeros+"PL")," -1."+zeros+" ");
    value(formatted("-1","99."+zeros+"PR")," <1."+zeros+">");
    value(formatted("1","99."+zeros+"S")," 1."+zeros+"+");
    value(formatted("9.995","9V99")," ###");
    value(formatted("-0.005","9V99"),"  -1");
    value("to_char(1.005::numeric,'FM999.00')","1.01");
    value("to_char(-1.005::numeric,'FM999.00')","-1.01");
    value("to_char('1e999'::numeric,'9.9EEEE')"," 1.0e+999");
    value("to_char(1::integer,'9."+zeros+"')"," 1."+zeros);
    value("to_char(-1::bigint,'99."+zeros+"PR')"," <1."+zeros+">");
    error("to_char(1::smallint,'9."+zeros+"')","22003");
    error("to_char(1::bigint,'9V"+std::string(1000,'9')+"')","22003");
    error("to_char(1::integer,'9V"+std::string(1000,'9')+"')","22003");
    error(formatted("1e999","9G999.99"),"22003");
    error(formatted("1e999","9\"mid\"9.9"),"22003");
    for (const auto& x:failed) std::cerr<<"FAIL "<<x.substr(0,120)<<'\n';
    std::cout<<"NUMERIC_TOCHAR_FIXED_EXACT_CHECKED="<<checked<<" FAILED="<<failed.size()<<'\n';
    return failed.empty()?0:1;
}
