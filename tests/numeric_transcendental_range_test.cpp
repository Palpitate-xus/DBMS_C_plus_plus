#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/expr_helper.h"
#include "test_utils.h"
#include <cmath>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;
using namespace dbms;
int main() {
    unsigned checked = 0; std::vector<std::string> failed;
    auto check = [&](bool value, const std::string& name) { ++checked; if (!value) failed.push_back(name); };
    try {
        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000, 60000);
        const auto db = testDbPath("numeric_transcendental_range"); cleanupTestDb("numeric_transcendental_range");
        if (g_engine.createDatabase(db, "utf8") != DBStatus::OK) throw std::runtime_error("create db");
        Session session; session.username="admin"; session.permission=1; session.currentDB=db;
        struct Scope { Session* prior=currentSession(); explicit Scope(Session& s){setCurrentSession(&s);} ~Scope(){setCurrentSession(prior);} } scope(session);
        auto eval = [&](const std::string& expr) {return ExprHelper::evalString(expr, {}, {}, db, "", &g_engine);};
        auto err = [&](const std::string& expr,const std::string& state) {auto r=eval(expr);check(!r.ok && r.sqlState==state,expr);};
        auto value = [&](const std::string& expr,const std::string& expected) {auto r=eval(expr);check(r.ok&&!r.isNull&&r.value==expected,expr);};
        auto null = [&](const std::string& expr) {auto r=eval(expr);check(r.ok&&r.isNull,expr);};
        err("exp('1e999'::numeric)","22003");
        value("exp('-1e999'::numeric)", "0." + std::string(1000, '0'));
        value("exp('-2304'::numeric)", "0." + std::string(1000, '0'));
        value("exp('-2303.9'::numeric)", "0." + std::string(1000, '0'));
        value("exp(0::numeric)", "1.0000000000000000");
        value("exp(1::numeric)", "2.7182818284590452");
        value("exp(100::numeric)", "26881171418161354484126255515800135873611119");
        value("scale(exp(100::numeric))", "0");
        value("exp(-100::numeric)", "0.00000000000000000000000000000000000000000003720075976020836");
        value("scale(exp(-100::numeric))", "59");
        value("exp('0.1000000000000000000'::numeric)", "1.1051709180756476248");
        value("scale(exp('0.1000000000000000000'::numeric))", "19");
        // PostgreSQL rounds these adjacent inputs on opposite sides of a carry.
        value("exp('-2.30258509299404568401799145468442'::numeric)", "0.09999999999999999999999999999999");
        value("exp('-2.30258509299404568401799145468441'::numeric)", "0.10000000000000000000000000000000");
        value("exp('-0.0000000100000000'::numeric)", "0.9999999900000001");
        value("ln('1e999'::numeric)", "2300.2825079010516");
        err("ln('1e-999'::numeric)","22003");
        value("ln('1.0000000000000000001'::numeric)", "0.00000000000000000010000000000000000");
        value("log('1.0000000000000000001'::numeric)", "0.00000000000000000004342944819032518");
        value("log(2::numeric,'1.0000000000000000001'::numeric)", "0.00000000000000000014426950408889634");
        value("ln('2.0000000000000000000000000000000001'::numeric)",
              "0.6931471805599453094172321214581766");
        value("log('1.00000001'::numeric,'1.00000002000000009'::numeric)",
              "1.99999999900000001");
        value("log('1.00000001'::numeric,'1.00000002000000010'::numeric)",
              "2.00000000000000000");
        value("log('1.00000001'::numeric,'1.00000002000000011'::numeric)",
              "2.00000000099999999");
        value("exp('1e-999'::numeric)", "1." + std::string(998, '0') + "1");
        value("exp('-1e-999'::numeric)", "0." + std::string(999, '9'));
        value("log('1e999'::numeric)", "999.0000000000000");
        value("log10('1e999'::numeric)", "999.0000000000000");
        value("log(10::numeric,'1e999'::numeric)", "999.0000000000000");
        value("log('100.00'::numeric)", "2.0000000000000000");
        value("log('0.010'::numeric)", "-2.0000000000000000");
        value("log(10.00::numeric,'1e-100'::numeric)", "-100." + std::string(100, '0'));
        err("log('1e-999'::numeric)","22003");
        err("log(1::numeric,10::numeric)","22012");
        err("log(1::numeric,0::numeric)","2201E");
        err("log('Infinity'::numeric,'-Infinity'::numeric)","2201E");
        err("log('1e-1000'::numeric,'1e-1000'::numeric)","22003");
        err("log(0::numeric,10::numeric)","2201E");
        err("log(-1::numeric,10::numeric)","2201E");
        err("log(2::numeric,0::numeric)","2201E");
        err("ln('-Infinity'::numeric)","2201E");
        value("log('0.5'::numeric,'Infinity'::numeric)","Infinity");
        value("log(1::numeric,'Infinity'::numeric)","Infinity");
        value("log('Infinity'::numeric,'Infinity'::numeric)","NaN");
        value("log('Infinity'::numeric,10::numeric)","0");
        value("log('-Infinity'::numeric,'NaN'::numeric)","NaN");
        value("log('NaN'::numeric,0::numeric)","NaN");
        value("log(0::numeric,'NaN'::numeric)","NaN");
        value("exp('NaN'::numeric)","NaN");
        value("exp('Infinity'::numeric)","Infinity");
        value("exp('-Infinity'::numeric)","0");
        null("exp(NULL::numeric)"); null("ln(NULL::numeric)"); null("log(NULL::numeric,1::numeric)");
        DdlExecutor ddl;
        if(ddl.executeSql("CREATE TABLE transdef(v text DEFAULT (exp('1e999'::numeric)::text))",session)) throw std::runtime_error("create default");
        if(g_engine.insertRow(db,"transdef",{{"v","keep"}})!=DBStatus::OK) throw std::runtime_error("insert existing");
        bool rejected=false;try {(void)g_engine.insertRow(db,"transdef",{});}catch(const DbError& e){rejected=e.sqlState()=="22003";}
        check(rejected,"oversized exp DEFAULT rejects with 22003");
        check(g_engine.query(db,"transdef",{},{"v"})==std::vector<std::string>{"keep "},"failed default preserves original row");
        check(!g_engine.inTransaction()&&g_engine.getLockManager().lockedTables().empty(),"failure releases transaction and locks");
        for(const auto& f:failed) std::cerr<<"FAIL "<<f<<'\n';
        std::cout<<"NUMERIC_TRANSCENDENTAL_RANGE_CHECKED="<<checked<<" FAILED="<<failed.size()<<'\n';
        return failed.empty()?0:1;
    } catch(const std::exception& e) {std::cerr<<"SETUP_ERROR "<<e.what()<<'\n';return 2;}
}
