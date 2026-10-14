#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/query_binding.h"
#include "parser/parser.h"
#include <cassert>
#include <iostream>
#include <limits>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    SQLParser parser;
    ExprEvaluator evaluator;
    size_t controls = 0;
    auto check = [&](const std::string& sql, const std::string& expected,
                     bool nullValue, const std::string& state) {
        ++controls;
        auto parsed = parser.parse(sql);
        assert(parsed.isValid());
        std::cerr << "[BIT CAST] start " << sql << '\n';
        try {
            const auto* select = static_cast<SelectStmt*>(parsed.stmt.get());
            const auto actual = evaluator.eval(select->selectList.front().expr.get(),{});
            std::cerr << "[BIT CAST] " << sql << " actual=" << actual.value
                      << " null=" << actual.isNull << '\n';
            assert(state.empty() && actual.isNull == nullValue &&
                   (nullValue || actual.value == expected));
        } catch (const DbError& error) {
            std::cerr << "[BIT CAST] error " << error.sqlState() << '\n';
            assert(!state.empty() && error.sqlState() == state);
        }
    };
    const std::vector<std::string> targets = {"smallint","integer","bigint","numeric",
                                            "real","double precision","money","boolean","bytea"};
    const std::vector<std::string> sources = {"B''","B'1'","B'111'","B'111'::varbit",
                                            "NULL::bit","NULL::varbit"};
    for (size_t source = 0; source < sources.size(); ++source)
        for (const auto& target : targets) {
            const bool permitted = (target=="integer" || target=="bigint") &&
                                   (source<3 || source==4);
            check("SELECT ("+sources[source]+")::"+target,
                  source==0?"0":source==1?"1":"7", source==4,
                  permitted?"":"42846");
        }
    for (const size_t length : {31U,32U,33U,63U,64U,65U})
        for (const auto& target : {std::string("integer"),std::string("bigint")}) {
            const size_t width = target=="integer"?32:64;
            const std::string expected = length==width?"-1":
                length==31?"2147483647":length==32?"4294967295":
                length==33?"8589934591":"9223372036854775807";
            check("SELECT B'"+std::string(length,'1')+"'::"+target,expected,false,
                  length>width?"22003":"");
        }
    check("SELECT B'10000000000000000000000000000000'::integer","-2147483648",false,"");
    check("SELECT B'"+std::string("1")+std::string(63,'0')+"'::bigint",
          "-9223372036854775808",false,"");
    check("SELECT B'"+std::string(65,'0')+"'::bigint","",false,"22003");
    check("SELECT B'00000101'::integer","5",false,"");
    for (const auto& source : {std::string("1"),std::string("-1"),std::string("NULL")})
        for (const auto& original : {std::string("smallint"),std::string("integer"),std::string("bigint")})
            for (const auto& target : {std::string("bit(5)"),std::string("bit(65)"),std::string("varbit"),std::string("varbit(5)")}) {
                const bool permitted = original!="smallint" && target.rfind("bit(",0)==0;
                const size_t length = target=="bit(65)"?65:5;
                const std::string expected = source=="-1"?std::string(length,'1'):
                                            std::string(length-1,'0')+"1";
                check("SELECT ("+source+"::"+original+")::"+target,expected,source=="NULL",
                      permitted?"":"42846");
            }
    for (const auto& original : {std::string("numeric"),std::string("numeric(8,2)"),
            std::string("real"),std::string("double precision"),std::string("money"),
            std::string("boolean"),std::string("bytea")})
        for (const bool nullValue : {false,true})
            for (const auto& target : {std::string("bit(5)"),std::string("bit(65)"),
                                      std::string("varbit"),std::string("varbit(5)")}) {
                const std::string source = nullValue?"NULL":original=="bytea"?"'\\x01'":
                                           original=="boolean"?"true":original=="money"?"'1'":"1";
                check("SELECT ("+source+"::"+original+")::"+target,"",false,"42846");
            }
    for (const auto& target : {std::string("integer"),std::string("bigint")}) {
        ++controls;
        RowContext context;
        context.setParameters({ExprValue("bit(5)","00101")});
        CastExpr cast;
        cast.typeName=target;
        auto parameter=std::make_unique<ParameterExpr>();
        parameter->slot=0;
        parameter->declaredType="bit(5)";
        cast.operand=std::move(parameter);
        assert(evaluator.eval(&cast,context).value=="5");
    }
    // Both retained cast node spellings are checked at preparation, including
    // cases whose operand can never be demanded by the executor.
    for (const auto& sql : {
            "SELECT CAST(NULL::varbit AS integer)",
            "SELECT CASE WHEN false THEN B'1'::smallint ELSE 0::smallint END",
            "SELECT CAST(B'1'::varbit AS bigint) WHERE false",
            "SELECT (NULL::smallint)::bit(5) WHERE false",
            "SELECT CASE WHEN false THEN B'1'::money ELSE '0'::money END",
            "SELECT CAST(1::numeric AS bit(5)) WHERE false"}) {
        ++controls;
        try { (void)prepareQuery(sql,{},{}); assert(false); }
        catch (const DbError& error) { assert(error.sqlState()=="42846"); }
    }
    // Adjacent codecs retain their established semantics.
    check("SELECT '111'::integer","111",false,"");
    check("SELECT '\\x00000101'::bytea::integer","257",false,"");
    check("SELECT 1.6::numeric::integer","2",false,"");
    check("SELECT true::integer","1",false,"");
    std::cout << "[BIT INTEGER CAST VALUE] all " << controls
              << " binary width/sign/empty/NULL/type binding and adjacent controls passed\n";
}
