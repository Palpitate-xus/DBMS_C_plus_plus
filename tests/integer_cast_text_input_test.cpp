#include "expression/ExprEvaluator.h"
#include "catalog/type_registry.h"
#include "common/DbError.h"
#include <iostream>

using namespace dbms;
int main() {
    TypeRegistry::instance().bootstrap();
    struct Case {const char* target; const char* input; const char* expected; const char* state; bool nullInput;};
    const Case cases[] = {
        {R"r278(smallint)r278", R"r278()r278", R"r278()r278", R"r278()r278", true},
        {R"r278(smallint)r278", R"r278(0)r278", R"r278(0)r278", R"r278()r278", false},
        {R"r278(smallint)r278", R"r278(010)r278", R"r278(10)r278", R"r278()r278", false},
        {R"r278(smallint)r278", R"r278(-0)r278", R"r278(0)r278", R"r278()r278", false},
        {R"r278(smallint)r278", R"r278( +0x_Ff 	)r278", R"r278(255)r278", R"r278()r278", false},
        {R"r278(smallint)r278", R"r278(-0b_1000_0000)r278", R"r278(-128)r278", R"r278()r278", false},
        {R"r278(smallint)r278", R"r278(0O_10)r278", R"r278(8)r278", R"r278()r278", false},
        {R"r278(smallint)r278", R"r278(1_000)r278", R"r278(1000)r278", R"r278()r278", false},
        {R"r278(smallint)r278", R"r278(+0X7fff)r278", R"r278(32767)r278", R"r278()r278", false},
        {R"r278(smallint)r278", R"r278(-0x8000)r278", R"r278(-32768)r278", R"r278()r278", false},
        {R"r278(smallint)r278", R"r278(32768)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(smallint)r278", R"r278(-32769)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(smallint)r278", R"r278(2147483647)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(smallint)r278", R"r278(-2147483648)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(smallint)r278", R"r278(2147483648)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(smallint)r278", R"r278(-2147483649)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(smallint)r278", R"r278(9223372036854775807)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(smallint)r278", R"r278(-9223372036854775808)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(smallint)r278", R"r278(0x7fff_ffff_ffff_ffff)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(smallint)r278", R"r278(-0x8000_0000_0000_0000)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(smallint)r278", R"r278(0x8000_0000_0000_0000)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(smallint)r278", R"r278()r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(bad)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(1.2)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(+ 1)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(1__2)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(0x__f)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(0b2)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(0o8)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(0x)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(_1)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(1_)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(0xF_)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(32768bad)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(smallint)r278", R"r278(2147483648bad)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(smallint)r278", R"r278(999999999999999999999bad)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(integer)r278", R"r278()r278", R"r278()r278", R"r278()r278", true},
        {R"r278(integer)r278", R"r278(0)r278", R"r278(0)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278(010)r278", R"r278(10)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278(-0)r278", R"r278(0)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278( +0x_Ff 	)r278", R"r278(255)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278(-0b_1000_0000)r278", R"r278(-128)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278(0O_10)r278", R"r278(8)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278(1_000)r278", R"r278(1000)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278(+0X7fff)r278", R"r278(32767)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278(-0x8000)r278", R"r278(-32768)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278(32768)r278", R"r278(32768)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278(-32769)r278", R"r278(-32769)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278(2147483647)r278", R"r278(2147483647)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278(-2147483648)r278", R"r278(-2147483648)r278", R"r278()r278", false},
        {R"r278(integer)r278", R"r278(2147483648)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(integer)r278", R"r278(-2147483649)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(integer)r278", R"r278(9223372036854775807)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(integer)r278", R"r278(-9223372036854775808)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(integer)r278", R"r278(0x7fff_ffff_ffff_ffff)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(integer)r278", R"r278(-0x8000_0000_0000_0000)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(integer)r278", R"r278(0x8000_0000_0000_0000)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(integer)r278", R"r278()r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(bad)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(1.2)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(+ 1)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(1__2)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(0x__f)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(0b2)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(0o8)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(0x)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(_1)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(1_)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(0xF_)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(32768bad)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(2147483648bad)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(integer)r278", R"r278(999999999999999999999bad)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(bigint)r278", R"r278()r278", R"r278()r278", R"r278()r278", true},
        {R"r278(bigint)r278", R"r278(0)r278", R"r278(0)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(010)r278", R"r278(10)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(-0)r278", R"r278(0)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278( +0x_Ff 	)r278", R"r278(255)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(-0b_1000_0000)r278", R"r278(-128)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(0O_10)r278", R"r278(8)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(1_000)r278", R"r278(1000)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(+0X7fff)r278", R"r278(32767)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(-0x8000)r278", R"r278(-32768)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(32768)r278", R"r278(32768)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(-32769)r278", R"r278(-32769)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(2147483647)r278", R"r278(2147483647)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(-2147483648)r278", R"r278(-2147483648)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(2147483648)r278", R"r278(2147483648)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(-2147483649)r278", R"r278(-2147483649)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(9223372036854775807)r278", R"r278(9223372036854775807)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(-9223372036854775808)r278", R"r278(-9223372036854775808)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(0x7fff_ffff_ffff_ffff)r278", R"r278(9223372036854775807)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(-0x8000_0000_0000_0000)r278", R"r278(-9223372036854775808)r278", R"r278()r278", false},
        {R"r278(bigint)r278", R"r278(0x8000_0000_0000_0000)r278", R"r278()r278", R"r278(22003)r278", false},
        {R"r278(bigint)r278", R"r278()r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(bad)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(1.2)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(+ 1)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(1__2)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(0x__f)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(0b2)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(0o8)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(0x)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(_1)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(1_)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(0xF_)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(32768bad)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(2147483648bad)r278", R"r278()r278", R"r278(22P02)r278", false},
        {R"r278(bigint)r278", R"r278(999999999999999999999bad)r278", R"r278()r278", R"r278(22003)r278", false},
    };
    size_t controls = 0, failures = 0;
    for (const auto& test : cases) for (const std::string source : {"unknown", "text", "varchar", "bpchar"})
        for (const bool bound : {false, true}) {
            ++controls; bool good = false; std::string state, detail;
            try {
                CastExpr cast; cast.typeName = test.target;
                auto input = std::make_unique<ParameterExpr>(); input->slot = 0; input->declaredType = source;
                cast.operand = std::move(input);
                RowContext row; row.setParameters({ExprValue(source, test.input, test.nullInput)});
                ExprEvaluator evaluator;
                if (bound) evaluator.bindScalarFunctions(&cast, nullptr);
                const auto value = evaluator.eval(&cast,row);
                good = !*test.state && value.typeName == test.target && value.isNull == test.nullInput &&
                    (value.isNull || value.value == test.expected);
                detail = value.value;
            } catch (const DbError& error) {
                state = error.sqlState(); detail = error.message(); good = state == test.state;
            } catch (const std::exception& error) { detail = error.what(); }
            if (!good) {
                ++failures; std::cerr << "INTEGER_CAST_TEXT_NATIVE_FAIL " << source << " -> " << test.target
                    << " input=" << test.input << " expected=" << test.expected << '/' << test.state
                    << " actual=" << detail << '/' << state << " bound=" << bound << '\n';
            }
        }
    std::cout << "INTEGER_CAST_TEXT_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
