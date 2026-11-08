#include "catalog/type_registry.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "parser/parser.h"

#include <atomic>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    struct Case { const char* sql; const char* type; const char* value; const char* state; bool isNull; };
    const std::vector<Case> cases = {
        {"log(2::double precision)", "double precision", "0.3010299956639812", "", false},
        {"exp(1::numeric)", "numeric", "2.7182818284590452", "", false},
        {"ln(NULL::numeric)", "numeric", "", "", true},
        {"log10('1')", "double precision", "0", "", false},
        {"length('test'::text)", "integer", "4", "", false},
        {"upper('x'::text)", "text", "X", "", false},
        {"array_position(ARRAY[1,2,3],2)", "integer", "2", "", false},
        {"array_positions(ARRAY[1,2,2],2)", "integer[]", "{2,3}", "", false},
        {"abs(-123::integer)", "integer", "123", "", false},
        {"width_bucket(2::numeric,0::numeric,5::numeric,3)", "integer", "2", "", false},
        {"pi()", "double precision", "3.141592653589793", "", false},
        {"atan2(1,2)", "double precision", "0.4636476090008061", "", false},
        {"log('bad')", "", "", "22P02", false},
        {"length(1::integer)", "", "", "42883", false},
        {"gcd(24,18)", "integer", "6", "", false},
        {"scale(1.200::numeric)", "integer", "3", "", false}
    };
    constexpr int workers = 48;
    constexpr int repeats = 21;
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::vector<std::string> failures(workers);
    std::vector<std::thread> threads;
    for (int i = 0; i < workers; ++i) {
        threads.emplace_back([&, i] {
            const bool overrideLog = i >= 32;
            const auto& test = cases[static_cast<size_t>(i) % cases.size()];
            const std::string expression = overrideLog ? "log('007'::text)" : test.sql;
            auto parsed = SQLParser().parseForBinding("SELECT " + expression);
            ExprEvaluator evaluator;
            int calls = 0;
            bool argumentsOk = true;
            const std::string marker = std::to_string(1000 + i);
            if (overrideLog) {
                evaluator.registerFunction("log", [&](const std::vector<ExprValue>& args) {
                    ++calls;
                    argumentsOk = argumentsOk && args.size() == 1 && args[0].typeName == "text" &&
                                  !args[0].isNull && args[0].value == "007";
                    return ExprValue("integer", marker);
                });
            }
            ++ready;
            while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
            if (!parsed.success) { failures[i] = "parse failed"; return; }
            auto* select = dynamic_cast<SelectStmt*>(parsed.stmt.get());
            if (!select || select->selectList.empty()) { failures[i] = "unexpected AST"; return; }
            auto* expressionNode = select->selectList[0].expr.get();
            try {
                evaluator.bindScalarFunctions(expressionNode);
                for (int n = 0; n < repeats; ++n) {
                    const auto result = evaluator.eval(expressionNode, {});
                    if (overrideLog) {
                        if (result.typeName != "integer" || result.isNull || result.value != marker) {
                            failures[i] = "native callback result changed"; return;
                        }
                    } else if (*test.state || result.typeName != test.type ||
                               result.isNull != test.isNull || (!result.isNull && result.value != test.value)) {
                        failures[i] = "unexpected builtin result"; return;
                    }
                }
                if (overrideLog && (!argumentsOk || calls != repeats)) failures[i] = "native callback arguments or count changed";
            } catch (const DbError& error) {
                if (overrideLog || error.sqlState() != test.state) failures[i] = "unexpected SQLSTATE: " + error.sqlState();
            } catch (const std::exception& error) {
                failures[i] = std::string("unexpected exception: ") + error.what();
            }
        });
    }
    while (ready.load() != workers) std::this_thread::yield();
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) thread.join();
    int failed = 0;
    for (int i = 0; i < workers; ++i) {
        if (!failures[i].empty()) { ++failed; std::cerr << "worker " << i << ": " << failures[i] << '\n'; }
    }
    std::cout << "SCALAR_SIGNATURE_CONCURRENT_WORKERS=" << workers << " REPEATS=" << repeats
              << " FAILED=" << failed << '\n';
    return failed == 0 ? 0 : 1;
}
