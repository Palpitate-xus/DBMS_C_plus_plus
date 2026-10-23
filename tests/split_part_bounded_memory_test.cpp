#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "expression/ExprEvaluator.h"
#include "parser/ast.h"

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <vector>

// Observe the actual callback's allocations, not elapsed time or process RSS.
// No allocation is refused and the counter is armed only around evaluation.
namespace {
thread_local bool observeAllocation = false;
thread_local size_t largestAllocation = 0;
}
void* operator new(std::size_t size) {
    if (observeAllocation) largestAllocation = std::max(largestAllocation, size);
    if (void* result = std::malloc(size ? size : 1)) return result;
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete[](void* pointer) noexcept { std::free(pointer); }
void operator delete(void* pointer, std::size_t) noexcept { std::free(pointer); }
void operator delete[](void* pointer, std::size_t) noexcept { std::free(pointer); }

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    ExprEvaluator evaluator;
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "SPLIT_PART_BOUNDED_MEMORY_FAIL " << label << '\n'; }
    };
    FunctionCallExpr call;
    call.funcName = "split_part";
    for (const auto* name : {"s", "d", "n"}) {
        auto column = std::make_unique<ColumnRefExpr>();
        column->column = name;
        call.args.push_back(std::move(column));
    }
    const auto rowFor = [](const ExprValue& source, const ExprValue& delimiter, const ExprValue& position) {
        RowContext row; row.set("s", source); row.set("d", delimiter); row.set("n", position); return row;
    };
    const auto evaluate = [&](const std::string& source, const std::string& delimiter, int32_t position,
                              const std::string& type = "text") {
        return evaluator.eval(&call, rowFor(ExprValue(type, source), ExprValue(type, delimiter),
                                           ExprValue("integer", std::to_string(position))));
    };
    struct Sample { std::string source, delimiter; std::vector<std::string> fields; };
    const std::vector<Sample> samples = {
        {"", ",", {""}}, {"a,b,c", ",", {"a", "b", "c"}},
        {",a,,", ",", {"", "a", "", ""}}, {"aaa", "aa", {"", "a"}},
        {"aaaaa", "aa", {"", "", "a"}}, {"a<>b<>", "<>", {"a", "b", ""}},
        {"é界😀界z", "界", {"é", "😀", "z"}}, {"unchanged", "", {"unchanged"}}
    };
    const std::vector<int32_t> positions = {std::numeric_limits<int32_t>::min(), -5, -3, -2, -1,
                                           1, 2, 3, 5, std::numeric_limits<int32_t>::max()};
    for (const auto* type : {"text", "varchar"}) for (const auto& sample : samples) {
        for (const auto position : positions) {
            const int64_t index = position > 0 ? int64_t(position) - 1 : int64_t(sample.fields.size()) + position;
            const std::string expected = index < 0 || index >= int64_t(sample.fields.size()) ? "" : sample.fields[size_t(index)];
            const auto value = evaluate(sample.source, sample.delimiter, position, type);
            check(!value.isNull && value.typeName == "text" && value.value == expected, "exact forward nonoverlapping field");
        }
        bool rejected = false;
        try { (void)evaluate(sample.source, sample.delimiter, 0, type); }
        catch (const std::exception& error) { rejected = std::string(error.what()).find("22023") != std::string::npos; }
        check(rejected, "zero position remains 22023 including empty source/delimiter");
    }
    for (size_t nullIndex = 0; nullIndex != 3; ++nullIndex) {
        auto row = rowFor(ExprValue("text", "a,b"), ExprValue("text", ","), ExprValue("integer", "0"));
        row.set(nullIndex == 0 ? "s" : nullIndex == 1 ? "d" : "n",
                ExprValue(nullIndex == 2 ? "integer" : "text", "not a valid nonnull payload", true));
        const auto value = evaluator.eval(&call, row);
        check(value.isNull && value.typeName == "text", "strict typed NULL does not inspect payload/zero index");
    }
    for (const auto* type : {"character", "bpchar"}) {
        check(evaluate("a,b  ", ",  ", -1, type).value == "b", "CHAR source/delimiter trailing blanks removed");
        check(evaluate("a,b  ", ",  ", 1, type).value == "a", "CHAR positive field");
    }
    for (const size_t fields : {65536U, 262144U}) {
        std::string source = "first,";
        for (size_t i = 1; i < fields; ++i) source += "x,";
        source += "last";
        for (const int32_t position : {1, 2, -1, std::numeric_limits<int32_t>::min(), 0}) {
            auto row = rowFor(ExprValue("text", source), ExprValue("text", ","),
                              ExprValue("integer", std::to_string(position)));
            largestAllocation = 0;
            observeAllocation = true;
            ExprValue value;
            bool rejected = false;
            try { value = evaluator.eval(&call, row); }
            catch (const std::exception& error) { rejected = std::string(error.what()).find("22023") != std::string::npos; }
            observeAllocation = false;
            const size_t maximum = largestAllocation;
            const std::string expected = position == 1 ? "first" : position == 2 ? "x" : position == -1 ? "last" : "";
            check(position == 0 ? rejected : !rejected && !value.isNull && value.typeName == "text" && value.value == expected,
                  "complete large-input result/error");
            // Allow complete input copies and fixed evaluator bookkeeping,
            // but not a vector<string> allocation proportional to field count.
            check(maximum <= source.size() * 2 + 65536, "no full field-vector allocation");
            std::cout << "SPLIT_PART_ALLOCATION_OBSERVATION fields=" << fields << " position=" << position
                      << " input=" << source.size() << " largest=" << maximum << '\n';
        }
    }
    int effects = 0;
    evaluator.registerFunction("split_input_effect", [&](const std::vector<ExprValue>&) { ++effects; return ExprValue("text", "a,b"); });
    auto effect = std::make_unique<FunctionCallExpr>(); effect->funcName = "split_input_effect";
    call.args[0] = std::move(effect);
    const auto row = rowFor(ExprValue("text", "unused"), ExprValue("text", ","), ExprValue("integer", "2"));
    const auto value = evaluator.eval(&call, row);
    check(effects == 1 && value.typeName == "text" && value.value == "b", "input expression evaluated once");
    evaluator.registerFunction("split_part", [](const std::vector<ExprValue>&) { return ExprValue("text", "opaque"); });
    check(evaluator.eval(&call, row).value == "opaque", "native callback override retained");
    if (controls != 205) { ++failures; std::cerr << "SPLIT_PART_BAD_REACH " << controls << '\n'; }
    std::cout << "SPLIT_PART_BOUNDED_MEMORY_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
