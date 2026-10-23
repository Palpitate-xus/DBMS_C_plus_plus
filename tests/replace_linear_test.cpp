#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"

#include <array>
#include <chrono>
#include <iostream>
#include <string>
#include <vector>

using namespace dbms;
struct Sample { std::string source, from, to, result; };

int main() {
    TypeRegistry::instance().bootstrap();
    ExprEvaluator evaluator;
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "REPLACE_LINEAR_FAILURE " << label << '\n'; }
    };
    const auto call = [&](const std::vector<ExprValue>& args) {
        FunctionCallExpr function; function.funcName = "replace"; RowContext row;
        for (size_t i = 0; i < args.size(); ++i) {
            const std::string key = "a" + std::to_string(i);
            auto column = std::make_unique<ColumnRefExpr>(); column->column = key;
            function.args.push_back(std::move(column)); row.set(key, args[i]);
        }
        return evaluator.eval(&function, row);
    };
    const auto text = [](const std::string& value) { return ExprValue("text", value); };
    const auto good = [](const ExprValue& value, const std::string& result) {
        return !value.isNull && value.typeName == "text" && value.value == result;
    };
    const auto quote = [](const std::string& value) {
        std::string result = "'";
        for (char ch : value) { result += ch; if (ch == '\'') result += ch; }
        return result + "'";
    };
    const std::vector<Sample> samples = {
        {"", "", "", ""}, {"", "a", "b", ""}, {"abc", "", "x", "abc"},
        {"abc", "x", "y", "abc"}, {"abc", "abc", "X", "X"}, {"abc", "abc", "", ""},
        {"aaaa", "a", "xx", "xxxxxxxx"}, {"aaaa", "aa", "b", "bb"},
        {"aaa", "aa", "b", "ba"}, {"ababa", "aba", "X", "Xba"},
        {"aaa", "a", "aa", "aaaaaa"}, {"aaaa", "aa", "a", "aa"},
        {"abab", "ab", "", ""}, {"abXXab", "ab", "界", "界XX界"},
        {"aé界😀", "é界", "!", "a!😀"}, {"ééé", "éé", "界", "界é"},
        {"😀😀", "😀", "é", "éé"}, {"éé", "́", "!", "e!e!"},
        {"é", "e", "x", "é"}, {"a b  ", " ", "", "ab"},
        {"a'b'a", "'", "\"", "a\"b\"a"}, {"a\\b\\a", "\\", "'", "a'b'a"},
        {"a\nb\na", "\n", "\t", "a\tb\ta"}, {"xax", "x", "xax", "xaxaxax"}
    };
    for (const auto& item : samples) {
        for (const auto* type : {"text", "varchar"})
            check(good(call({ExprValue(type,item.source),ExprValue(type,item.from),
                             ExprValue(type,item.to)}),item.result), "typed sample");
        const auto value = ExprHelper::evalString("replace(" + quote(item.source) + "," +
            quote(item.from) + "," + quote(item.to) + ")", {});
        check(value.ok && !value.isNull && value.typeName == "text" &&
              value.value == item.result, "SQL sample");
    }
    const std::vector<std::pair<std::string,std::string>> character = {
        {"replace('ab  '::char(4),'b','!')", "a!"},
        {"replace('ab  '::varchar,'b','!')", "a!  "},
        {"replace('a b  ',' '::char(1),'x')", "a b  "},
        {"replace('aba','b '::char(2),'X')", "aXa"},
        {"replace('aba','b','X  '::char(3))", "aXa"},
        {"replace('é界  '::char(4),'界','😀')", "é😀"}
    };
    for (const auto& item : character) {
        const auto value = ExprHelper::evalString(item.first, {});
        check(value.ok && !value.isNull && value.value == item.second &&
              value.typeName == "text", "CHAR argument");
    }
    for (unsigned mask = 1; mask < 8; ++mask) {
        std::vector<ExprValue> args = {text("abc"),text("b"),text("X")};
        for (size_t i = 0; i < args.size(); ++i) if (mask & (1U << i))
            args[i] = ExprValue("text", "", true);
        const auto value = call(args);
        check(value.isNull && value.typeName == "text", "strict NULL");
    }
    for (const size_t count : {16384U,65536U,262144U}) {
        const std::string input(count,'a');
        for (const auto& item : std::vector<Sample>{
            {input,"a","",""}, {input,"a","bb",std::string(count*2,'b')},
            {input,"a","b",std::string(count,'b')},
            {"a"+std::string(count,'x')+"a","a","",std::string(count,'x')}
        }) {
            const auto started = std::chrono::steady_clock::now();
            const auto value = call({text(item.source),text(item.from),text(item.to)});
            const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now()-started).count();
            check(good(value,item.result), "dense/sparse replacement");
            // Timing is diagnostic, never a machine-dependent pass threshold.
            std::cout << "REPLACE_LINEAR_ELAPSED_NS " << count << ' ' << item.to.size()
                      << ' ' << item.source.size() << ' ' << elapsed << '\n';
        }
    }
    for (int nullArgument : {-1,1,2}) {
        std::array<int,3> effects{};
        FunctionCallExpr function; function.funcName="replace";
        const std::array<std::string,3> values={"aba","b","X"};
        for(size_t i=0;i<values.size();++i) {
            const std::string name="replace_effect_"+std::to_string(i);
            evaluator.registerFunction(name,[&,i](const std::vector<ExprValue>&) {
                ++effects[i]; return ExprValue("text",values[i],static_cast<int>(i)==nullArgument);
            });
            auto arg=std::make_unique<FunctionCallExpr>();arg->funcName=name;
            function.args.push_back(std::move(arg));
        }
        (void)ExprHelper::inferParsedResultType(&function,{},"",nullptr,&evaluator);
        const bool pure=effects==std::array<int,3>{0,0,0};
        const auto value=evaluator.eval(&function,{});
        check(pure && effects==std::array<int,3>{1,1,1} &&
              (nullArgument<0 ? good(value,"aXa") : value.isNull), "metadata/effects");
    }
    evaluator.registerFunction("replace",[](const std::vector<ExprValue>& args) {
        return ExprValue("text","opaque"+std::to_string(args.size()));
    });
    check(good(call({text("a"),text("b")}),"opaque2"), "native replacement keeps arity");
    if(controls!=101){++failures;std::cerr<<"REPLACE_LINEAR_BAD_INVENTORY "<<controls<<'\n';}
    std::cout<<"REPLACE_LINEAR_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
