#include "catalog/type_registry.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/ast.h"

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

using namespace dbms;
struct Case { std::string name; std::vector<ExprValue> args; std::string expected; };
static ExprValue text(const std::string& value) { return ExprValue("text", value); }
static ExprValue integer(int value) { return ExprValue("integer", std::to_string(value)); }
static ExprValue nullText() { return ExprValue("text", "", true); }

int main() {
    TypeRegistry::instance().bootstrap();
    ExprEvaluator evaluator;
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "UTF8_SPLIT_LINEAR_FAILURE " << label << '\n'; }
    };
    const auto call = [&](const std::string& name, const std::vector<ExprValue>& args) {
        FunctionCallExpr function; function.funcName = name; RowContext row;
        for (size_t i = 0; i < args.size(); ++i) {
            const std::string key = "a" + std::to_string(i);
            auto column = std::make_unique<ColumnRefExpr>(); column->column = key;
            function.args.push_back(std::move(column)); row.set(key, args[i]);
        }
        return evaluator.eval(&function, row);
    };
    const auto test = [&](const Case& item) {
        const auto value = call(item.name, item.args);
        check(!value.isNull && value.value == item.expected &&
              value.typeName == (item.name == "string_to_array" ? "text[]" : "text"), item.name);
    };
    struct Trim { std::string input, set, both, left, right; };
    for (const auto& item : std::vector<Trim>{
        {"", "é", "", "", ""}, {"ééxé", "é", "x", "xé", "ééx"},
        {"丰x丰", "中估", "丰x丰", "丰x丰", "丰x丰"}, {"😀界a界😀", "😀界", "a", "a界😀", "😀界a"},
        {"éxé", "é", "x", "xé", "éx"}, {" x ", "", " x ", " x ", " x "}}) {
        test({"btrim", {text(item.input), text(item.set)}, item.both});
        test({"ltrim", {text(item.input), text(item.set)}, item.left});
        test({"rtrim", {text(item.input), text(item.set)}, item.right});
    }
    struct Pad { std::string input; int length; std::string fill, left, right; };
    for (const auto& item : std::vector<Pad>{{"é",5,"😀界","😀界😀界é","é😀界😀界"},
        {"é界😀",2,"x","é界","é界"}, {"a",5,"","a","a"}, {"a",0,"界","",""},
        {"",3,"é","ée","ée"}}) for (const auto* name : {"lpad", "rpad"})
        test({name,{text(item.input),integer(item.length),text(item.fill)},std::string(name)=="lpad"?item.left:item.right});
    for (const auto& item : std::vector<Case>{
        {"translate",{text("aé界😀"),text("é界😀"),text("XYZ")},"aXYZ"},
        {"translate",{text("aé界😀"),text("é界😀"),text("Z")},"aZ"},
        {"translate",{text("aé界😀"),text(""),text("x")},"aé界😀"},
        {"translate",{text("éé"),text("éé"),text("ab")},"aa"},
        {"translate",{text("é"),text("́"),text("!")},"e!"},
        {"translate",{text(""),text("é"),text("a")},""},
        {"string_to_array",{text("aé界😀"),nullText()},"{a,é,界,😀}"},
        {"string_to_array",{text(""),nullText()},"{}"},
        {"string_to_array",{text("é"),nullText()},"{e,́}"},
        {"string_to_array",{text("é界é"),nullText(),text("é")},"{NULL,界,NULL}"},
        {"string_to_array",{text("aé界"),text("")},"{aé界}"},
        {"string_to_array",{text("a界b界"),text("界")},"{a,b,\"\"}"}}) test(item);
    for (const auto& item : std::vector<Case>{{"btrim",{nullText(),text("é")},""},
        {"lpad",{nullText(),integer(3),text("é")},""},
        {"translate",{nullText(),text("é"),text("a")},""},
        {"string_to_array",{nullText(),nullText()},""}})
        check(call(item.name,item.args).isNull,item.name+" NULL");
    for (const size_t count : {2048U,4096U,8192U}) {
        std::string input, mapped, array = "{";
        for (size_t i=0;i<count;++i) { input += "aé界😀"; mapped += "aXYZ"; if(i)array+=',';array+="a,é,界,😀"; }
        array += '}';
        for (const auto& item : std::vector<Case>{{"btrim",{text(input),text("!")},input},
            {"btrim",{text("z"),text(input)},"z"},
            {"lpad",{text(""),integer(static_cast<int>(count*4)),text(input)},input},
            {"rpad",{text(""),integer(static_cast<int>(count*4)),text(input)},input},
            {"translate",{text(input),text("é界😀"),text("XYZ")},mapped},
            {"translate",{text("z"),text(input),text("X")},"z"},
            {"translate",{text("é"),text("é"),text(input)},"a"},
            {"string_to_array",{text(input),nullText()},array}}) {
            const auto started=std::chrono::steady_clock::now(); test(item);
            const auto elapsed=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now()-started).count();
            std::cout<<"UTF8_SPLIT_LINEAR_OBSERVATION "<<item.name<<' '<<count<<' '<<elapsed<<'\n';
        }
    }
    int effects=0;
    evaluator.registerFunction("split_input_effect",[&](const std::vector<ExprValue>&){++effects;return text("é界😀");});
    for (const auto& item : std::vector<Case>{{"btrim",{text("é")},"界😀"},
        {"lpad",{integer(5),text("z")},"zzé界😀"}, {"translate",{text("é"),text("a")},"a界😀"},
        {"string_to_array",{nullText()},"{é,界,😀}"}}) {
        FunctionCallExpr function;function.funcName=item.name;
        auto effect=std::make_unique<FunctionCallExpr>();effect->funcName="split_input_effect";function.args.push_back(std::move(effect));
        for(const auto& arg:item.args){auto literal=std::make_unique<LiteralExpr>();literal->typeName=arg.isNull?"null":arg.typeName;literal->value=arg.isNull?"null":arg.value;function.args.push_back(std::move(literal));}
        const int before=effects;(void)ExprHelper::inferParsedResultType(&function,{},"",nullptr,&evaluator);
        const bool pure=effects==before;const auto value=evaluator.eval(&function,{});
        check(pure && effects==before+1 && !value.isNull && value.value==item.expected,item.name+" input once");
    }
    for(const auto* name:{"btrim","lpad","translate","string_to_array"}) {
        evaluator.registerFunction(name,[](const std::vector<ExprValue>&){return text("opaque");});
        try {
            const auto value=call(name,{text("abc")});
            check(!value.isNull && value.typeName=="text" && value.value=="opaque",
                  std::string(name)+" opaque override");
        } catch(const std::exception& error) {
            check(false,std::string(name)+" opaque override "+error.what());
        }
    }
    if(controls!=76){++failures;std::cerr<<"UTF8_SPLIT_LINEAR_BAD_INVENTORY "<<controls<<'\n';}
    std::cout<<"UTF8_SPLIT_LINEAR_NATIVE_COMPLETE "<<controls<<" FAILURES "<<failures<<'\n';
    return failures?1:0;
}
