#include "catalog/type_registry.h"
#include "catalog/collation.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"

#include <clocale>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const std::string globalBefore = std::setlocale(LC_CTYPE, nullptr);
    ExprEvaluator evaluator;
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) { ++failures; std::cerr << "TEXT_CASE_LOCALE_NATIVE_FAILURE " << label << '\n'; }
    };
    const auto call = [&](const std::string& name, ExprValue input) {
        FunctionCallExpr function; function.funcName = name;
        auto column = std::make_unique<ColumnRefExpr>(); column->column = "v";
        function.args.push_back(std::move(column));
        RowContext row; row.set("v", std::move(input));
        return evaluator.eval(&function, row);
    };
    struct Case { std::string input; std::vector<std::string> unicode, ascii; };
    const std::vector<Case> cases = {
        {"", {"", "", ""}, {"", "", ""}},
        {"abc DEF", {"abc def", "ABC DEF", "Abc Def"}, {"abc def", "ABC DEF", "Abc Def"}},
        {"élÈVE déjà_vu", {"élève déjà_vu", "ÉLÈVE DÉJÀ_VU", "Élève Déjà_Vu"},
         {"élÈve déjà_vu", "éLÈVE DéJà_VU", "éLÈVe DéJà_Vu"}},
        {"ÄÉΣİẞǅ𐐀", {"äéσißǆ𐐨", "ÄÉΣİẞǄ𐐀", "Äéσißǆ𐐨"},
         {"ÄÉΣİẞǅ𐐀", "ÄÉΣİẞǅ𐐀", "ÄÉΣİẞǅ𐐀"}},
        {"äéσıßǆ𐐨", {"äéσıßǆ𐐨", "ÄÉΣIßǄ𐐀", "Äéσıßǆ𐐨"},
         {"äéσıßǆ𐐨", "äéσıßǆ𐐨", "äéσıßǆ𐐨"}},
        {"ΣΟΣ σος", {"σοσ σος", "ΣΟΣ ΣΟΣ", "Σοσ Σος"},
         {"ΣΟΣ σος", "ΣΟΣ σος", "ΣΟΣ σος"}},
        {"123éABC aßTEST", {"123éabc aßtest", "123ÉABC AßTEST", "123éabc Aßtest"},
         {"123éabc aßtest", "123éABC AßTEST", "123éAbc AßTest"}},
        {"😀éABC_中DEF", {"😀éabc_中def", "😀ÉABC_中DEF", "😀Éabc_中def"},
         {"😀éabc_中def", "😀éABC_中DEF", "😀éAbc_中Def"}}
    };
    const std::vector<std::string> names = {"lower", "upper", "initcap"};
    for (const auto& item : cases) for (const auto* type : {"text", "varchar", "char"})
        for (const auto* collation : {"C.utf8", "C", "POSIX"}) for (size_t i = 0; i < names.size(); ++i) {
            ExprValue input(type, item.input); input.collation = collation;
            if (std::string(type) == "char") input.value += "   ";
            try {
                const auto value = call(names[i], input);
                const auto& expected = std::string(collation) == "C.utf8" ? item.unicode : item.ascii;
                check(!value.isNull && value.typeName == "text" && value.value == expected[i] &&
                      value.collation == dbms::collation::normalizeName(collation), names[i] + " " + type + " " + collation);
            } catch (const std::exception& error) { check(false, names[i] + " " + error.what()); }
        }
    for (const auto* type : {"text", "varchar", "char"})
        for (const auto* collation : {"C.utf8", "C", "POSIX"}) for (const auto& name : names) {
            ExprValue input(type, "", true); input.collation = collation;
            try {
                const auto value = call(name, input);
                check(value.isNull && value.typeName == "text" && value.collation == dbms::collation::normalizeName(collation), name + " typed NULL");
            } catch (const std::exception& error) { check(false, name + " " + error.what()); }
        }
    for (const auto& raw : std::vector<std::string>{std::string("\xc3(", 2), std::string("\x80", 1), std::string("\xc0\xaf", 2)})
        for (const auto& name : names) {
            ExprValue input("text", raw); input.collation = "C.utf8";
            bool rejected = false;
            try { (void)call(name, input); }
            catch (const DbError& error) { rejected = error.sqlState() == "22021"; }
            catch (...) {}
            check(rejected, name + " invalid UTF8");
        }
    for (const auto& name : names) {
        ExprValue input("text", std::string("\x80", 1), true); input.collation = "C.utf8";
        try { check(call(name, input).isNull, name + " strict NULL before UTF8 conversion"); }
        catch (...) { check(false, name + " strict NULL before UTF8 conversion"); }
    }
    for (const auto& name : names) {
        size_t calls = 0;
        evaluator.registerFunction(name, [&](const std::vector<ExprValue>& args) {
            ++calls;
            if (args.size() != 1 || args.front().value != "é" || args.front().collation != "C.utf8")
                throw std::runtime_error("opaque input changed");
            return ExprValue("integer", "7");
        });
        ExprValue input("text", "é"); input.collation = "C.utf8";
        try {
            const auto value = call(name, input);
            check(calls == 1 && !value.isNull && value.typeName == "integer" && value.value == "7", name + " opaque callback");
        } catch (...) { check(false, name + " opaque callback"); }
    }
    check(globalBefore == std::setlocale(LC_CTYPE, nullptr), "global LC_CTYPE is unchanged");
    std::cout << "TEXT_CASE_LOCALE_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return controls == 259 && failures == 0 ? 0 : 1;
}
