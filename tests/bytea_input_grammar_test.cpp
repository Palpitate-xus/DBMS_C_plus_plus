#include "Session.h"
#include "catalog/type_registry.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include "expression/expr_helper.h"
#include "parser/parser.h"
#include "types/bytea.h"
#include <functional>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {
using namespace dbms;
struct Case { std::optional<std::string> input, value; std::string state; };

std::vector<Case> cases() {
    std::vector<Case> result = {
        {"\\xq0",{},"22023"}, {"\\x0",{},"22023"}, {"\\x0 0",{},"22023"},
        {"\\x00 g0",{},"22023"}, {"\\x00 1",{},"22023"}, {"\\xGG",{},"22023"},
        {"\\x0\t0",{},"22023"}, {"\\x   0",{},"22023"},
        {"\\",{},"22P02"}, {"\\q0",{},"22P02"}, {"\\400",{},"22P02"},
        {"\\08a",{},"22P02"}, {"\\00",{},"22P02"},
        {{},{},""}, {"","\\x",""}, {"abc","\\x616263",""}, {"\\x","\\x",""},
        {"\\x00AFff","\\x00afff",""}, {"\\x00 AF ff","\\x00afff",""},
        {"\\x00\tAF\nff\r","\\x00afff",""}, {"\\000\\377","\\x00ff",""},
        {"\\\\","\\x5c",""}, {"NULL","\\x4e554c4c",""}
    };
    for (const std::string value : {"\\X", "\\X00", "\\XAF", "\\X00ff", "\\X \t"})
        result.push_back({value,{},"22P02"});
    for (char space : {'\v','\f'}) {
        const std::string c(1, space);
        for (const auto& value : {"\\x"+c+"00", "\\x00"+c+"ff", "\\x00"+c, "\\x0"+c+"0"})
            result.push_back({value,{},"22023"});
    }
    for (char space : {' ','\t','\n','\r'}) {
        const std::string c(1, space);
        result.push_back({"\\x"+c+"00","\\x00",""});
        result.push_back({"\\x00"+c+"ff","\\x00ff",""});
        result.push_back({"\\x00"+c,"\\x00",""});
    }
    result.push_back({"\v","\\x0b",""});
    result.push_back({"\f","\\x0c",""});
    result.push_back({"\\x0b0c","\\x0b0c",""});
    return result;
}

void require(bool valid, const std::string& label) {
    if (!valid) throw std::runtime_error(label);
}

std::string literal(const std::optional<std::string>& value) {
    if (!value) return "NULL";
    std::string result = "'";
    for (char c : *value) { result += c; if (c == '\'') result += c; }
    return result + "'";
}

void outcome(const Case& item, const std::function<ExprValue()>& action) {
    ExprValue value;
    std::string state;
    try { value = action(); } catch (const DbError& error) { state = error.sqlState(); }
    require(state == item.state, "expected SQLSTATE " + item.state + ", got " + state);
    if (state.empty()) {
        require(value.typeName == "bytea" && value.isNull == !item.input, "wrong type/NULL bit");
        if (item.input) require(item.value && value.value == *item.value, "wrong BYTEA value");
    }
}
}  // namespace

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto inputs = cases();
    size_t controls = 0, failures = 0;
    const auto check = [&](const std::string& label, const std::function<void()>& action) {
        ++controls;
        try { action(); } catch (const std::exception& error) {
            ++failures;
            std::cerr << "BYTEA_INPUT_GRAMMAR_NATIVE_FAIL " << label << " : " << error.what() << '\n';
        }
    };
    for (const std::string source : {"unknown","text","varchar","name"}) for (const auto& item : inputs) {
        const auto text = literal(item.input);
        const auto expression = "CAST(" + (source == "unknown" ? text : text+"::"+source) + " AS bytea)";
        for (bool bind : {false,true}) check(expression + (bind ? " native bound" : " native raw"), [&] {
            CastExpr cast; cast.typeName = "bytea";
            auto parameter = std::make_unique<ParameterExpr>();
            parameter->slot = 0; parameter->declaredType = source; cast.operand = std::move(parameter);
            ExprEvaluator evaluator;
            if (bind) evaluator.bindScalarFunctions(&cast, &g_engine);
            RowContext row; row.setParameters({ExprValue(source, item.input.value_or(""), !item.input)});
            outcome(item, [&] { return evaluator.eval(&cast, row); });
        });
        for (bool bind : {false,true}) check(expression + (bind ? " parsed bound" : " parsed raw"), [&] {
            auto parsed = SQLParser().parseForBinding("SELECT " + expression);
            require(parsed.success, parsed.error);
            auto* select = dynamic_cast<SelectStmt*>(parsed.stmt.get());
            require(select && select->selectList.size() == 1, "missing target");
            auto* node = select->selectList.front().expr.get();
            ExprEvaluator evaluator;
            if (bind) evaluator.bindScalarFunctions(node, &g_engine);
            outcome(item, [&] { return evaluator.eval(node, {}); });
        });
        check(expression + " helper", [&] {
            outcome(item, [&] {
                const auto result = ExprHelper::evalString(expression, {}, {}, "", "", &g_engine);
                if (!result.ok) throw DbError(result.sqlState.empty() ? "XX000" : result.sqlState, result.error);
                return ExprValue(result.typeName, result.value, result.isNull);
            });
        });
    }
    for (const auto& pair : std::vector<std::pair<std::string,size_t>>{{"smallint",2},{"integer",4},{"bigint",8}})
        for (const std::string input : {"0","-1","1"}) for (bool bind : {false,true}) check("integer width " + pair.first, [&] {
            CastExpr cast; cast.typeName = "bytea";
            auto parameter = std::make_unique<ParameterExpr>();
            parameter->slot = 0; parameter->declaredType = pair.first; cast.operand = std::move(parameter);
            ExprEvaluator evaluator;
            if (bind) evaluator.bindScalarFunctions(&cast, &g_engine);
            RowContext row; row.setParameters({ExprValue(pair.first, input)});
            const auto value = evaluator.eval(&cast, row);
            std::string expected = "\\x";
            for (size_t i = 0; i < pair.second; ++i)
                expected += input == "-1" ? "ff" : i+1 == pair.second && input == "1" ? "01" : "00";
            require(!value.isNull && value.typeName == "bytea" && value.value == expected, "integer conversion changed");
        });
    for (const auto& item : inputs) if (item.input) check("direct input", [&] {
        auto result = ByteaValue::fromBytes("unchanged");
        require(ByteaValue::parse(*item.input, result) == item.state.empty(), "wrong accept/reject");
        require(result.toString() == (item.state.empty() ? *item.value : "\\x756e6368616e676564"), "bytes/output mutation");
    });
    std::string allBytes;
    for (unsigned i = 0; i < 256; ++i) {
        const char c = static_cast<char>(i);
        const bool space = c == ' ' || c == '\t' || c == '\n' || c == '\r';
        for (bool leading : {false,true}) check("boundary byte " + std::to_string(i), [&] {
            auto result = ByteaValue::fromBytes("unchanged");
            const auto input = leading ? "\\x"+std::string(1,c)+"00" : "\\x00"+std::string(1,c)+"ff";
            require(ByteaValue::parse(input, result) == space, "non-PG whitespace accepted");
            require(result.toString() == (space ? leading ? "\\x00" : "\\x00ff" : "\\x756e6368616e676564"), "wrong boundary bytes");
        });
        check("byte roundtrip " + std::to_string(i), [&] {
            auto original = ByteaValue::fromBytes(std::string(1,c));
            ByteaValue result;
            require(ByteaValue::parse(original.toString(), result) && result == original, "binary roundtrip changed");
        });
        allBytes += c;
    }
    check("all bytes including NUL", [&] {
        ByteaValue result;
        const auto original = ByteaValue::fromBytes(allBytes);
        require(ByteaValue::parse(original.toString(), result) && result.bytes() == allBytes, "all-byte roundtrip changed");
    });
    const std::string database = "bytea_grammar_owned";
    bool ready = false;
    check("create owned database", [&] { require(g_engine.createDatabase(database,"utf8") == DBStatus::OK, "create database failed"); });
    check("create BYTEA table and sentinel row", [&] {
        Session session; session.username = "testuser"; session.permission = 1; session.currentDB = database;
        DdlExecutor ddl;
        require(!ddl.executeSql("CREATE TABLE t (id integer PRIMARY KEY, b bytea)",session), "create table failed");
        require(g_engine.insert(database,"t",{{"id","0"},{"b","\\x7a"}}) == DBStatus::OK, "sentinel insert failed");
        ready = true;
    });
    size_t id = 0;
    // Storage's legacy string API uses a NULL sentinel; exercise SQL NULL and
    // literal 'NULL' above, not by reinterpreting that separate API here.
    for (const auto& item : inputs) if (item.input && *item.input != "NULL") {
        const auto key = std::to_string(++id);
        check("storage insert " + key, [&] {
            require(ready, "setup failed");
            const auto status = g_engine.insert(database,"t",{{"id",key},{"b",*item.input}});
            require(status == (item.state.empty() ? DBStatus::OK : DBStatus::INVALID_VALUE), "wrong insert status");
            const auto rows = g_engine.query(database,"t",{"=id "+key},{"b"},{});
            if (!item.state.empty()) require(rows.empty(), "rejected input left a row");
            else { require(rows.size() == 1, "missing inserted row"); auto row = rows.front(); while (!row.empty() && (row.back() == '\n' || row.back() == ' ')) row.pop_back(); require(row == (item.input->empty() ? std::string() : *item.value), "wrong stored bytes"); }
        });
        check("storage update " + key, [&] {
            require(ready, "setup failed");
            const auto before = g_engine.query(database,"t",{"=id 0"},{"b"},{});
            const auto status = g_engine.update(database,"t",{{"b",*item.input}},{"=id 0"});
            require(status == (item.state.empty() ? DBStatus::OK : DBStatus::INVALID_VALUE), "wrong update status");
            const auto rows = g_engine.query(database,"t",{"=id 0"},{"b"},{});
            if (!item.state.empty()) require(rows == before, "rejected update changed the row");
            else { require(rows.size() == 1, "missing updated row"); auto row = rows.front(); while (!row.empty() && (row.back() == '\n' || row.back() == ' ')) row.pop_back(); require(row == (item.input->empty() ? std::string() : *item.value), "wrong updated bytes"); }
        });
    }
    if (inputs.size() != 51 || controls != 1957) { ++failures; std::cerr << "BYTEA_INPUT_GRAMMAR_NATIVE_INVENTORY " << controls << '\n'; }
    std::cout << "BYTEA_INPUT_GRAMMAR_NATIVE_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
