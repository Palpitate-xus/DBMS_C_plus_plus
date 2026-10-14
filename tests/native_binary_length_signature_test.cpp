#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "expression/expr_helper.h"
#include "utils/Session.h"
#include "test_utils.h"

#include <algorithm>
#include <iostream>
#include <set>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

int main() {
    using namespace dbms;
    TypeRegistry::instance().bootstrap();
    const auto database = testDbPath("native_binary_length_signature");
    if (g_engine.createDatabase(database) != DBStatus::OK) return 2;
    Session session; session.username = "testuser"; session.permission = 1; session.currentDB = database;
    auto* previous = currentSession(); setCurrentSession(&session);
    struct Restore { Session* value; ~Restore() { setCurrentSession(value); } } restore{previous};
    size_t checks = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++checks;
        if (!good) { ++failures; std::cerr << "NATIVE_BINARY_LENGTH_FAILURE " << label << '\n'; }
    };
    const auto evaluate = [&](const std::string& sql, const std::string& type,
                              const std::string& value, bool null = false) {
        return ExprHelper::evalStringWithNulls(sql, {{"v", value}},
            null ? std::set<std::string>{"v"} : std::set<std::string>{},
            {{"v", type}}, database, session.username, &g_engine);
    };
    const auto integer = [](const ExprEvalResult& result, size_t expected) {
        return result.ok && !result.isNull && result.typeName == "integer" &&
               result.value == std::to_string(expected);
    };
    const std::vector<std::string> payloads = {
        "aé中", "\\x3132", std::string("a\0b", 3), "",
        std::string(1, static_cast<char>(0xff)) + '\0'};
    for (const auto* type : {"binary", "varbinary"}) {
        for (const auto& bytes : payloads)
            check(integer(evaluate("length(v)", type, bytes), bytes.size()),
                  std::string(type) + " raw byte length " + std::to_string(bytes.size()));
        check(integer(evaluate("length(v,'UTF8'::name)", type, "aé中"), 3),
              std::string(type) + " actual encoding character count");
        check(integer(evaluate("length(v,'UTF8'::name)", type, "\\x3132"), 6),
              std::string(type) + " encoding must not reinterpret a raw hex-looking value");
        const auto nullInput = evaluate("length(v)", type, "", true);
        check(nullInput.ok && nullInput.isNull && nullInput.typeName == "integer",
              std::string(type) + " typed NULL input");
        const auto nullEncoding = evaluate("length(v,NULL::name)", type, "aé中");
        check(nullEncoding.ok && nullEncoding.isNull && nullEncoding.typeName == "integer",
              std::string(type) + " typed NULL encoding");
        for (const auto* sql : {"upper(v)", "abs(v)"})
            check(evaluate(sql, type, "123").sqlState == "42883", std::string(type) + " strict " + sql);
    }
    // BYTEA/BLOB already contain encoded BYTEA datums, unlike native raw
    // BINARY/VARBINARY. Signature identity must not change either codec.
    for (const auto* type : {"bytea", "blob"})
        check(integer(evaluate("length(v)", type, "\\x3132"), 2), std::string(type) + " actual BYTEA codec");
    for (const auto* type : {"text", "character varying"})
        check(integer(evaluate("length(v)", type, "aé中"), 3), std::string(type) + " Unicode character count");
    for (const auto* sql : {"length(7)", "length()"})
        check(evaluate(sql, "text", "unused").sqlState == "42883", std::string("strict ") + sql);

    TableSchema table; table.tablename = "raw_values"; table.formatVersion = DATA_FILE_FORMAT_VERSION;
    table.append(makeVarBinaryColumn("v", false, 64));
    if (g_engine.createTable(database, table) != DBStatus::OK ||
        g_engine.insertRow(database, "raw_values", {{"v", "aé中"}}) != DBStatus::OK ||
        g_engine.insertRow(database, "raw_values", {{"v", "\\x3132"}}) != DBStatus::OK) return 2;
    try {
        StorageEngine::SelectExpr expression; expression.displayName = "n";
        expression.isScalar = true; expression.funcName = "length"; expression.funcArgs = {"v"};
        const auto rows = g_engine.queryExpr(database, "raw_values", {}, {expression});
        check(rows.size() == 2 && std::all_of(rows.begin(), rows.end(),
              [](const auto& row) { return row == "6 "; }), "real VARBINARY factory/heap/native projection");
    } catch (const std::exception& error) { check(false, std::string("real native projection ") + error.what()); }
    check(checks == 29, "complete matrix");
    if (g_engine.dropDatabase(database) != DBStatus::OK) return 2;
    std::cout << "NATIVE_BINARY_LENGTH_COMPLETE " << checks << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
