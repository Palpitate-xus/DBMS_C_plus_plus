#include "parser/parser.h"
#include "utils/Session.h"
#include <cassert>
#include <iostream>
#include <string>
#include <vector>

int main() {
    dbms::SQLParser parser;
    size_t checks = 0;
    const auto setting = [&](const std::string& sql, const std::string& expected,
                             dbms::SetStmt::Scope scope = dbms::SetStmt::Scope::Session) {
        for (bool binding : {false, true}) {
            auto parsed = binding ? parser.parseForBinding(sql) : parser.parse(sql);
            const auto* value = parsed.success
                ? dynamic_cast<const dbms::SetStmt*>(parsed.stmt.get()) : nullptr;
            if (!value) std::cerr << sql << ": " << parsed.error << '\n';
            assert(value && value->name == "search_path" && value->scope == scope);
            std::string joined;
            for (const auto& argument : value->values) {
                if (!joined.empty()) joined += ", ";
                joined += argument;
            }
            assert(joined == expected);
            ++checks;
        }
    };
    for (const auto& pair : std::vector<std::pair<std::string, std::string>>{
        {"'public, pg_catalog'", "\"public, pg_catalog\""},
        {"'Public'", "\"Public\""}, {"'public'", "public"},
        {"'\"two words\"'", "\"\"\"two words\"\"\""},
        {"'a''b'", "\"a'b\""}, {"'a,b'", "\"a,b\""}, {"''", "\"\""},
        {"'default'", "\"default\""}, {"\"default\"", "\"default\""},
        {"'123'", "\"123\""}, {"\"123\"", "\"123\""}, {"123", "123"},
        {"- 123", "-123"}, {"+123", "123"}, {"0x4", "4"}, {"0o17", "15"},
        {"0b101", "5"}, {"1_234", "1234"}, {"1.2", "1.2"}, {"1e2", "1e2"},
        {".5", ".5"}, {"-5.", "-5."}, {"'$user'", "\"$user\""},
        {"E'a\\tb'", "\"a\tb\""}, {"E'a\\x2cb'", "\"a,b\""},
        {"$$a,b$$", "\"a,b\""}, {"'a'\n'b'", "ab"},
        {"'a' -- continue\n'b'", "ab"}, {"E'a\\t'\n'b'", "\"a\tb\""},
        {"'a', 'B', pg_catalog", "a, \"B\", pg_catalog"},
        {"\"a\"\"b\"", "\"a\"\"b\""}, {"'中文'", "\"中文\""},
        {"'line\nbreak'", "\"line\nbreak\""}, {"' a '", "\" a \""}
    }) {
        setting("SET search_path TO " + pair.first, pair.second);
        setting("SET SESSION \"SEARCH_PATH\" = " + pair.first + ";", pair.second);
        setting("SET LOCAL search_path TO " + pair.first, pair.second, dbms::SetStmt::Scope::Local);
    }
    setting("SET search_path FROM CURRENT", "");
    setting("SET search_path TO DEFAULT", "default");
    for (const std::string sql : {
        "SET search_path", "SET search_path public", "SET search_path TO",
        "SET search_path TO public,", "SET search_path TO public,,pg_catalog",
        "SET search_path TO public junk", "SET search_path TO 'a' 'b'",
        "SET search_path TO 'a' /*\n*/ 'b'", "SET search_path TO null",
        "SET search_path TO select", "SET search_path TO 'a' + 'b'",
        "SET search_path TO helper()", "SET search_path TO DEFAULT,public",
        "SET search_path TO public,DEFAULT", "SET search_path TO B'01'",
        "SET search_path TO 1_.2", "SET search_path TO +",
        "SET search_path TO 'a'; SELECT 1"
    }) {
        for (bool binding : {false, true}) {
            auto parsed = binding ? parser.parseForBinding(sql) : parser.parse(sql);
            assert(!parsed.success && parsed.sqlState == "42601");
            ++checks;
        }
    }
    std::vector<std::string> entries;
    std::string canonical;
    assert(dbms::parseSessionSearchPath("\"\"", entries, canonical));
    assert(entries == std::vector<std::string>{""} && canonical == "\"\"");
    ++checks;
    assert(dbms::parseSessionSearchPath("\"\", public", entries, canonical));
    assert((entries == std::vector<std::string>{"", "public"}) && canonical == "\"\", public");
    ++checks;
    assert(dbms::parseSessionSearchPath("", entries, canonical) && entries.empty() && canonical.empty());
    ++checks;
    for (const std::string bad : {",", "a,,b", "a,", "\"a\"junk", "\"unterminated"}) {
        assert(!dbms::parseSessionSearchPath(bad, entries, canonical));
        ++checks;
    }
    for (const std::string sql : {"SET search_path_extra TO public,pg_catalog",
                                 "SET search_path.extra TO public,pg_catalog"}) {
        auto parsed = parser.parse(sql);
        const auto* value = parsed.success ? dynamic_cast<const dbms::SetStmt*>(parsed.stmt.get()) : nullptr;
        assert(value && value->name != "search_path");
        ++checks;
    }
    std::cout << "[SEARCH PATH VALUES GRAMMAR] checks=" << checks << " passed\n";
}

