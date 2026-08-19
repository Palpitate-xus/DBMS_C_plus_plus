// ============================================================================
// dollar_quote_test — PostgreSQL dollar-quoted string literals ($$...$$ and
// $tag$...$tag$) in the tokenizer, and their use as CREATE FUNCTION bodies.
//
// The tokenizer converts a dollar-quoted literal into an ordinary single-
// quoted string token (with embedded quotes doubled), so every downstream
// consumer that already handles string literals — function bodies, procedure
// bodies, trigger actions — accepts dumps written with dollar-quoting.
// ============================================================================

#include "parser/parser.h"
#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "Session.h"
#include <cassert>
#include <iostream>
#include "test_utils.h"

extern dbms::StorageEngine g_engine;

static void cleanup(const std::string& db) { if (std::filesystem::exists(db)) std::filesystem::remove_all(db); }

static void test_tokenizer_forms() {
    using dbms::SQLParser;

    // Anonymous $$: semicolons, quotes and newlines survive as one token.
    {
        auto t = SQLParser::tokenize("CREATE FUNCTION f() AS $$ BEGIN RETURN 1; END $$");
        bool found = false;
        for (auto& s : t) if (s == "' BEGIN RETURN 1; END '") found = true;
        assert(found);
    }
    // Tagged $fn$ ... $fn$
    {
        auto t = SQLParser::tokenize("AS $fn$ SELECT 'it''s; fine' $fn$ LANGUAGE sql");
        bool found = false;
        for (auto& s : t)
            if (s.find("fine") != std::string::npos &&
                s.find(";") != std::string::npos &&
                s.front() == '\'' && s.back() == '\'') found = true;
        assert(found);
    }
    // $$ inside a normal string stays literal
    {
        auto t = SQLParser::tokenize("SELECT 'a $$ b'");
        bool ok = false;
        for (auto& s : t) if (s == "'a $$ b'") ok = true;
        assert(ok);
    }
    // Unterminated $ falls back to ordinary tokenization
    {
        auto t = SQLParser::tokenize("SELECT a$b");
        assert(!t.empty());
    }
    // Newlines in body
    {
        auto t = SQLParser::tokenize("AS $$line1\nline2 'quoted'$$");
        assert(t.back() == "'line1\nline2 ''quoted'''");
    }
    // Adjacent to delimiters
    {
        auto t = SQLParser::tokenize("AS $$x$$;");
        assert(t.size() == 3 && t[1] == "'x'");
    }
    // Tag with underscore
    {
        auto t = SQLParser::tokenize("AS $body_1$ hi $body_1$");
        bool ok = false;
        for (auto& s : t) if (s == "' hi '") ok = true;
        assert(ok);
    }
    std::cout << "[DQ] tokenizer forms OK" << std::endl;
}

static void test_function_body_dollar_quoted() {
    std::string db = testDbPath("dq_func");
    cleanup(db);
    assert(g_engine.createDatabase(db, "utf8") == dbms::DBStatus::OK);

    Session s;
    s.username = "testuser";
    s.permission = 1;
    s.currentDB = db;
    dbms::DdlExecutor ddl;

    // Body containing quotes and semicolons, dollar-quoted anonymously.
    assert(!ddl.executeSql(
        "CREATE FUNCTION note() RETURNS varchar AS $$ SELECT 'it''s; fine' $$ LANGUAGE sql", s));
    assert(g_engine.udfExists(db, "note"));
    auto info = g_engine.getUDF(db, "note");
    // Stored body keeps the SQL-level '' escaping of the source text, the
    // same result the equivalent plain-quoted spelling produces — the two
    // literal forms are interchangeable for function bodies.
    assert(info.expression.find("it''s; fine") != std::string::npos);

    // Tagged form must behave identically.
    assert(!ddl.executeSql(
        "CREATE FUNCTION note2() RETURNS varchar AS $fn$ SELECT 'ok' $fn$ LANGUAGE sql", s));
    assert(g_engine.udfExists(db, "note2"));
    auto info2 = g_engine.getUDF(db, "note2");
    assert(info2.expression.find("'ok'") != std::string::npos);

    // Multi-line body with embedded semicolons (procedure).
    assert(!ddl.executeSql(
        "CREATE PROCEDURE multi() AS $$\ninsert into nothing_here values (1);\ninsert into nothing_here values (2)\n$$ LANGUAGE sql", s));
    assert(g_engine.procedureExists(db, "multi"));

    cleanup(db);
    std::cout << "[DQ] function/procedure bodies OK" << std::endl;
}

int main() {
    test_tokenizer_forms();
    test_function_body_dollar_quoted();
    std::cout << "[DQ] all tests passed" << std::endl;
    return 0;
}
