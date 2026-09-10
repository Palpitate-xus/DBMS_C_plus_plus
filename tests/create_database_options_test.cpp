#include "commands/DdlExecutor.h"
#include "commands/TableManage.h"
#include "parser/ast.h"
#include "parser/parser.h"
#include "process/OutputCapture.h"
#include "Session.h"
#include "test_utils.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

extern dbms::StorageEngine g_engine;

namespace {

std::string executeCaptured(dbms::DdlExecutor& executor, Session& session,
                            const std::string& sql, bool& failed) {
    std::ostringstream output;
    {
        dbms::ScopedOutputCapture capture(output);
        failed = executor.executeSql(sql, session);
    }
    return output.str();
}

void assertNotCreated(const std::string& database) {
    assert(!std::filesystem::exists(database));
    assert(!std::filesystem::exists(database + ".archive"));
}

void testParserPreservesAllOptions() {
    dbms::SQLParser parser;
    const auto parsed = parser.parse(
        "CREATE DATABASE Cat08_All WITH OWNER = alice TEMPLATE template0 "
        "ENCODING 'UTF8' STRATEGY WAL_LOG LOCALE 'C' LC_COLLATE = 'C' "
        "LC_CTYPE 'C' BUILTIN_LOCALE 'C' ICU_LOCALE 'und' "
        "ICU_RULES '&V << w' LOCALE_PROVIDER builtin "
        "COLLATION_VERSION '1.0' TABLESPACE pg_default "
        "ALLOW_CONNECTIONS true CONNECTION LIMIT -1 IS_TEMPLATE false "
        "OID 16384 LOCATION '/unused';");
    assert(parsed.success);
    const auto* statement =
        dynamic_cast<const dbms::CreateDatabaseStmt*>(parsed.stmt.get());
    assert(statement != nullptr);
    assert(statement->databaseName == "cat08_all");
    assert(statement->withClause);
    assert(statement->options.size() == 18);
    assert(statement->options.at("connection_limit").value() == "-1");
    assert(statement->options.at("encoding").value() == "'UTF8'");
    assert(statement->options.at("icu_rules").value() == "'&V << w'");

    const auto withOnly = parser.parse("CREATE DATABASE with_only WITH;");
    assert(withOnly.success);
    const auto* withOnlyStatement =
        dynamic_cast<const dbms::CreateDatabaseStmt*>(withOnly.stmt.get());
    assert(withOnlyStatement && withOnlyStatement->withClause &&
           withOnlyStatement->options.empty());

    const auto defaults = parser.parse(
        "CREATE DATABASE defaults ENCODING = DEFAULT;");
    assert(defaults.success);
    const auto* defaultsStatement =
        dynamic_cast<const dbms::CreateDatabaseStmt*>(defaults.stmt.get());
    assert(defaultsStatement &&
           !defaultsStatement->options.at("encoding").has_value());

    const auto underscored = parser.parse(
        "CREATE DATABASE underscored CONNECTION_LIMIT 2;");
    assert(underscored.success);
    const auto* underscoredStatement =
        dynamic_cast<const dbms::CreateDatabaseStmt*>(underscored.stmt.get());
    assert(underscoredStatement &&
           underscoredStatement->options.at("connection_limit").value() == "2");

    const auto quotedOption = parser.parse(
        "CREATE DATABASE quoted_option \"encoding\" UTF8;");
    assert(quotedOption.success);
    const auto* quotedOptionStatement =
        dynamic_cast<const dbms::CreateDatabaseStmt*>(quotedOption.stmt.get());
    assert(quotedOptionStatement &&
           quotedOptionStatement->options.count("encoding") == 1);

    const auto keywordName = parser.parse("CREATE DATABASE if;");
    assert(keywordName.success);
    const auto* keywordNameStatement =
        dynamic_cast<const dbms::CreateDatabaseStmt*>(keywordName.stmt.get());
    assert(keywordNameStatement && keywordNameStatement->databaseName == "if");
}

void testParserRejectsMalformedOptions() {
    dbms::SQLParser parser;
    const std::vector<std::string> invalid = {
        "CREATE DATABASE IF NOT EXISTS bad_if",
        "CREATE DATABASE with",
        "CREATE DATABASE 123",
        "CREATE DATABASE bad.qualified",
        "CREATE DATABASE bad UNKNOWN_OPTION value",
        "CREATE DATABASE bad OWNER SELECT",
        "CREATE DATABASE bad OWNER 123abc",
        "CREATE DATABASE bad ENCODING UTF8 ENCODING UTF8",
        "CREATE DATABASE bad OWNER",
        "CREATE DATABASE bad CONNECTION value",
        "CREATE DATABASE bad CONNECTION LIMIT",
        "CREATE DATABASE bad ENCODING =",
        "CREATE DATABASE bad ENCODING UTF8, OWNER alice",
        "CREATE DATABASE bad ENCODING UTF8; trailing"
    };
    for (const auto& sql : invalid) {
        const auto parsed = parser.parse(sql);
        assert(!parsed.success);
        assert(!parsed.stmt);
    }
}

void testExecutorFailsClosedBeforeStorage() {
    Session session;
    session.username = "testuser";
    session.permission = 1;
    dbms::DdlExecutor executor;

    const std::vector<std::string> unsupportedOptions = {
        "OWNER alice", "TEMPLATE template0", "STRATEGY WAL_LOG",
        "LOCALE 'C'", "LC_COLLATE 'C'", "LC_CTYPE 'C'",
        "BUILTIN_LOCALE 'C'", "ICU_LOCALE 'und'", "ICU_RULES '&V << w'",
        "LOCALE_PROVIDER builtin", "COLLATION_VERSION '1.0'",
        "TABLESPACE pg_default", "ALLOW_CONNECTIONS true",
        "CONNECTION LIMIT -1", "IS_TEMPLATE false", "OID 16384",
        "LOCATION '/unused'"
    };
    for (size_t i = 0; i < unsupportedOptions.size(); ++i) {
        const std::string database = testDbPath(
            "cat08_unsupported_" + std::to_string(i));
        bool failed = false;
        const std::string output = executeCaptured(
            executor, session,
            "CREATE DATABASE " + database + " " + unsupportedOptions[i],
            failed);
        assert(failed);
        assert(output.find("SQLSTATE 0A000") != std::string::npos);
        assertNotCreated(database);
    }

    for (const auto& encoding : {
             "LATIN1", "'SQL_ASCII'", "'ISO-8859-1'", "windows1252",
             "0", "34"}) {
        const std::string database = testDbPath(
            "cat08_encoding_" + std::to_string(encoding[0]) +
            std::to_string(std::char_traits<char>::length(encoding)));
        bool failed = false;
        const std::string output = executeCaptured(
            executor, session,
            "CREATE DATABASE " + database + " ENCODING " + encoding,
            failed);
        assert(failed);
        assert(output.find("SQLSTATE 0A000") != std::string::npos);
        assertNotCreated(database);
    }

    const std::string invalidEncoding = testDbPath("cat08_invalid_encoding");
    bool failed = false;
    std::string output = executeCaptured(
        executor, session,
        "CREATE DATABASE " + invalidEncoding + " ENCODING no_such_encoding",
        failed);
    assert(failed);
    assert(output.find("SQLSTATE 42704") != std::string::npos);
    assertNotCreated(invalidEncoding);

    const std::string quotedCode = testDbPath("cat08_quoted_encoding_code");
    output = executeCaptured(
        executor, session,
        "CREATE DATABASE " + quotedCode + " ENCODING '6'", failed);
    assert(failed);
    assert(output.find("SQLSTATE 42704") != std::string::npos);
    assertNotCreated(quotedCode);

    const std::string direct = testDbPath("cat08_direct_non_utf8");
    assert(g_engine.createDatabase(direct, "latin1") ==
           dbms::DBStatus::INVALID_VALUE);
    assertNotCreated(direct);
}

void testUtf8FormsAreCanonical() {
    Session session;
    session.username = "testuser";
    session.permission = 1;
    dbms::DdlExecutor executor;
    const std::vector<std::string> clauses = {
        "", "WITH", "ENCODING DEFAULT", "ENCODING UTF8",
        "ENCODING 'UTF-8'", "ENCODING 'u_t-f 8'", "ENCODING UNICODE",
        "ENCODING +6", "\"encoding\" UTF8"
    };
    for (size_t i = 0; i < clauses.size(); ++i) {
        const std::string database =
            testDbPath("cat08_utf8_" + std::to_string(i));
        bool failed = true;
        const std::string sql = "CREATE DATABASE " + database +
            (clauses[i].empty() ? "" : " " + clauses[i]);
        const std::string output = executeCaptured(
            executor, session, sql, failed);
        assert(!failed);
        assert(output.find("CREATE DATABASE succeeded") != std::string::npos);
        assert(std::filesystem::is_directory(database));
        std::ifstream charset(
            std::filesystem::path(database) / ".charset");
        std::string value;
        assert(std::getline(charset, value));
        assert(value == "utf8");
        assert(g_engine.dropDatabase(database) == dbms::DBStatus::OK);
    }
}

} // namespace

int main() {
    cleanupAllTestData();
    testParserPreservesAllOptions();
    testParserRejectsMalformedOptions();
    testExecutorFailsClosedBeforeStorage();
    testUtf8FormsAreCanonical();
    finalCleanupTestData();
    std::cout << "[CREATE DATABASE OPTIONS] passed\n";
    return 0;
}
