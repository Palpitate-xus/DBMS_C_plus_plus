// Exercise the real SQL dispatcher, including nested stack-local sessions.
#define main dbms_session_lifetime_program_main_not_invoked
#include "../../src/main.cpp"
#undef main

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database = "__t_session_execution_lifetime";
    assert(g_engine.createDatabase(database, "utf8") == dbms::DBStatus::OK);
    Session parent, caller;
    parent.username = "parent";
    caller.username = "caller";
    caller.permission = 1;
    caller.currentDB = database;
    dbms::setCurrentSession(&parent);
    size_t checks = 0;
    const auto run = [&](const std::string& sql, const std::string& state = "",
                         bool returnedError = false) {
        std::ostringstream output;
        std::string actualState;
        bool failed = false;
        try {
            dbms::ScopedOutputCapture capture(output);
            failed = execute(sql, caller);
        } catch (const dbms::DbError& error) {
            actualState = error.sqlState();
        }
        const bool correctError = returnedError
            ? failed && actualState.empty() && !state.empty() &&
              output.str().find("(SQLSTATE " + state + ")") != std::string::npos
            : !failed && actualState == state;
        if (!correctError) {
            std::cerr << "SESSION_LIFETIME_SQL " << sql << " failed=" << failed
                      << " state=" << actualState << " expected=" << state
                      << " output=" << output.str() << '\n';
        }
        assert(correctError);
        assert(dbms::currentSession() == &parent);
        ++checks;
    };
    run("");
    run("/* unterminated", "42601");
    run("SET application_name='caller setting'");
    assert(caller.applicationName == "caller setting" && parent.applicationName.empty());
    run("SET \"APPLICATION_NAME\" TO 'r142 quoted a=b compatibility_mode'");
    assert(caller.applicationName == "r142 quoted a=b compatibility_mode" && parent.applicationName.empty());
    run("SET SESSION \"CLIENT_ENCODING\" TO 'UTF8'");
    assert(caller.clientEncoding == "UTF8");
    run("SET \"SEARCH_PATH\" TO pg_catalog");
    assert(caller.searchPath == "pg_catalog" && parent.searchPath == "public");
    run("SET \"LC_MONETARY\" TO 'C'");
    assert(caller.lcMonetary == "C");
    run("SET \"search_path\" TO public");
    const auto compatibilityBeforeQuotedSetting = caller.compatibilityMode;
    caller.compatibilityMode = "extended";
    run("SET @r142_quote_guard = 7");
    assert(caller.userVariables.at("r142_quote_guard") == "7");
    {
        std::ostringstream output;
        dbms::ScopedOutputCapture capture(output);
        assert(execute("SET \"@r142_quote_guard\" = 9", caller));
        assert(output.str().find("Invalid value for parameter") != std::string::npos);
    }
    assert(caller.userVariables.at("r142_quote_guard") == "7");
    assert(caller.compatibilityMode == "extended" && dbms::currentSession() == &parent);
    ++checks; // the direct legacy bool-error call above executed one command
    caller.userVariables.erase("r142_quote_guard");
    caller.compatibilityMode = compatibilityBeforeQuotedSetting;
    run("SET application_name='caller setting'");
    run("SET search_path TO 'public, pg_catalog'");
    assert(caller.searchPath == "\"public, pg_catalog\"" && parent.searchPath == "public");
    run("SET search_path TO 'Upper'");
    assert(caller.searchPath == "\"Upper\"");
    run("SET search_path TO '\"two words\"'");
    assert(caller.searchPath == "\"\"\"two words\"\"\"");
    run("SET search_path TO ''");
    assert(caller.searchPath == "\"\"");
    run("SET search_path TO public");
    run("BEGIN");
    run("SET LOCAL search_path TO 'Upper'");
    assert(caller.searchPath == "\"Upper\"" && parent.searchPath == "public");
    run("SAVEPOINT r145_native");
    run("SET search_path TO 'a''b'");
    assert(caller.searchPath == "\"a'b\"");
    run("ROLLBACK TO r145_native");
    assert(caller.searchPath == "\"Upper\"");
    run("COMMIT");
    assert(caller.searchPath == "public" && parent.searchPath == "public");
    run("SET search_path TO public");
    run("BEGIN");
    run("SET LOCAL search_path TO 'Upper'");
    run("SET search_path FROM CURRENT");
    run("COMMIT");
    assert(caller.searchPath == "\"Upper\"" && parent.searchPath == "public");
    run("SET search_path TO public");
    run("BEGIN");
    run("SET LOCAL search_path TO 'Upper'");
    run("SET LOCAL search_path FROM CURRENT");
    run("COMMIT");
    assert(caller.searchPath == "public" && parent.searchPath == "public");
    run("SET search_path TO public");
    const std::string parentTimeZone = parent.timeZone;
    run("SET SESSION TimeZone TO 'Asia/Shanghai'");
    assert(caller.timeZone == "Asia/Shanghai" && caller.timezoneOffsetMinutes == 480);
    assert(parent.timeZone == parentTimeZone);
    run("SET SESSION TIME ZONE 'UTC'");
    assert(caller.timeZone == "UTC" && caller.timezoneOffsetMinutes == 0);
    run("SET SESSION TimeZone TO 'Invalid/No_Such_Zone'", "22023", true);
    assert(caller.timeZone == "UTC" && parent.timeZone == parentTimeZone);
    const int64_t nowSeconds = static_cast<int64_t>(std::chrono::system_clock::to_time_t(
        std::chrono::system_clock::now())) + Date(1970, 1, 1).convert() * 86400LL;
    const auto newYorkOffset = dbms::ianaTimezoneOffsetMinutes("America/New_York", nowSeconds);
    assert(newYorkOffset);
    const std::vector<std::pair<std::string, int>> savedZones = {
        {"UTC", 0}, {"Asia/Shanghai", 480}, {"Asia/Kathmandu", 345},
        {"America/New_York", *newYorkOffset}, {"+05:30", -330}, {"-03:30", 210},
        {"<+05:30>-05:30", 330}, {"<-03:30>+03:30", -210}, {"<+00>-00", 0},
        {"<+05>-05", 300}, {"<-06>+06", -360}
    };
    const std::vector<std::string> resetZones = {
        "SET TimeZone TO DEFAULT", "SET TimeZone = DEFAULT", "SET TIME ZONE DEFAULT",
        "SET SESSION TimeZone TO DEFAULT", "SET SESSION TimeZone = DEFAULT",
        "SET SESSION TIME ZONE DEFAULT", "SET TIME ZONE LOCAL", "SET SESSION TIME ZONE LOCAL",
        "RESET TimeZone", "RESET TIME ZONE", "RESET ALL"
    };
    for (const auto& saved : savedZones) {
        caller.defaultTimeZone = saved.first;
        for (const auto& command : resetZones) {
            run("SET TimeZone TO 'Pacific/Kiritimati'");
            assert(caller.timezoneOffsetMinutes == 840);
            run(command);
            assert(caller.timeZone == saved.first && caller.timezoneOffsetMinutes == saved.second);
            assert(caller.defaultTimeZone == saved.first && parent.timeZone == parentTimeZone);
        }
    }
    for (const std::string invalid : {"Invalid/No_Such_Zone", "<+05>-06", "<+05>garbage", "", "DEFAULT"}) {
        caller.defaultTimeZone = invalid;
        for (const std::string command : {"RESET TimeZone", "RESET ALL", "SET TimeZone TO DEFAULT"}) {
            run("SET TimeZone TO 'UTC'");
            caller.applicationName = "keep-default-error";
            run(command, "22023", true);
            assert(caller.timeZone == "UTC" && caller.timezoneOffsetMinutes == 0);
            assert(caller.applicationName == "keep-default-error" && parent.timeZone == parentTimeZone);
        }
    }
    caller.defaultTimeZone = "UTC";
    run("SET TimeZone TO 'UTC'");
    run("SELECT 1");
    // The native dispatcher reports this legacy error as bool + diagnostic;
    // lexical and prepared trigger failures below are real DbError throws.
    run("SELECT 1/0", "22012", true);
    assert(!g_engine.inTransaction());
    run("CREATE TABLE session_sink(id INT PRIMARY KEY,v INT CHECK(v >= 0))");
    run("INSERT INTO session_sink VALUES(1,5)");
    run("CREATE VIEW session_view AS SELECT id,v FROM session_sink");
    run("CREATE TRIGGER session_update INSTEAD OF UPDATE ON session_view FOR EACH ROW UPDATE session_sink SET v=NEW.v WHERE id=OLD.id");
    size_t calls = 0;
    g_engine.setTriggerExecutor([&](const std::string& action) {
        auto* active = dbms::currentSession();
        assert(active == &caller);
        Session child = *active;
        ++calls;
        const bool failed = execute(action, child);
        assert(dbms::currentSession() == active);
        return failed;
    });
    run("UPDATE session_view SET v=7");
    assert(calls == 1);
    const auto before = g_engine.query(database, "session_sink", {}, {});
    run("UPDATE session_view SET v=-1", "23514");
    assert(calls == 2 && !g_engine.inTransaction());
    assert(g_engine.query(database, "session_sink", {}, {}) == before);
    run("SELECT v FROM session_sink");
    run("UPDATE session_view SET v=9");
    assert(calls == 3);
    dbms::setCurrentSession(nullptr);
    {
        std::ostringstream output;
        dbms::ScopedOutputCapture capture(output);
        assert(!execute("SELECT 1", caller));
    }
    assert(dbms::currentSession() == nullptr);
    ++checks;
    std::cout << "[SESSION EXECUTION LIFETIME REAL FRONTEND] checks=" << checks << '\n';
}
