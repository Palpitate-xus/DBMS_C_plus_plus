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
