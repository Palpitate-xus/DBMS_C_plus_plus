#include "process/StatementNotice.h"
#include "process/OutputCapture.h"
#include <array>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>

int main() {
    using namespace dbms;
    int controls = 0, failures = 0;
    auto check = [&](bool good, const char* label) {
        ++controls;
        if (!good) ++failures;
        std::cout << "STATEMENT_NOTICE " << label << " pass=" << good << '\n';
    };
    std::vector<StatementNotice> outer, inner;
    std::ostringstream rows;
    bool caught = false;
    {
        ScopedOutputCapture stdoutCapture(rows);
        ScopedStatementNoticeCapture capture(outer);
        std::cout << "NOTICE: row data\n";
        emitStatementNotice({"NOTICE", "42P07", "duplicate\nmessage (SQLSTATE 00000)"});
        {
            ScopedStatementNoticeCapture nested(inner);
            emitStatementNotice({"WARNING", "22023", "a warning"});
        }
        emitStatementNotice({"NOTICE", "00000", "user (SQLSTATE 42P07)"});
        try {
            ScopedStatementNoticeCapture nested(inner);
            emitStatementNotice({"NOTICE", "42P07", "before exception"});
            throw std::runtime_error("unwind");
        } catch (const std::runtime_error&) { caught = true; }
        emitStatementNotice({"NOTICE", "00000", "after exception"});
    }
    check(rows.str() == "NOTICE: row data\n", "stdout-bytes-not-metadata");
    check(outer.size() == 3, "outer-count");
    check(inner.size() == 2, "nested-count");
    check(outer.size() == 3 && outer[0].severity == "NOTICE" && outer[0].sqlState == "42P07" &&
          outer[0].message == "duplicate\nmessage (SQLSTATE 00000)", "code-and-multiline-payload");
    check(outer.size() == 3 && outer[1].sqlState == "00000" &&
          outer[1].message == "user (SQLSTATE 42P07)", "user-text-not-code");
    check(caught && outer.size() == 3 && outer[2].message == "after exception", "exception-restores-owner");
    check(inner.size() == 2 && inner[0].severity == "WARNING" && inner[0].sqlState == "22023", "warning-code");
    check(inner.size() == 2 && inner[1].message == "before exception", "nested-unwind-keeps-receiver");
    std::vector<StatementNotice> empty;
    { ScopedStatementNoticeCapture scope(empty); }
    check(empty.empty(), "empty-statement-no-stale-messages");
    std::ostringstream fallback;
    {
        ScopedOutputCapture capture(fallback);
        emitStatementNotice({"NOTICE", "42P07", "embedded"});
        emitStatementNotice({"WARNING", "22023", "warning"});
    }
    check(fallback.str() == "NOTICE: embedded\nWARNING: warning\n", "CLI-embedded-fallback");
    std::array<std::vector<StatementNotice>, 8> workers;
    std::vector<StatementNotice> parent;
    {
        ScopedStatementNoticeCapture parentScope(parent);
        std::array<std::thread, 8> threads;
        for (size_t id = 0; id < threads.size(); ++id) {
            threads[id] = std::thread([&, id] {
                ScopedStatementNoticeCapture scope(workers[id]);
                for (size_t n = 0; n < 256; ++n)
                    emitStatementNotice({"NOTICE", n % 2 ? "00000" : "42P07",
                        std::to_string(id) + ":" + std::to_string(n)});
            });
        }
        for (auto& thread : threads) thread.join();
        emitStatementNotice({"NOTICE", "00000", "parent"});
    }
    check(parent.size() == 1 && parent[0].message == "parent", "parent-thread-isolation");
    for (size_t id = 0; id < workers.size(); ++id) {
        bool good = workers[id].size() == 256;
        for (size_t n = 0; n < workers[id].size(); ++n)
            good = good && workers[id][n].severity == "NOTICE" &&
                workers[id][n].sqlState == (n % 2 ? "00000" : "42P07") &&
                workers[id][n].message == std::to_string(id) + ":" + std::to_string(n);
        check(good, "worker-isolation-exact-payloads");
    }
    fallback.str(""); fallback.clear();
    {
        ScopedOutputCapture capture(fallback);
        emitStatementNotice({"NOTICE", "42P07", "last"});
    }
    check(fallback.str() == "NOTICE: last\n", "all-scopes-restored");
    std::cout << "STATEMENT_NOTICE_COMPLETE " << controls << " FAILURES " << failures << std::endl;
    if (controls != 20) return 2;
    return failures ? 1 : 0;
}
