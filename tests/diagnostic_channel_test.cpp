#include "process/OutputCapture.h"
#include <array>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

// Pure component test: also buildable with OutputCapture.cpp alone, without
// StorageEngine, a protocol server, test stubs, or any database I/O.
int main() {
    using namespace dbms;
    std::vector<StatementDiagnostic> outer, inner;
    std::ostringstream rows;
    bool caught = false;
    {
        ScopedDiagnosticCapture capture([&](StatementDiagnostic diagnostic) {
            outer.push_back(std::move(diagnostic));
        });
        ScopedOutputCapture output(rows);
        std::cout << "NOTICE: ordinary row\nERROR: ordinary row\n";
        emitNotice("first\nsecond");
        {
            ScopedDiagnosticCapture nested([&](StatementDiagnostic diagnostic) {
                inner.push_back(std::move(diagnostic));
            });
            emitWarning("head\nNOTICE: part of one message");
        }
        emitNotice("outer restored");
        try {
            ScopedDiagnosticCapture throwing([](StatementDiagnostic) {
                throw std::runtime_error("sink failure");
            });
            emitNotice("throw");
        } catch (const std::runtime_error&) { caught = true; }
        emitNotice("after throw");
    }
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const char* label) {
        ++controls; if (!good) ++failures;
        std::cout << "DIAGNOSTIC_CHANNEL " << label << " pass=" << good << std::endl;
    };
    check(rows.str() == "NOTICE: ordinary row\nERROR: ordinary row\n", "row-bytes-not-diagnostics");
    check(outer.size() == 3, "outer-exact-count");
    check(outer.size() == 3 && outer[0].severity == "NOTICE" && outer[0].sqlState == "00000" &&
          outer[0].message == "first\nsecond", "multiline-notice");
    check(inner.size() == 1 && inner[0].severity == "WARNING" && inner[0].sqlState == "01000" &&
          inner[0].message == "head\nNOTICE: part of one message", "nested-warning");
    check(outer.size() == 3 && outer[1].message == "outer restored", "nested-scope-restored");
    check(caught && outer.size() == 3 && outer[2].message == "after throw", "exception-restores-sink");
    std::ostringstream fallback;
    {
        ScopedOutputCapture capture(fallback);
        emitNotice("fallback"); emitWarning("fallback warning");
    }
    check(fallback.str() == "NOTICE: fallback\nWARNING: fallback warning\n", "embedded-printable-fallback");
    std::array<std::vector<StatementDiagnostic>, 2> workers;
    std::vector<StatementDiagnostic> leader;
    {
        ScopedDiagnosticCapture capture([&](StatementDiagnostic diagnostic) { leader.push_back(std::move(diagnostic)); });
        std::array<std::thread, 2> threads;
        for (size_t thread = 0; thread < threads.size(); ++thread) {
            threads[thread] = std::thread([&, thread]() {
                ScopedDiagnosticCapture local([&](StatementDiagnostic diagnostic) {
                    workers[thread].push_back(std::move(diagnostic));
                });
                for (size_t row = 0; row < 64; ++row)
                    emitWarning(std::to_string(thread) + ":" + std::to_string(row));
            });
        }
        for (auto& thread : threads) thread.join();
        emitNotice("leader");
    }
    check(leader.size() == 1 && leader[0].message == "leader", "leader-isolation");
    for (size_t thread = 0; thread < workers.size(); ++thread) {
        bool good = workers[thread].size() == 64;
        for (size_t row = 0; row < workers[thread].size(); ++row)
            good = good && workers[thread][row].severity == "WARNING" && workers[thread][row].sqlState == "01000" &&
                workers[thread][row].message == std::to_string(thread) + ":" + std::to_string(row);
        check(good, thread ? "worker-one-isolation" : "worker-zero-isolation");
    }
    StatementDiagnostic custom;
    {
        ScopedDiagnosticCapture capture([&](StatementDiagnostic diagnostic) { custom = std::move(diagnostic); });
        emitDiagnostic({"INFO", "00000", " unicode: \xE4\xB8\xAD\nNULL "});
    }
    check(custom.severity == "INFO" && custom.sqlState == "00000" &&
          custom.message == " unicode: \xE4\xB8\xAD\nNULL ", "payload-bytes-preserved");
    fallback.str(""); fallback.clear();
    {
        ScopedOutputCapture capture(fallback);
        emitNotice("last");
    }
    check(fallback.str() == "NOTICE: last\n", "all-sinks-restored");
    std::cout << "DIAGNOSTIC_CHANNEL_COMPLETE " << controls << " FAILURES " << failures << std::endl;
    if (controls != 12) return 2;
    return failures ? 1 : 0;
}
