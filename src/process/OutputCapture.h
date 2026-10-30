#pragma once

#include <iosfwd>
#include <functional>
#include <string>

namespace dbms {

// Diagnostics are not row/command-output text: messages can contain newlines,
// while ordinary row data can begin with NOTICE: or ERROR:.
struct StatementDiagnostic {
    std::string severity;
    std::string sqlState;
    std::string message;
};
using DiagnosticSink = std::function<void(StatementDiagnostic)>;

class ScopedDiagnosticCapture {
public:
    explicit ScopedDiagnosticCapture(DiagnosticSink sink);
    ~ScopedDiagnosticCapture();
    ScopedDiagnosticCapture(const ScopedDiagnosticCapture&) = delete;
    ScopedDiagnosticCapture& operator=(const ScopedDiagnosticCapture&) = delete;
private:
    DiagnosticSink sink_;
    const DiagnosticSink* previous_ = nullptr;
};

void emitDiagnostic(StatementDiagnostic diagnostic);
void emitNotice(std::string message);
void emitWarning(std::string message);

// Redirect std::cout only for the current thread.  The legacy executor still
// emits textual results through std::cout, while protocol sessions must be
// able to capture those results concurrently without replacing the process-
// wide stream buffer.
class ScopedOutputCapture {
public:
    explicit ScopedOutputCapture(std::ostream& target);
    ~ScopedOutputCapture();

    ScopedOutputCapture(const ScopedOutputCapture&) = delete;
    ScopedOutputCapture& operator=(const ScopedOutputCapture&) = delete;

private:
    std::streambuf* previous_ = nullptr;
};

} // namespace dbms
