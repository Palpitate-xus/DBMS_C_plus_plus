#pragma once

#include <iosfwd>
#include <string>
#include <vector>

namespace dbms {

// Internal diagnostic metadata; never recovered from SQL or stdout text.
struct StatementNotice {
    std::string severity;
    std::string sqlState;
    std::string message;
    // Byte position in the explicitly bound, in-memory output capture.
    // This is transport-local ordering metadata, never persisted or sent.
    std::streamoff outputOffset = -1;
};

// The receiver belongs to one execution scope on one thread. Nested scopes
// restore their caller even when execution unwinds through an exception.
class ScopedStatementNoticeCapture {
public:
    explicit ScopedStatementNoticeCapture(std::vector<StatementNotice>& target);
    ScopedStatementNoticeCapture(std::vector<StatementNotice>& target,
                                 std::ostream& output);
    ~ScopedStatementNoticeCapture();
    ScopedStatementNoticeCapture(const ScopedStatementNoticeCapture&) = delete;
    ScopedStatementNoticeCapture& operator=(const ScopedStatementNoticeCapture&) = delete;
private:
    std::vector<StatementNotice>* previous_ = nullptr;
    std::streambuf* previousOutput_ = nullptr;
};

// Embedded and CLI callers without a receiver keep the printable diagnostic.
void emitStatementNotice(StatementNotice notice);

} // namespace dbms
