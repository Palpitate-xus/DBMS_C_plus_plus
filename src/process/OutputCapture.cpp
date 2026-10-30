#include "OutputCapture.h"
#include "StatementNotice.h"

#include <iostream>
#include <mutex>
#include <streambuf>
#include <utility>

namespace dbms {
namespace {

thread_local std::streambuf* g_threadCapture = nullptr;
thread_local const DiagnosticSink* g_threadDiagnosticSink = nullptr;

class MultiplexingStreambuf final : public std::streambuf {
public:
    explicit MultiplexingStreambuf(std::streambuf* fallback)
        : fallback_(fallback) {}

protected:
    int_type overflow(int_type ch) override {
        std::streambuf* target = g_threadCapture ? g_threadCapture : fallback_;
        if (ch == traits_type::eof()) {
            return target->pubsync() == 0 ? traits_type::not_eof(ch) : ch;
        }
        return target->sputc(traits_type::to_char_type(ch));
    }

    std::streamsize xsputn(const char* data, std::streamsize count) override {
        std::streambuf* target = g_threadCapture ? g_threadCapture : fallback_;
        return target->sputn(data, count);
    }

    int sync() override {
        std::streambuf* target = g_threadCapture ? g_threadCapture : fallback_;
        return target->pubsync();
    }

private:
    std::streambuf* fallback_;
};

std::once_flag g_installOnce;
MultiplexingStreambuf* g_multiplexingBuffer = nullptr;

void ensureInstalled() {
    std::call_once(g_installOnce, [] {
        g_multiplexingBuffer =
            new MultiplexingStreambuf(std::cout.rdbuf());
        std::cout.rdbuf(g_multiplexingBuffer);
    });
}

} // namespace

ScopedDiagnosticCapture::ScopedDiagnosticCapture(DiagnosticSink sink)
    : sink_(std::move(sink)), previous_(g_threadDiagnosticSink) {
    g_threadDiagnosticSink = &sink_;
}

ScopedDiagnosticCapture::~ScopedDiagnosticCapture() {
    g_threadDiagnosticSink = previous_;
}

void emitDiagnostic(StatementDiagnostic diagnostic) {
    if (g_threadDiagnosticSink) {
        (*g_threadDiagnosticSink)(std::move(diagnostic));
    } else {
        // Embedded callers retain a printable fallback; protocol/CLI owners
        // install a sink independently of stdout row-output capture.
        std::cout << diagnostic.severity << ": " << diagnostic.message << '\n';
    }
}

void emitNotice(std::string message) {
    emitDiagnostic({"NOTICE", "00000", std::move(message)});
}

void emitWarning(std::string message) {
    emitDiagnostic({"WARNING", "01000", std::move(message)});
}

namespace {
thread_local std::vector<StatementNotice>* g_statementNoticeTarget = nullptr;
thread_local std::streambuf* g_statementNoticeOutput = nullptr;
}

ScopedStatementNoticeCapture::ScopedStatementNoticeCapture(
        std::vector<StatementNotice>& target)
    : previous_(g_statementNoticeTarget),
      previousOutput_(g_statementNoticeOutput) {
    g_statementNoticeTarget = &target;
    g_statementNoticeOutput = nullptr;
}

ScopedStatementNoticeCapture::ScopedStatementNoticeCapture(
        std::vector<StatementNotice>& target, std::ostream& output)
    : ScopedStatementNoticeCapture(target) {
    g_statementNoticeOutput = output.rdbuf();
}

ScopedStatementNoticeCapture::~ScopedStatementNoticeCapture() {
    g_statementNoticeTarget = previous_;
    g_statementNoticeOutput = previousOutput_;
}

void emitStatementNotice(StatementNotice notice) {
    // Preserve the pre-existing Root diagnostic receiver without adopting
    // its draft API into the accepted implementation. Standalone new
    // receivers still own their messages; protocol-bound ones share the
    // already active Root receiver to preserve the emission order.
    if (g_threadDiagnosticSink &&
        (!g_statementNoticeTarget || g_statementNoticeOutput)) {
        emitDiagnostic({std::move(notice.severity), std::move(notice.sqlState),
                        std::move(notice.message)});
        return;
    }
    notice.outputOffset = -1;
    if (g_statementNoticeTarget) {
        if (g_statementNoticeOutput) {
            const auto offset = g_statementNoticeOutput->pubseekoff(
                0, std::ios_base::cur, std::ios_base::out);
            if (offset != std::streampos(std::streamoff(-1)))
                notice.outputOffset = static_cast<std::streamoff>(offset);
        }
        g_statementNoticeTarget->push_back(std::move(notice));
    } else {
        std::cout << notice.severity << ": " << notice.message << '\n';
    }
}

ScopedOutputCapture::ScopedOutputCapture(std::ostream& target) {
    ensureInstalled();
    previous_ = g_threadCapture;
    g_threadCapture = target.rdbuf();
}

ScopedOutputCapture::~ScopedOutputCapture() {
    g_threadCapture = previous_;
}

} // namespace dbms
