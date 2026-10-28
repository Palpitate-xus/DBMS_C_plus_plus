#include "OutputCapture.h"
#include "StatementNotice.h"

#include <iostream>
#include <mutex>
#include <streambuf>
#include <utility>

namespace dbms {
namespace {

thread_local std::streambuf* g_threadCapture = nullptr;

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
