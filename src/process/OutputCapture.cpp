#include "OutputCapture.h"

#include <iostream>
#include <mutex>
#include <streambuf>

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

ScopedOutputCapture::ScopedOutputCapture(std::ostream& target) {
    ensureInstalled();
    previous_ = g_threadCapture;
    g_threadCapture = target.rdbuf();
}

ScopedOutputCapture::~ScopedOutputCapture() {
    g_threadCapture = previous_;
}

} // namespace dbms
