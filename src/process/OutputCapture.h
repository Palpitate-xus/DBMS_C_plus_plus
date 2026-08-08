#pragma once

#include <iosfwd>

namespace dbms {

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
