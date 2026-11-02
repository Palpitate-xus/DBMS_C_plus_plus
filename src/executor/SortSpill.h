#pragma once

#include "common/DbError.h"
#include "Session.h"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <string>
#include <unistd.h>
#include <vector>

namespace dbms::sort_detail {

[[noreturn]] inline void ioError(const char* operation, int error = errno) {
    const char* state = error == ENOSPC ? "53100" :
        error == EMFILE || error == ENFILE ? "53000" : "58030";
    throw DbError(state, std::string("could not ") + operation +
        " sort temporary file: " + std::strerror(error));
}

// Private, process-local sort tapes. Bootstrap puts the process in its data
// directory. Unlink immediately, including before allocating any run buffers:
// normal close, exceptions, cancellation and process death leave no pathname.
// At most two files are needed, independently of the number of input runs.
class File {
    int fd_ = -1;
public:
    File() {
        char name[] = ".dbms-sort-XXXXXX";
        fd_ = ::mkstemp(name);
        if (fd_ < 0) ioError("create");
        if (::unlink(name) != 0) {
            const int error = errno;
            ::close(fd_); fd_ = -1;
            ioError("unlink", error);
        }
        if (::fcntl(fd_, F_SETFD, FD_CLOEXEC) < 0) {
            const int error = errno;
            ::close(fd_); fd_ = -1;
            ioError("protect descriptor of", error);
        }
    }
    ~File() { if (fd_ >= 0) ::close(fd_); }
    File(const File&) = delete;
    File& operator=(const File&) = delete;
    int fd() const { return fd_; }
    void truncate() {
        checkForQueryInterrupt();
        int result;
        do { result = ::ftruncate(fd_, 0); } while (result < 0 && errno == EINTR);
        if (result < 0) ioError("truncate");
    }
};

inline void writeAt(int fd, uint64_t offset, const char* data, size_t length) {
    if (offset > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) ||
        length > static_cast<uint64_t>(std::numeric_limits<off_t>::max()) - offset)
        throw DbError("54000", "sort temporary file is too large");
    while (length) {
        checkForQueryInterrupt();
        const size_t part = std::min(length, static_cast<size_t>(SSIZE_MAX));
        const auto wrote = ::pwrite(fd, data, part, static_cast<off_t>(offset));
        if (wrote < 0 && errno == EINTR) continue;
        if (wrote <= 0) ioError("write", wrote == 0 ? EIO : errno);
        data += wrote; length -= static_cast<size_t>(wrote); offset += wrote;
    }
}

class Writer {
    int fd_;
    uint64_t offset_ = 0;
    std::vector<char> buffer_;
    size_t used_ = 0;
public:
    Writer(int fd, size_t bufferSize) : fd_(fd), buffer_(bufferSize) {}
    uint64_t position() const { return offset_ + used_; }
    void flush() {
        if (!used_) return;
        writeAt(fd_, offset_, buffer_.data(), used_);
        offset_ += used_; used_ = 0;
    }
    void append(const void* value, size_t length) {
        const auto* data = static_cast<const char*>(value);
        if (length > buffer_.size()) {
            flush(); writeAt(fd_, offset_, data, length); offset_ += length;
            return;
        }
        if (length > buffer_.size() - used_) flush();
        if (length) std::memcpy(buffer_.data() + used_, data, length);
        used_ += length;
    }
    template <typename Value> void scalar(const Value& value) { append(&value, sizeof(value)); }
};

class Reader {
    int fd_;
    uint64_t position_, end_;
    std::vector<char> buffer_;
    size_t used_ = 0, available_ = 0;
public:
    Reader(int fd, uint64_t begin, uint64_t end, size_t bufferSize)
        : fd_(fd), position_(begin), end_(end), buffer_(bufferSize) {}
    uint64_t remaining() const { return end_ - position_; }
    void read(void* output, size_t length) {
        if (length > remaining()) throw DbError("58030", "truncated sort temporary run");
        auto* data = static_cast<char*>(output);
        while (length) {
            checkForQueryInterrupt();
            if (used_ == available_) {
                const size_t wanted = static_cast<size_t>(std::min<uint64_t>(buffer_.size(), remaining()));
                ssize_t result;
                do {
                    checkForQueryInterrupt();
                    result = ::pread(fd_, buffer_.data(), wanted, static_cast<off_t>(position_));
                } while (result < 0 && errno == EINTR);
                if (result <= 0) ioError("read", result == 0 ? EIO : errno);
                used_ = 0; available_ = static_cast<size_t>(result);
            }
            const size_t part = std::min(length, available_ - used_);
            std::memcpy(data, buffer_.data() + used_, part);
            used_ += part; position_ += part; data += part; length -= part;
        }
    }
    template <typename Value> Value scalar() { Value value{}; read(&value, sizeof(value)); return value; }
    std::string string(size_t length) {
        std::string result(length, '\0'); read(result.data(), length); return result;
    }
};

// Native private framing, never a durable format or a PostgreSQL disk codec.
// Headers link runs by byte extent, so no input-sized run directory is kept
// in memory. Every merge pass can discover its runs sequentially.
struct RunHeader { uint64_t bytes = 0, rows = 0; };

} // namespace dbms::sort_detail
