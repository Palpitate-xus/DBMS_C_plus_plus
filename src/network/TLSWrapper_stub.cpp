// Stub implementation for systems without OpenSSL
#include "TLSWrapper.h"
#include <algorithm>
#include <cerrno>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <sys/socket.h>

namespace dbms {

SecureSocket::SecureSocket(int sockfd, SSL_CTX*) : fd(sockfd), ssl(nullptr), useTLS(false), tlsOK(false) {}
SecureSocket::SecureSocket(SecureSocket&& other) noexcept
    : fd(other.fd), ssl(nullptr), useTLS(false), tlsOK(false) {
    other.fd = -1;
}
SecureSocket& SecureSocket::operator=(SecureSocket&& other) noexcept {
    if (this != &other) {
        close();
        fd = other.fd;
        other.fd = -1;
    }
    return *this;
}
bool SecureSocket::initTLS(SSL_CTX*) { return false; }
bool SecureSocket::handshake() { return false; }
ssize_t SecureSocket::send(const void* buf, size_t len) {
    if (fd < 0) return -1;
    return ::send(fd, buf, len, MSG_NOSIGNAL);
}
SocketWriteResult SecureSocket::sendAllUntil(
    const void* buf, size_t len,
    std::chrono::steady_clock::time_point deadline,
    const std::function<bool()>& interrupted) {
    if (fd < 0) return SocketWriteResult::Error;
    const int oldFlags = ::fcntl(fd, F_GETFL, 0);
    if (oldFlags < 0) return SocketWriteResult::Error;
    const bool restoreBlocking = (oldFlags & O_NONBLOCK) == 0;
    if (restoreBlocking && ::fcntl(fd, F_SETFL, oldFlags | O_NONBLOCK) < 0)
        return SocketWriteResult::Error;
    struct RestoreFlags {
        int fd;
        int flags;
        bool restore;
        ~RestoreFlags() {
            if (restore) (void)::fcntl(fd, F_SETFL, flags);
        }
    } restore{fd, oldFlags, restoreBlocking};

    const auto* bytes = static_cast<const unsigned char*>(buf);
    size_t written = 0;
    while (written < len) {
        if (interrupted && interrupted())
            return SocketWriteResult::Interrupted;
        if (deadline != std::chrono::steady_clock::time_point::max() &&
            std::chrono::steady_clock::now() >= deadline)
            return SocketWriteResult::TimedOut;
        const ssize_t count = ::send(
            fd, bytes + written, len - written,
            MSG_NOSIGNAL | MSG_DONTWAIT);
        if (count > 0) {
            written += static_cast<size_t>(count);
            continue;
        }
        if (count < 0 && errno == EINTR) continue;
        if (count < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
            return SocketWriteResult::Error;

        const auto now = std::chrono::steady_clock::now();
        if (deadline != std::chrono::steady_clock::time_point::max() &&
            now >= deadline)
            return SocketWriteResult::TimedOut;
        int timeoutMs = 50;
        if (deadline != std::chrono::steady_clock::time_point::max()) {
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(
                deadline - now).count();
            timeoutMs = static_cast<int>(std::min<int64_t>(50, remaining));
        }
        pollfd descriptor{};
        descriptor.fd = fd;
        descriptor.events = POLLOUT;
        const int ready = ::poll(&descriptor, 1, timeoutMs);
        if (ready == 0) continue;
        if (ready < 0) {
            if (errno == EINTR) continue;
            return SocketWriteResult::Error;
        }
        if ((descriptor.revents & POLLOUT) == 0)
            return SocketWriteResult::Error;
    }
    return SocketWriteResult::Complete;
}
ssize_t SecureSocket::recv(void* buf, size_t len) {
    if (fd < 0) return -1;
    return ::recv(fd, buf, len, 0);
}
SocketReadResult SecureSocket::recvSomeUntil(
    void* buf, size_t len, size_t& received,
    std::chrono::steady_clock::time_point deadline,
    const std::function<bool()>& interrupted,
    bool& transportProgress) {
    received = 0;
    transportProgress = false;
    if (fd < 0 || len == 0) return SocketReadResult::Error;
    while (true) {
        if (interrupted && interrupted())
            return SocketReadResult::Interrupted;
        if (deadline != std::chrono::steady_clock::time_point::max() &&
            std::chrono::steady_clock::now() >= deadline)
            return SocketReadResult::TimedOut;
        const ssize_t count = ::recv(fd, buf, len, MSG_DONTWAIT);
        if (count > 0) {
            received = static_cast<size_t>(count);
            transportProgress = true;
            return SocketReadResult::Data;
        }
        if (count == 0) return SocketReadResult::Error;
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            return SocketReadResult::Error;

        const auto now = std::chrono::steady_clock::now();
        if (deadline != std::chrono::steady_clock::time_point::max() &&
            now >= deadline)
            return SocketReadResult::TimedOut;
        int timeoutMs = 50;
        if (deadline != std::chrono::steady_clock::time_point::max()) {
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(
                deadline - now).count();
            timeoutMs = static_cast<int>(std::min<int64_t>(50, remaining));
        }
        pollfd descriptor{};
        descriptor.fd = fd;
        descriptor.events = POLLIN;
        const int ready = ::poll(&descriptor, 1, timeoutMs);
        if (ready == 0) continue;
        if (ready < 0) {
            if (errno == EINTR) continue;
            return SocketReadResult::Error;
        }
        if ((descriptor.revents & POLLIN) == 0)
            return SocketReadResult::Error;
    }
}
bool SecureSocket::hasBufferedInput() const { return false; }
void SecureSocket::close() {
    if (fd >= 0) { ::close(fd); fd = -1; }
}

TLSServerContext::TLSServerContext() : ctx_(nullptr), enabled_(false) {}
TLSServerContext::~TLSServerContext() {}
bool TLSServerContext::init(const std::string&, const std::string&) { return false; }

} // namespace dbms

// Stub OpenSSL functions with matching signatures
extern "C" {
    const SSL_METHOD* TLS_server_method(void) { return nullptr; }
    SSL_CTX* SSL_CTX_new(const SSL_METHOD*) { return nullptr; }
    void SSL_CTX_free(SSL_CTX*) {}
    int SSL_CTX_use_certificate_file(SSL_CTX*, const char*, int) { return 0; }
    int SSL_CTX_use_PrivateKey_file(SSL_CTX*, const char*, int) { return 0; }
    int SSL_CTX_check_private_key(const SSL_CTX*) { return 0; }
    SSL* SSL_new(SSL_CTX*) { return nullptr; }
    void SSL_free(SSL*) {}
    int SSL_set_fd(SSL*, int) { return 0; }
    int SSL_accept(SSL*) { return -1; }
    int SSL_read(SSL*, void*, int) { return -1; }
    int SSL_write(SSL*, const void*, int) { return -1; }
    int SSL_shutdown(SSL*) { return -1; }
    int SSL_get_error(const SSL*, int) { return 0; }
    int SSL_pending(const SSL*) { return 0; }
}
