#include "TLSWrapper.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace dbms {

// ========================================================================
// SecureSocket
// ========================================================================
SecureSocket::SecureSocket(int sockfd, SSL_CTX* ctx) : fd(sockfd) {
    if (ctx) initTLS(ctx);
}

SecureSocket::SecureSocket(SecureSocket&& other) noexcept
    : fd(other.fd), ssl(other.ssl), useTLS(other.useTLS), tlsOK(other.tlsOK) {
    other.fd = -1;
    other.ssl = nullptr;
    other.useTLS = false;
    other.tlsOK = false;
}

SecureSocket& SecureSocket::operator=(SecureSocket&& other) noexcept {
    if (this != &other) {
        close();
        fd = other.fd;
        ssl = other.ssl;
        useTLS = other.useTLS;
        tlsOK = other.tlsOK;
        other.fd = -1;
        other.ssl = nullptr;
        other.useTLS = false;
        other.tlsOK = false;
    }
    return *this;
}

bool SecureSocket::initTLS(SSL_CTX* ctx) {
    if (!ctx || fd < 0) return false;
    ssl = SSL_new(ctx);
    if (!ssl) return false;
    SSL_set_fd(ssl, fd);
    useTLS = true;
    return true;
}

bool SecureSocket::handshake() {
    if (!useTLS || !ssl) { tlsOK = false; return false; }
    int ret = SSL_accept(ssl);
    if (ret <= 0) {
        int err = SSL_get_error(ssl, ret);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
            // Non-blocking would retry; for blocking sockets this is an error
        }
        tlsOK = false;
        return false;
    }
    tlsOK = true;
    return true;
}

ssize_t SecureSocket::send(const void* buf, size_t len) {
    if (useTLS && ssl && tlsOK) {
        int n = SSL_write(ssl, buf, static_cast<int>(len));
        return n > 0 ? n : -1;
    }
    if (fd >= 0) {
        return ::send(fd, buf, len, MSG_NOSIGNAL);
    }
    return -1;
}

SocketWriteResult SecureSocket::sendAllUntil(
    const void* buf, size_t len,
    std::chrono::steady_clock::time_point deadline,
    const std::function<bool()>& interrupted) {
    if (fd < 0 || (useTLS && (!ssl || !tlsOK)))
        return SocketWriteResult::Error;

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
    short waitEvents = POLLOUT;
    while (written < len) {
        if (interrupted && interrupted())
            return SocketWriteResult::Interrupted;
        if (deadline != std::chrono::steady_clock::time_point::max() &&
            std::chrono::steady_clock::now() >= deadline)
            return SocketWriteResult::TimedOut;

        if (useTLS && ssl && tlsOK) {
            const size_t remaining = len - written;
            const int chunk = static_cast<int>(std::min<size_t>(
                remaining, static_cast<size_t>(std::numeric_limits<int>::max())));
            errno = 0;
            const int count = SSL_write(ssl, bytes + written, chunk);
            if (count > 0) {
                written += static_cast<size_t>(count);
                waitEvents = POLLOUT;
                continue;
            }
            const int sslError = SSL_get_error(ssl, count);
            if (sslError == SSL_ERROR_WANT_READ) {
                waitEvents = POLLIN;
            } else if (sslError == SSL_ERROR_WANT_WRITE) {
                waitEvents = POLLOUT;
            } else if (sslError == SSL_ERROR_SYSCALL &&
                       (errno == EINTR || errno == EAGAIN ||
                        errno == EWOULDBLOCK)) {
                waitEvents = POLLOUT;
            } else {
                return SocketWriteResult::Error;
            }
        } else {
            const ssize_t count = ::send(
                fd, bytes + written, len - written,
                MSG_NOSIGNAL | MSG_DONTWAIT);
            if (count > 0) {
                written += static_cast<size_t>(count);
                continue;
            }
            if (count < 0 && errno == EINTR) continue;
            if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                waitEvents = POLLOUT;
            } else {
                return SocketWriteResult::Error;
            }
        }

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
        descriptor.events = waitEvents;
        const int ready = ::poll(&descriptor, 1, timeoutMs);
        if (ready == 0) continue;
        if (ready < 0) {
            if (errno == EINTR) continue;
            return SocketWriteResult::Error;
        }
        if ((descriptor.revents & waitEvents) == 0)
            return SocketWriteResult::Error;
    }
    return SocketWriteResult::Complete;
}

ssize_t SecureSocket::recv(void* buf, size_t len) {
    if (useTLS && ssl && tlsOK) {
        int n = SSL_read(ssl, buf, static_cast<int>(len));
        return n > 0 ? n : -1;
    }
    if (fd >= 0) {
        return ::read(fd, buf, len);
    }
    return -1;
}

bool SecureSocket::hasBufferedInput() const {
    return useTLS && ssl && tlsOK && SSL_pending(ssl) > 0;
}

void SecureSocket::close() {
    if (ssl) {
        SSL_shutdown(ssl);
        SSL_free(ssl);
        ssl = nullptr;
    }
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
    useTLS = false;
    tlsOK = false;
}

// ========================================================================
// TLSServerContext
// ========================================================================
TLSServerContext::TLSServerContext() {
    // OpenSSL 1.1+ and 3.x auto-initialize on first use.
    // No explicit init required.
}

TLSServerContext::~TLSServerContext() {
    if (ctx_) SSL_CTX_free(ctx_);
}

bool TLSServerContext::init(const std::string& certFile, const std::string& keyFile) {
    if (ctx_) { SSL_CTX_free(ctx_); ctx_ = nullptr; }

    const auto* method = TLS_server_method();
    if (!method) return false;

    ctx_ = SSL_CTX_new(method);
    if (!ctx_) return false;

    if (SSL_CTX_use_certificate_file(ctx_, certFile.c_str(), SSL_FILETYPE_PEM) <= 0) {
        SSL_CTX_free(ctx_); ctx_ = nullptr; return false;
    }
    if (SSL_CTX_use_PrivateKey_file(ctx_, keyFile.c_str(), SSL_FILETYPE_PEM) <= 0) {
        SSL_CTX_free(ctx_); ctx_ = nullptr; return false;
    }
    if (SSL_CTX_check_private_key(ctx_) <= 0) {
        SSL_CTX_free(ctx_); ctx_ = nullptr; return false;
    }
    enabled_ = true;
    return true;
}

} // namespace dbms
