#include "TLSWrapper.h"
#include <cerrno>
#include <cstring>
#include <unistd.h>
#include <filesystem>
#include <iostream>

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
        return ::write(fd, buf, len);
    }
    return -1;
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

bool TLSServerContext::generateSelfSignedCert(const std::string& certPath,
                                               const std::string& keyPath) {
    if (std::filesystem::exists(certPath) && std::filesystem::exists(keyPath)) return true;

    std::string cmd = "openssl req -x509 -newkey rsa:2048 -keyout " + keyPath +
                      " -out " + certPath +
                      " -days 365 -nodes -subj /CN=localhost 2>/dev/null";
    int ret = std::system(cmd.c_str());
    return ret == 0 && std::filesystem::exists(certPath) && std::filesystem::exists(keyPath);
}

} // namespace dbms
