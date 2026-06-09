// Stub implementation for systems without OpenSSL
#include "TLSWrapper.h"
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
    return ::send(fd, buf, len, 0);
}
ssize_t SecureSocket::recv(void* buf, size_t len) {
    if (fd < 0) return -1;
    return ::recv(fd, buf, len, 0);
}
void SecureSocket::close() {
    if (fd >= 0) { ::close(fd); fd = -1; }
}

TLSServerContext::TLSServerContext() : ctx_(nullptr), enabled_(false) {}
TLSServerContext::~TLSServerContext() {}
bool TLSServerContext::init(const std::string&, const std::string&) { return false; }
bool TLSServerContext::generateSelfSignedCert(const std::string&, const std::string&) { return false; }

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
}
