#pragma once

#include <string>
#include <functional>

// Minimal OpenSSL forward declarations (link with -lssl -lcrypto)
// When libssl-dev is installed, these match the real declarations.
extern "C" {
    typedef struct ssl_st SSL;
    typedef struct ssl_ctx_st SSL_CTX;
    typedef struct ssl_method_st SSL_METHOD;

    const SSL_METHOD* TLS_server_method(void);
    SSL_CTX* SSL_CTX_new(const SSL_METHOD* meth);
    void SSL_CTX_free(SSL_CTX* ctx);
    int SSL_CTX_use_certificate_file(SSL_CTX* ctx, const char* file, int type);
    int SSL_CTX_use_PrivateKey_file(SSL_CTX* ctx, const char* file, int type);
    int SSL_CTX_check_private_key(const SSL_CTX* ctx);

    SSL* SSL_new(SSL_CTX* ctx);
    void SSL_free(SSL* ssl);
    int SSL_set_fd(SSL* ssl, int fd);
    int SSL_accept(SSL* ssl);
    int SSL_read(SSL* ssl, void* buf, int num);
    int SSL_write(SSL* ssl, const void* buf, int num);
    int SSL_shutdown(SSL* ssl);
    int SSL_get_error(const SSL* ssl, int ret);
}

constexpr int SSL_FILETYPE_PEM = 1;
constexpr int SSL_ERROR_WANT_READ = 2;
constexpr int SSL_ERROR_WANT_WRITE = 3;

namespace dbms {

// Secure socket wrapper: uses OpenSSL TLS when available,
// falls back to plain socket I/O otherwise.
struct SecureSocket {
    int fd = -1;
    SSL* ssl = nullptr;
    bool useTLS = false;
    bool tlsOK = false;

    SecureSocket() = default;
    explicit SecureSocket(int sockfd, SSL_CTX* ctx = nullptr);
    ~SecureSocket() { close(); }

    SecureSocket(const SecureSocket&) = delete;
    SecureSocket& operator=(const SecureSocket&) = delete;
    SecureSocket(SecureSocket&& other) noexcept;
    SecureSocket& operator=(SecureSocket&& other) noexcept;

    bool initTLS(SSL_CTX* ctx);
    bool handshake();
    ssize_t send(const void* buf, size_t len);
    ssize_t recv(void* buf, size_t len);
    void close();
};

// TLS context manager for the server
class TLSServerContext {
public:
    TLSServerContext();
    ~TLSServerContext();

    bool init(const std::string& certFile, const std::string& keyFile);
    bool enabled() const { return enabled_; }
    SSL_CTX* ctx() const { return ctx_; }

    // Generate self-signed certificate if missing
    static bool generateSelfSignedCert(const std::string& certPath,
                                        const std::string& keyPath);

private:
    SSL_CTX* ctx_ = nullptr;
    bool enabled_ = false;
};

} // namespace dbms
