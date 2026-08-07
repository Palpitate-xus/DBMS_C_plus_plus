#pragma once

#include "sha256.h"

#include <array>
#include <cctype>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace dbms {

namespace scram {

using Bytes = std::vector<uint8_t>;

inline Bytes toBytes(const std::string& value) {
    return Bytes(value.begin(), value.end());
}

inline std::string bytesToString(const Bytes& value) {
    return std::string(reinterpret_cast<const char*>(value.data()), value.size());
}

inline Bytes sha256Bytes(const Bytes& value) {
    const auto digest = SHA256::digestBytes(bytesToString(value));
    return Bytes(digest.begin(), digest.end());
}

inline Bytes hmacSha256(const Bytes& key, const Bytes& message) {
    constexpr size_t blockSize = 64;
    Bytes normalizedKey = key;
    if (normalizedKey.size() > blockSize) normalizedKey = sha256Bytes(normalizedKey);
    normalizedKey.resize(blockSize, 0);

    Bytes inner(blockSize, 0x36);
    Bytes outer(blockSize, 0x5c);
    for (size_t i = 0; i < blockSize; ++i) {
        inner[i] ^= normalizedKey[i];
        outer[i] ^= normalizedKey[i];
    }
    inner.insert(inner.end(), message.begin(), message.end());
    const Bytes innerDigest = sha256Bytes(inner);
    outer.insert(outer.end(), innerDigest.begin(), innerDigest.end());
    return sha256Bytes(outer);
}

inline Bytes pbkdf2Sha256(const std::string& password, const Bytes& salt,
                          uint32_t iterations) {
    if (iterations == 0) return {};
    Bytes saltBlock = salt;
    saltBlock.insert(saltBlock.end(), {0, 0, 0, 1});
    Bytes previous = hmacSha256(toBytes(password), saltBlock);
    Bytes result = previous;
    for (uint32_t i = 1; i < iterations; ++i) {
        previous = hmacSha256(toBytes(password), previous);
        for (size_t j = 0; j < result.size(); ++j) result[j] ^= previous[j];
    }
    return result;
}

inline std::string base64Encode(const Bytes& input) {
    static constexpr char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string output;
    output.reserve((input.size() + 2) / 3 * 4);
    for (size_t i = 0; i < input.size(); i += 3) {
        uint32_t value = static_cast<uint32_t>(input[i]) << 16;
        if (i + 1 < input.size()) value |= static_cast<uint32_t>(input[i + 1]) << 8;
        if (i + 2 < input.size()) value |= input[i + 2];
        output.push_back(alphabet[(value >> 18) & 0x3f]);
        output.push_back(alphabet[(value >> 12) & 0x3f]);
        output.push_back(i + 1 < input.size() ? alphabet[(value >> 6) & 0x3f] : '=');
        output.push_back(i + 2 < input.size() ? alphabet[value & 0x3f] : '=');
    }
    return output;
}

inline int base64Value(char c) {
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= 'a' && c <= 'z') return c - 'a' + 26;
    if (c >= '0' && c <= '9') return c - '0' + 52;
    if (c == '+') return 62;
    if (c == '/') return 63;
    return -1;
}

inline bool base64Decode(const std::string& input, Bytes& output) {
    output.clear();
    if (input.empty()) return true;
    if (input.size() % 4 != 0) return false;
    for (size_t i = 0; i < input.size(); i += 4) {
        int a = base64Value(input[i]);
        int b = base64Value(input[i + 1]);
        if (a < 0 || b < 0) return false;
        int c = input[i + 2] == '=' ? -2 : base64Value(input[i + 2]);
        int d = input[i + 3] == '=' ? -2 : base64Value(input[i + 3]);
        if (c == -1 || d == -1 || (c == -2 && d != -2)) return false;
        uint32_t value = (static_cast<uint32_t>(a) << 18) |
                         (static_cast<uint32_t>(b) << 12) |
                         (static_cast<uint32_t>(c < 0 ? 0 : c) << 6) |
                         static_cast<uint32_t>(d < 0 ? 0 : d);
        output.push_back(static_cast<uint8_t>((value >> 16) & 0xff));
        if (c != -2) output.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
        if (d != -2) output.push_back(static_cast<uint8_t>(value & 0xff));
    }
    return true;
}

struct Verifier {
    uint32_t iterations = 0;
    Bytes salt;
    Bytes storedKey;
    Bytes serverKey;
};

inline bool parseVerifier(const std::string& encoded, Verifier& verifier) {
    constexpr const char* prefix = "SCRAM-SHA-256$";
    if (encoded.rfind(prefix, 0) != 0) return false;
    const std::string body = encoded.substr(std::char_traits<char>::length(prefix));
    const size_t iterationsColon = body.find(':');
    const size_t saltDollar = body.find('$');
    const size_t keysColon = saltDollar == std::string::npos
                                 ? std::string::npos : body.find(':', saltDollar + 1);
    if (iterationsColon == std::string::npos || saltDollar == std::string::npos ||
        keysColon == std::string::npos || iterationsColon > saltDollar) return false;
    try {
        verifier.iterations = static_cast<uint32_t>(std::stoul(body.substr(0, iterationsColon)));
    } catch (...) {
        return false;
    }
    if (!base64Decode(body.substr(iterationsColon + 1, saltDollar - iterationsColon - 1), verifier.salt) ||
        !base64Decode(body.substr(saltDollar + 1, keysColon - saltDollar - 1), verifier.storedKey) ||
        !base64Decode(body.substr(keysColon + 1), verifier.serverKey)) {
        return false;
    }
    return verifier.iterations > 0 && verifier.salt.size() >= 1 &&
           verifier.storedKey.size() == 32 && verifier.serverKey.size() == 32;
}

inline std::string makeVerifier(const std::string& password, const Bytes& salt,
                                uint32_t iterations = 4096) {
    const Bytes saltedPassword = pbkdf2Sha256(password, salt, iterations);
    const Bytes clientKey = hmacSha256(saltedPassword, toBytes("Client Key"));
    const Bytes storedKey = sha256Bytes(clientKey);
    const Bytes serverKey = hmacSha256(saltedPassword, toBytes("Server Key"));
    return "SCRAM-SHA-256$" + std::to_string(iterations) + ":" +
           base64Encode(salt) + "$" + base64Encode(storedKey) + ":" +
           base64Encode(serverKey);
}

inline std::string makeRandomVerifier(const std::string& password,
                                      uint32_t iterations = 4096) {
    std::array<uint8_t, 16> salt{};
    std::random_device random;
    for (auto& byte : salt) byte = static_cast<uint8_t>(random());
    return makeVerifier(password, Bytes(salt.begin(), salt.end()), iterations);
}

inline bool constantTimeEqual(const Bytes& left, const Bytes& right) {
    if (left.size() != right.size()) return false;
    uint8_t difference = 0;
    for (size_t i = 0; i < left.size(); ++i) difference |= left[i] ^ right[i];
    return difference == 0;
}

inline bool verifyPassword(const std::string& password, const std::string& encoded) {
    Verifier verifier;
    if (!parseVerifier(encoded, verifier)) return false;
    const Bytes saltedPassword = pbkdf2Sha256(password, verifier.salt, verifier.iterations);
    const Bytes clientKey = hmacSha256(saltedPassword, toBytes("Client Key"));
    const Bytes storedKey = sha256Bytes(clientKey);
    return constantTimeEqual(storedKey, verifier.storedKey);
}

inline bool verifyClientProof(const Verifier& verifier, const std::string& authMessage,
                              const std::string& proof, std::string& serverSignature) {
    Bytes clientProof;
    if (!base64Decode(proof, clientProof) || clientProof.size() != 32) return false;
    const Bytes clientSignature = hmacSha256(verifier.storedKey, toBytes(authMessage));
    Bytes clientKey(32);
    for (size_t i = 0; i < clientKey.size(); ++i) clientKey[i] = clientProof[i] ^ clientSignature[i];
    if (!constantTimeEqual(sha256Bytes(clientKey), verifier.storedKey)) return false;
    serverSignature = base64Encode(hmacSha256(verifier.serverKey, toBytes(authMessage)));
    return true;
}

} // namespace scram

} // namespace dbms
