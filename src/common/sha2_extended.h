#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>

namespace sha2_extended_detail {

template <std::size_t N>
inline std::string toHex(const std::array<uint8_t, N>& digest) {
    std::ostringstream out;
    for (const uint8_t byte : digest) {
        out << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<unsigned int>(byte);
    }
    return out.str();
}

class SHA224Core {
public:
    SHA224Core()
        : state_{0xc1059ed8U, 0x367cd507U, 0x3070dd17U, 0xf70e5939U,
                 0xffc00b31U, 0x68581511U, 0x64f98fa7U, 0xbefa4fa4U} {}

    void update(const uint8_t* input, std::size_t length) {
        totalBytes_ += static_cast<uint64_t>(length);
        while (length != 0) {
            const std::size_t available = buffer_.size() - bufferLength_;
            const std::size_t copied = length < available ? length : available;
            for (std::size_t i = 0; i < copied; ++i) {
                buffer_[bufferLength_ + i] = input[i];
            }
            bufferLength_ += copied;
            input += copied;
            length -= copied;
            if (bufferLength_ == buffer_.size()) {
                transform(buffer_.data());
                bufferLength_ = 0;
            }
        }
    }

    std::array<uint8_t, 28> finalize() {
        const uint64_t bitLength = totalBytes_ * 8U;
        buffer_[bufferLength_++] = 0x80U;
        if (bufferLength_ > 56) {
            while (bufferLength_ < buffer_.size()) buffer_[bufferLength_++] = 0;
            transform(buffer_.data());
            bufferLength_ = 0;
        }
        while (bufferLength_ < 56) buffer_[bufferLength_++] = 0;
        for (std::size_t i = 0; i < 8; ++i) {
            buffer_[63 - i] = static_cast<uint8_t>(bitLength >> (i * 8));
        }
        transform(buffer_.data());

        std::array<uint8_t, 28> digest{};
        for (std::size_t word = 0; word < 7; ++word) {
            for (std::size_t byte = 0; byte < 4; ++byte) {
                digest[word * 4 + byte] = static_cast<uint8_t>(
                    state_[word] >> (24 - byte * 8));
            }
        }
        return digest;
    }

private:
    std::array<uint32_t, 8> state_;
    std::array<uint8_t, 64> buffer_{};
    std::size_t bufferLength_ = 0;
    uint64_t totalBytes_ = 0;

    static uint32_t rotateRight(uint32_t value, unsigned amount) {
        return (value >> amount) | (value << (32U - amount));
    }

    void transform(const uint8_t* block) {
        static constexpr std::array<uint32_t, 64> constants = {
            0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U,
            0x3956c25bU, 0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U,
            0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U,
            0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U,
            0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU,
            0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
            0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U,
            0xc6e00bf3U, 0xd5a79147U, 0x06ca6351U, 0x14292967U,
            0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U,
            0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
            0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U,
            0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
            0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U,
            0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU, 0x682e6ff3U,
            0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U,
            0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U
        };

        std::array<uint32_t, 64> words{};
        for (std::size_t i = 0; i < 16; ++i) {
            words[i] = (static_cast<uint32_t>(block[i * 4]) << 24) |
                       (static_cast<uint32_t>(block[i * 4 + 1]) << 16) |
                       (static_cast<uint32_t>(block[i * 4 + 2]) << 8) |
                       static_cast<uint32_t>(block[i * 4 + 3]);
        }
        for (std::size_t i = 16; i < words.size(); ++i) {
            const uint32_t s0 = rotateRight(words[i - 15], 7) ^
                                rotateRight(words[i - 15], 18) ^
                                (words[i - 15] >> 3);
            const uint32_t s1 = rotateRight(words[i - 2], 17) ^
                                rotateRight(words[i - 2], 19) ^
                                (words[i - 2] >> 10);
            words[i] = words[i - 16] + s0 + words[i - 7] + s1;
        }

        uint32_t a = state_[0];
        uint32_t b = state_[1];
        uint32_t c = state_[2];
        uint32_t d = state_[3];
        uint32_t e = state_[4];
        uint32_t f = state_[5];
        uint32_t g = state_[6];
        uint32_t h = state_[7];
        for (std::size_t i = 0; i < words.size(); ++i) {
            const uint32_t sum1 = rotateRight(e, 6) ^ rotateRight(e, 11) ^
                                  rotateRight(e, 25);
            const uint32_t choice = (e & f) ^ ((~e) & g);
            const uint32_t temp1 = h + sum1 + choice + constants[i] + words[i];
            const uint32_t sum0 = rotateRight(a, 2) ^ rotateRight(a, 13) ^
                                  rotateRight(a, 22);
            const uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
            const uint32_t temp2 = sum0 + majority;
            h = g;
            g = f;
            f = e;
            e = d + temp1;
            d = c;
            c = b;
            b = a;
            a = temp1 + temp2;
        }
        state_[0] += a;
        state_[1] += b;
        state_[2] += c;
        state_[3] += d;
        state_[4] += e;
        state_[5] += f;
        state_[6] += g;
        state_[7] += h;
    }
};

class SHA512Core {
public:
    explicit SHA512Core(const std::array<uint64_t, 8>& initialState)
        : state_(initialState) {}

    void update(const uint8_t* input, std::size_t length) {
        const uint64_t length64 = static_cast<uint64_t>(length);
        const uint64_t previousLow = bitLengthLow_;
        bitLengthLow_ += length64 << 3;
        if (bitLengthLow_ < previousLow) ++bitLengthHigh_;
        bitLengthHigh_ += length64 >> 61;

        while (length != 0) {
            const std::size_t available = buffer_.size() - bufferLength_;
            const std::size_t copied = length < available ? length : available;
            for (std::size_t i = 0; i < copied; ++i) {
                buffer_[bufferLength_ + i] = input[i];
            }
            bufferLength_ += copied;
            input += copied;
            length -= copied;
            if (bufferLength_ == buffer_.size()) {
                transform(buffer_.data());
                bufferLength_ = 0;
            }
        }
    }

    template <std::size_t DigestBytes>
    std::array<uint8_t, DigestBytes> finalize() {
        static_assert(DigestBytes <= 64, "SHA-512 family digest is at most 64 bytes");
        buffer_[bufferLength_++] = 0x80U;
        if (bufferLength_ > 112) {
            while (bufferLength_ < buffer_.size()) buffer_[bufferLength_++] = 0;
            transform(buffer_.data());
            bufferLength_ = 0;
        }
        while (bufferLength_ < 112) buffer_[bufferLength_++] = 0;
        for (std::size_t i = 0; i < 8; ++i) {
            buffer_[119 - i] = static_cast<uint8_t>(bitLengthHigh_ >> (i * 8));
            buffer_[127 - i] = static_cast<uint8_t>(bitLengthLow_ >> (i * 8));
        }
        transform(buffer_.data());

        std::array<uint8_t, DigestBytes> digest{};
        for (std::size_t i = 0; i < DigestBytes; ++i) {
            digest[i] = static_cast<uint8_t>(
                state_[i / 8] >> (56 - (i % 8) * 8));
        }
        return digest;
    }

private:
    std::array<uint64_t, 8> state_;
    std::array<uint8_t, 128> buffer_{};
    std::size_t bufferLength_ = 0;
    uint64_t bitLengthHigh_ = 0;
    uint64_t bitLengthLow_ = 0;

    static uint64_t rotateRight(uint64_t value, unsigned amount) {
        return (value >> amount) | (value << (64U - amount));
    }

    void transform(const uint8_t* block) {
        static constexpr std::array<uint64_t, 80> constants = {
            0x428a2f98d728ae22ULL, 0x7137449123ef65cdULL,
            0xb5c0fbcfec4d3b2fULL, 0xe9b5dba58189dbbcULL,
            0x3956c25bf348b538ULL, 0x59f111f1b605d019ULL,
            0x923f82a4af194f9bULL, 0xab1c5ed5da6d8118ULL,
            0xd807aa98a3030242ULL, 0x12835b0145706fbeULL,
            0x243185be4ee4b28cULL, 0x550c7dc3d5ffb4e2ULL,
            0x72be5d74f27b896fULL, 0x80deb1fe3b1696b1ULL,
            0x9bdc06a725c71235ULL, 0xc19bf174cf692694ULL,
            0xe49b69c19ef14ad2ULL, 0xefbe4786384f25e3ULL,
            0x0fc19dc68b8cd5b5ULL, 0x240ca1cc77ac9c65ULL,
            0x2de92c6f592b0275ULL, 0x4a7484aa6ea6e483ULL,
            0x5cb0a9dcbd41fbd4ULL, 0x76f988da831153b5ULL,
            0x983e5152ee66dfabULL, 0xa831c66d2db43210ULL,
            0xb00327c898fb213fULL, 0xbf597fc7beef0ee4ULL,
            0xc6e00bf33da88fc2ULL, 0xd5a79147930aa725ULL,
            0x06ca6351e003826fULL, 0x142929670a0e6e70ULL,
            0x27b70a8546d22ffcULL, 0x2e1b21385c26c926ULL,
            0x4d2c6dfc5ac42aedULL, 0x53380d139d95b3dfULL,
            0x650a73548baf63deULL, 0x766a0abb3c77b2a8ULL,
            0x81c2c92e47edaee6ULL, 0x92722c851482353bULL,
            0xa2bfe8a14cf10364ULL, 0xa81a664bbc423001ULL,
            0xc24b8b70d0f89791ULL, 0xc76c51a30654be30ULL,
            0xd192e819d6ef5218ULL, 0xd69906245565a910ULL,
            0xf40e35855771202aULL, 0x106aa07032bbd1b8ULL,
            0x19a4c116b8d2d0c8ULL, 0x1e376c085141ab53ULL,
            0x2748774cdf8eeb99ULL, 0x34b0bcb5e19b48a8ULL,
            0x391c0cb3c5c95a63ULL, 0x4ed8aa4ae3418acbULL,
            0x5b9cca4f7763e373ULL, 0x682e6ff3d6b2b8a3ULL,
            0x748f82ee5defb2fcULL, 0x78a5636f43172f60ULL,
            0x84c87814a1f0ab72ULL, 0x8cc702081a6439ecULL,
            0x90befffa23631e28ULL, 0xa4506cebde82bde9ULL,
            0xbef9a3f7b2c67915ULL, 0xc67178f2e372532bULL,
            0xca273eceea26619cULL, 0xd186b8c721c0c207ULL,
            0xeada7dd6cde0eb1eULL, 0xf57d4f7fee6ed178ULL,
            0x06f067aa72176fbaULL, 0x0a637dc5a2c898a6ULL,
            0x113f9804bef90daeULL, 0x1b710b35131c471bULL,
            0x28db77f523047d84ULL, 0x32caab7b40c72493ULL,
            0x3c9ebe0a15c9bebcULL, 0x431d67c49c100d4cULL,
            0x4cc5d4becb3e42b6ULL, 0x597f299cfc657e2aULL,
            0x5fcb6fab3ad6faecULL, 0x6c44198c4a475817ULL
        };

        std::array<uint64_t, 80> words{};
        for (std::size_t i = 0; i < 16; ++i) {
            uint64_t value = 0;
            for (std::size_t byte = 0; byte < 8; ++byte) {
                value = (value << 8) | block[i * 8 + byte];
            }
            words[i] = value;
        }
        for (std::size_t i = 16; i < words.size(); ++i) {
            const uint64_t s0 = rotateRight(words[i - 15], 1) ^
                                rotateRight(words[i - 15], 8) ^
                                (words[i - 15] >> 7);
            const uint64_t s1 = rotateRight(words[i - 2], 19) ^
                                rotateRight(words[i - 2], 61) ^
                                (words[i - 2] >> 6);
            words[i] = words[i - 16] + s0 + words[i - 7] + s1;
        }

        uint64_t a = state_[0];
        uint64_t b = state_[1];
        uint64_t c = state_[2];
        uint64_t d = state_[3];
        uint64_t e = state_[4];
        uint64_t f = state_[5];
        uint64_t g = state_[6];
        uint64_t h = state_[7];
        for (std::size_t i = 0; i < words.size(); ++i) {
            const uint64_t sum1 = rotateRight(e, 14) ^ rotateRight(e, 18) ^
                                  rotateRight(e, 41);
            const uint64_t choice = (e & f) ^ ((~e) & g);
            const uint64_t temp1 = h + sum1 + choice + constants[i] + words[i];
            const uint64_t sum0 = rotateRight(a, 28) ^ rotateRight(a, 34) ^
                                  rotateRight(a, 39);
            const uint64_t majority = (a & b) ^ (a & c) ^ (b & c);
            const uint64_t temp2 = sum0 + majority;
            h = g;
            g = f;
            f = e;
            e = d + temp1;
            d = c;
            c = b;
            b = a;
            a = temp1 + temp2;
        }
        state_[0] += a;
        state_[1] += b;
        state_[2] += c;
        state_[3] += d;
        state_[4] += e;
        state_[5] += f;
        state_[6] += g;
        state_[7] += h;
    }
};

}  // namespace sha2_extended_detail

class SHA224 {
public:
    static std::array<uint8_t, 28> digestBytes(const std::string& input) {
        sha2_extended_detail::SHA224Core sha;
        sha.update(reinterpret_cast<const uint8_t*>(input.data()), input.size());
        return sha.finalize();
    }

    static std::string hash(const std::string& input) {
        return sha2_extended_detail::toHex(digestBytes(input));
    }
};

class SHA384 {
public:
    static std::array<uint8_t, 48> digestBytes(const std::string& input) {
        sha2_extended_detail::SHA512Core sha({
            0xcbbb9d5dc1059ed8ULL, 0x629a292a367cd507ULL,
            0x9159015a3070dd17ULL, 0x152fecd8f70e5939ULL,
            0x67332667ffc00b31ULL, 0x8eb44a8768581511ULL,
            0xdb0c2e0d64f98fa7ULL, 0x47b5481dbefa4fa4ULL
        });
        sha.update(reinterpret_cast<const uint8_t*>(input.data()), input.size());
        return sha.finalize<48>();
    }

    static std::string hash(const std::string& input) {
        return sha2_extended_detail::toHex(digestBytes(input));
    }
};

class SHA512 {
public:
    static std::array<uint8_t, 64> digestBytes(const std::string& input) {
        sha2_extended_detail::SHA512Core sha({
            0x6a09e667f3bcc908ULL, 0xbb67ae8584caa73bULL,
            0x3c6ef372fe94f82bULL, 0xa54ff53a5f1d36f1ULL,
            0x510e527fade682d1ULL, 0x9b05688c2b3e6c1fULL,
            0x1f83d9abfb41bd6bULL, 0x5be0cd19137e2179ULL
        });
        sha.update(reinterpret_cast<const uint8_t*>(input.data()), input.size());
        return sha.finalize<64>();
    }

    static std::string hash(const std::string& input) {
        return sha2_extended_detail::toHex(digestBytes(input));
    }
};
