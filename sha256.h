#pragma once

#include <cstdint>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

class SHA256 {
public:
    static std::string hash(const std::string& input) {
        SHA256 sha;
        sha.update(reinterpret_cast<const uint8_t*>(input.data()), input.size());
        uint8_t digest[32];
        sha.finalize(digest);
        std::ostringstream oss;
        for (int i = 0; i < 32; ++i)
            oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]);
        return oss.str();
    }

private:
    uint32_t state[8] = {
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19
    };
    uint64_t bitlen = 0;
    uint8_t data[64];
    uint32_t datalen = 0;

    static uint32_t rotr(uint32_t x, uint32_t n) { return (x >> n) | (x << (32 - n)); }
    static uint32_t ch(uint32_t x, uint32_t y, uint32_t z) { return (x & y) ^ (~x & z); }
    static uint32_t maj(uint32_t x, uint32_t y, uint32_t z) { return (x & y) ^ (x & z) ^ (y & z); }
    static uint32_t ep0(uint32_t x) { return rotr(x, 2) ^ rotr(x, 13) ^ rotr(x, 22); }
    static uint32_t ep1(uint32_t x) { return rotr(x, 6) ^ rotr(x, 11) ^ rotr(x, 25); }
    static uint32_t sig0(uint32_t x) { return rotr(x, 7) ^ rotr(x, 18) ^ (x >> 3); }
    static uint32_t sig1(uint32_t x) { return rotr(x, 17) ^ rotr(x, 19) ^ (x >> 10); }

    void transform(const uint8_t* block) {
        static const uint32_t k[64] = {
            0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
            0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
            0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
            0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
            0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
            0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
            0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
            0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
        };
        uint32_t m[64];
        for (int i = 0; i < 16; ++i)
            m[i] = (block[i * 4] << 24) | (block[i * 4 + 1] << 16) | (block[i * 4 + 2] << 8) | block[i * 4 + 3];
        for (int i = 16; i < 64; ++i)
            m[i] = sig1(m[i - 2]) + m[i - 7] + sig0(m[i - 15]) + m[i - 16];

        uint32_t a = state[0], b = state[1], c = state[2], d = state[3];
        uint32_t e = state[4], f = state[5], g = state[6], h = state[7];

        for (int i = 0; i < 64; ++i) {
            uint32_t t1 = h + ep1(e) + ch(e, f, g) + k[i] + m[i];
            uint32_t t2 = ep0(a) + maj(a, b, c);
            h = g; g = f; f = e; e = d + t1;
            d = c; c = b; b = a; a = t1 + t2;
        }
        state[0] += a; state[1] += b; state[2] += c; state[3] += d;
        state[4] += e; state[5] += f; state[6] += g; state[7] += h;
    }

    void update(const uint8_t* data_in, size_t len) {
        for (size_t i = 0; i < len; ++i) {
            data[datalen++] = data_in[i];
            if (datalen == 64) {
                transform(data);
                bitlen += 512;
                datalen = 0;
            }
        }
    }

    void finalize(uint8_t* hash_out) {
        uint32_t i = datalen;
        if (datalen < 56) {
            data[i++] = 0x80;
            while (i < 56) data[i++] = 0x00;
        } else {
            data[i++] = 0x80;
            while (i < 64) data[i++] = 0x00;
            transform(data);
            std::memset(data, 0, 56);
        }
        bitlen += datalen * 8;
        for (int i = 0; i < 8; ++i)
            data[63 - i] = static_cast<uint8_t>((bitlen >> (i * 8)) & 0xff);
        transform(data);
        for (int i = 0; i < 4; ++i) {
            for (int j = 0; j < 8; ++j)
                hash_out[i + j * 4] = static_cast<uint8_t>((state[j] >> (24 - i * 8)) & 0xff);
        }
    }
};

inline std::string sha256(const std::string& input) {
    return SHA256::hash(input);
}

// Simple MD5 implementation
class MD5 {
public:
    static std::string hash(const std::string& input) {
        MD5 md5;
        md5.update(reinterpret_cast<const uint8_t*>(input.data()), input.size());
        uint8_t digest[16];
        md5.finalize(digest);
        std::ostringstream oss;
        for (int i = 0; i < 16; ++i)
            oss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]);
        return oss.str();
    }

private:
    uint32_t state[4] = { 0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476 };
    uint32_t count[2] = { 0, 0 };
    uint8_t buffer[64];

    static uint32_t F(uint32_t x, uint32_t y, uint32_t z) { return (x & y) | (~x & z); }
    static uint32_t G(uint32_t x, uint32_t y, uint32_t z) { return (x & z) | (y & ~z); }
    static uint32_t H(uint32_t x, uint32_t y, uint32_t z) { return x ^ y ^ z; }
    static uint32_t I(uint32_t x, uint32_t y, uint32_t z) { return y ^ (x | ~z); }
    static uint32_t rotateLeft(uint32_t x, uint32_t n) { return (x << n) | (x >> (32 - n)); }

    static void FF(uint32_t& a, uint32_t b, uint32_t c, uint32_t d, uint32_t x, uint32_t s, uint32_t ac) {
        a = rotateLeft(a + F(b, c, d) + x + ac, s) + b;
    }
    static void GG(uint32_t& a, uint32_t b, uint32_t c, uint32_t d, uint32_t x, uint32_t s, uint32_t ac) {
        a = rotateLeft(a + G(b, c, d) + x + ac, s) + b;
    }
    static void HH(uint32_t& a, uint32_t b, uint32_t c, uint32_t d, uint32_t x, uint32_t s, uint32_t ac) {
        a = rotateLeft(a + H(b, c, d) + x + ac, s) + b;
    }
    static void II(uint32_t& a, uint32_t b, uint32_t c, uint32_t d, uint32_t x, uint32_t s, uint32_t ac) {
        a = rotateLeft(a + I(b, c, d) + x + ac, s) + b;
    }

    void transform(const uint8_t* block) {
        uint32_t a = state[0], b = state[1], c = state[2], d = state[3];
        uint32_t x[16];
        for (int i = 0; i < 16; ++i)
            x[i] = block[i * 4] | (block[i * 4 + 1] << 8) | (block[i * 4 + 2] << 16) | (block[i * 4 + 3] << 24);

        FF(a, b, c, d, x[ 0],  7, 0xD76AA478); FF(d, a, b, c, x[ 1], 12, 0xE8C7B756);
        FF(c, d, a, b, x[ 2], 17, 0x242070DB); FF(b, c, d, a, x[ 3], 22, 0xC1BDCEEE);
        FF(a, b, c, d, x[ 4],  7, 0xF57C0FAF); FF(d, a, b, c, x[ 5], 12, 0x4787C62A);
        FF(c, d, a, b, x[ 6], 17, 0xA8304613); FF(b, c, d, a, x[ 7], 22, 0xFD469501);
        FF(a, b, c, d, x[ 8],  7, 0x698098D8); FF(d, a, b, c, x[ 9], 12, 0x8B44F7AF);
        FF(c, d, a, b, x[10], 17, 0xFFFF5BB1); FF(b, c, d, a, x[11], 22, 0x895CD7BE);
        FF(a, b, c, d, x[12],  7, 0x6B901122); FF(d, a, b, c, x[13], 12, 0xFD987193);
        FF(c, d, a, b, x[14], 17, 0xA679438E); FF(b, c, d, a, x[15], 22, 0x49B40821);

        GG(a, b, c, d, x[ 1],  5, 0xF61E2562); GG(d, a, b, c, x[ 6],  9, 0xC040B340);
        GG(c, d, a, b, x[11], 14, 0x265E5A51); GG(b, c, d, a, x[ 0], 20, 0xE9B6C7AA);
        GG(a, b, c, d, x[ 5],  5, 0xD62F105D); GG(d, a, b, c, x[10],  9, 0x02441453);
        GG(c, d, a, b, x[15], 14, 0xD8A1E681); GG(b, c, d, a, x[ 4], 20, 0xE7D3FBC8);
        GG(a, b, c, d, x[ 9],  5, 0x21E1CDE6); GG(d, a, b, c, x[14],  9, 0xC33707D6);
        GG(c, d, a, b, x[ 3], 14, 0xF4D50D87); GG(b, c, d, a, x[ 8], 20, 0x455A14ED);
        GG(a, b, c, d, x[13],  5, 0xA9E3E905); GG(d, a, b, c, x[ 2],  9, 0xFCEFA3F8);
        GG(c, d, a, b, x[ 7], 14, 0x676F02D9); GG(b, c, d, a, x[12], 20, 0x8D2A4C8A);

        HH(a, b, c, d, x[ 5],  4, 0xFFFA3942); HH(d, a, b, c, x[ 8], 11, 0x8771F681);
        HH(c, d, a, b, x[11], 16, 0x6D9D6122); HH(b, c, d, a, x[14], 23, 0xFDE5380C);
        HH(a, b, c, d, x[ 1],  4, 0xA4BEEA44); HH(d, a, b, c, x[ 4], 11, 0x4BDECFA9);
        HH(c, d, a, b, x[ 7], 16, 0xF6BB4B60); HH(b, c, d, a, x[10], 23, 0xBEBFBC70);
        HH(a, b, c, d, x[13],  4, 0x289B7EC6); HH(d, a, b, c, x[ 0], 11, 0xEAA127FA);
        HH(c, d, a, b, x[ 3], 16, 0xD4EF3085); HH(b, c, d, a, x[ 6], 23, 0x04881D05);
        HH(a, b, c, d, x[ 9],  4, 0xD9D4D039); HH(d, a, b, c, x[12], 11, 0xE6DB99E5);
        HH(c, d, a, b, x[15], 16, 0x1FA27CF8); HH(b, c, d, a, x[ 2], 23, 0xC4AC5665);

        II(a, b, c, d, x[ 0],  6, 0xF4292244); II(d, a, b, c, x[ 7], 10, 0x432AFF97);
        II(c, d, a, b, x[14], 15, 0xAB9423A7); II(b, c, d, a, x[ 5], 21, 0xFC93A039);
        II(a, b, c, d, x[12],  6, 0x655B59C3); II(d, a, b, c, x[ 3], 10, 0x8F0CCC92);
        II(c, d, a, b, x[10], 15, 0xFFEFF47D); II(b, c, d, a, x[ 1], 21, 0x85845DD1);
        II(a, b, c, d, x[ 8],  6, 0x6FA87E4F); II(d, a, b, c, x[15], 10, 0xFE2CE6E0);
        II(c, d, a, b, x[ 6], 15, 0xA3014314); II(b, c, d, a, x[13], 21, 0x4E0811A1);
        II(a, b, c, d, x[ 4],  6, 0xF7537E82); II(d, a, b, c, x[11], 10, 0xBD3AF235);
        II(c, d, a, b, x[ 2], 15, 0x2AD7D2BB); II(b, c, d, a, x[ 9], 21, 0xEB86D391);

        state[0] += a; state[1] += b; state[2] += c; state[3] += d;
    }

    void update(const uint8_t* data_in, size_t len) {
        uint32_t i, index, partLen;
        index = (count[0] >> 3) & 0x3F;
        partLen = 64 - index;
        count[0] += static_cast<uint32_t>(len) << 3;
        if (count[0] < (static_cast<uint32_t>(len) << 3)) count[1]++;
        count[1] += static_cast<uint32_t>(len >> 29);
        if (len >= partLen) {
            std::memcpy(&buffer[index], data_in, partLen);
            transform(buffer);
            for (i = partLen; i + 63 < len; i += 64) transform(&data_in[i]);
            index = 0;
        } else {
            i = 0;
        }
        std::memcpy(&buffer[index], &data_in[i], len - i);
    }

    void finalize(uint8_t* hash_out) {
        uint8_t bits[8];
        uint32_t index = (count[0] >> 3) & 0x3F;
        uint32_t padLen = (index < 56) ? (56 - index) : (120 - index);
        uint8_t padding[64];
        padding[0] = 0x80;
        for (size_t i = 1; i < padLen; ++i) padding[i] = 0;
        // Save original bit count before padding corrupts it
        uint32_t origCount[2] = { count[0], count[1] };
        update(padding, padLen);
        for (int i = 0; i < 8; ++i) bits[i] = (origCount[i >> 2] >> ((i & 3) << 3)) & 0xFF;
        update(bits, 8);
        for (int i = 0; i < 16; ++i)
            hash_out[i] = (state[i >> 2] >> ((i & 3) << 3)) & 0xFF;
    }
};

inline std::string md5(const std::string& input) {
    return MD5::hash(input);
}
