#include "types/numeric.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <limits>
#include <sstream>

namespace dbms {

namespace {

bool isAllZero(const std::vector<uint8_t>& d) {
    for (auto v : d) if (v != 0) return false;
    return true;
}

void stripLeadingZeros(std::vector<uint8_t>& d) {
    size_t i = 0;
    while (i + 1 < d.size() && d[i] == 0) ++i;
    if (i > 0) d.erase(d.begin(), d.begin() + i);
}

std::vector<uint8_t> addDigitVectors(const std::vector<uint8_t>& a,
                                         const std::vector<uint8_t>& b) {
    std::vector<uint8_t> res;
    res.reserve(std::max(a.size(), b.size()) + 1);
    int carry = 0;
    int i = static_cast<int>(a.size()) - 1;
    int j = static_cast<int>(b.size()) - 1;
    while (i >= 0 || j >= 0 || carry) {
        int av = (i >= 0) ? a[i--] : 0;
        int bv = (j >= 0) ? b[j--] : 0;
        int s = av + bv + carry;
        res.push_back(static_cast<uint8_t>(s % 10));
        carry = s / 10;
    }
    std::reverse(res.begin(), res.end());
    return res;
}

// Precondition: a >= b.
std::vector<uint8_t> subDigitVectors(const std::vector<uint8_t>& a,
                                         const std::vector<uint8_t>& b) {
    std::vector<uint8_t> res;
    res.reserve(a.size());
    int borrow = 0;
    int i = static_cast<int>(a.size()) - 1;
    int j = static_cast<int>(b.size()) - 1;
    while (i >= 0) {
        int av = a[i--] - borrow;
        int bv = (j >= 0) ? b[j--] : 0;
        if (av < bv) {
            av += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        res.push_back(static_cast<uint8_t>(av - bv));
    }
    std::reverse(res.begin(), res.end());
    stripLeadingZeros(res);
    return res;
}

int cmpDigitVectors(const std::vector<uint8_t>& a,
                    const std::vector<uint8_t>& b) {
    if (a.size() != b.size()) return a.size() < b.size() ? -1 : 1;
    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i] != b[i]) return a[i] < b[i] ? -1 : 1;
    }
    return 0;
}

std::vector<uint8_t> mulDigitVectors(const std::vector<uint8_t>& a,
                                         const std::vector<uint8_t>& b) {
    if (isAllZero(a) || isAllZero(b)) return {0};
    std::vector<int> tmp(a.size() + b.size(), 0);
    for (int i = static_cast<int>(a.size()) - 1; i >= 0; --i) {
        for (int j = static_cast<int>(b.size()) - 1; j >= 0; --j) {
            tmp[i + j + 1] += a[i] * b[j];
        }
    }
    for (int i = static_cast<int>(tmp.size()) - 1; i > 0; --i) {
        if (tmp[i] >= 10) {
            tmp[i - 1] += tmp[i] / 10;
            tmp[i] %= 10;
        }
    }
    std::vector<uint8_t> res;
    res.reserve(tmp.size());
    for (int v : tmp) res.push_back(static_cast<uint8_t>(v));
    stripLeadingZeros(res);
    return res;
}

// Divide integer magnitude D by V, returning quotient and remainder.
// Both D and V are little-endian magnitude vectors (most significant first).
void divDigitVectors(const std::vector<uint8_t>& D,
                     const std::vector<uint8_t>& V,
                     std::vector<uint8_t>& quotient,
                     std::vector<uint8_t>& remainder) {
    if (isAllZero(V)) throw std::runtime_error("division by zero");
    quotient.assign(D.size(), 0);
    std::vector<uint8_t> rem;
    for (size_t idx = 0; idx < D.size(); ++idx) {
        rem.push_back(D[idx]);
        stripLeadingZeros(rem);
        if (rem.empty()) rem.push_back(0);
        int q = 0;
        while (cmpDigitVectors(rem, V) >= 0) {
            rem = subDigitVectors(rem, V);
            ++q;
            if (q > 9) break; // safety, should never happen
        }
        quotient[idx] = static_cast<uint8_t>(q);
    }
    remainder = rem.empty() ? std::vector<uint8_t>{0} : rem;
    stripLeadingZeros(quotient);
}

// Increment a magnitude vector by 1 (in place).
void incrementDigitVector(std::vector<uint8_t>& d) {
    int carry = 1;
    for (int i = static_cast<int>(d.size()) - 1; i >= 0 && carry; --i) {
        int s = d[i] + carry;
        d[i] = static_cast<uint8_t>(s % 10);
        carry = s / 10;
    }
    if (carry) d.insert(d.begin(), 1);
}

// Divide magnitude by 10^drop, rounding half-up.
std::vector<uint8_t> divideByPowerOf10(const std::vector<uint8_t>& d, int drop) {
    if (drop <= 0) return d;
    if (static_cast<int>(d.size()) <= drop) {
        // Everything drops: quotient 0, but half-up still rounds on the
        // most significant dropped digit (d[0] when size == drop).
        if (static_cast<int>(d.size()) == drop && !d.empty() && d[0] >= 5)
            return {1};
        return {0};
    }
    std::vector<uint8_t> q(d.begin(), d.end() - drop);
    uint8_t roundDigit = d[d.size() - drop];
    bool roundUp = roundDigit >= 5;
    if (roundUp) incrementDigitVector(q);
    stripLeadingZeros(q);
    if (q.empty()) q.push_back(0);
    return q;
}

} // namespace

Numeric::Numeric(int64_t v) {
    if (v == 0) {
        digits_ = {0};
        return;
    }
    sign_ = (v > 0) ? 1 : -1;
    uint64_t uv = v < 0
        ? static_cast<uint64_t>(-(v + 1)) + 1
        : static_cast<uint64_t>(v);
    while (uv > 0) {
        digits_.push_back(static_cast<uint8_t>(uv % 10));
        uv /= 10;
    }
    std::reverse(digits_.begin(), digits_.end());
    precision_ = static_cast<int>(digits_.size());
}

Numeric::Numeric(const std::string& s) : Numeric(fromString(s)) {}

Numeric Numeric::fromString(const std::string& s) {
    size_t begin = 0;
    while (begin < s.size() &&
           std::isspace(static_cast<unsigned char>(s[begin]))) {
        ++begin;
    }
    size_t end = s.size();
    while (end > begin &&
           std::isspace(static_cast<unsigned char>(s[end - 1]))) {
        --end;
    }
    if (begin == end) throw std::invalid_argument("invalid numeric: empty");
    const std::string str = s.substr(begin, end - begin);
    std::string keyword = str;
    std::transform(keyword.begin(), keyword.end(), keyword.begin(),
                   [](unsigned char ch) {
                       return static_cast<char>(std::tolower(ch));
                   });
    if (keyword == "nan") return nan();
    if (keyword == "infinity" || keyword == "inf" ||
        keyword == "+infinity" || keyword == "+inf")
        return infinity(1);
    if (keyword == "-infinity" || keyword == "-inf") return infinity(-1);

    Numeric n;
    size_t pos = 0;
    if (str[pos] == '+') {
        ++pos;
    } else if (str[pos] == '-') {
        n.sign_ = -1;
        ++pos;
    }

    std::string intPart;
    std::string fracPart;
    bool sawDot = false;
    bool sawDigit = false;
    bool sawExponent = false;
    for (; pos < str.size(); ++pos) {
        char ch = str[pos];
        if (ch == '.') {
            if (sawDot) throw std::invalid_argument("invalid numeric: multiple dots");
            sawDot = true;
        } else if (ch == 'e' || ch == 'E') {
            if (!sawDigit)
                throw std::invalid_argument("invalid numeric exponent");
            sawExponent = true;
            ++pos;
            break;
        } else if (ch >= '0' && ch <= '9') {
            sawDigit = true;
            if (sawDot) fracPart.push_back(ch); else intPart.push_back(ch);
        } else {
            throw std::invalid_argument("invalid numeric character");
        }
    }
    if (!sawDigit)
        throw std::invalid_argument("invalid numeric: no digits");

    int64_t exponent = 0;
    if (sawExponent) {
        int exponentSign = 1;
        if (pos < str.size() && (str[pos] == '+' || str[pos] == '-')) {
            if (str[pos] == '-') exponentSign = -1;
            ++pos;
        }
        if (pos == str.size())
            throw std::invalid_argument("invalid numeric exponent");

        int64_t magnitude = 0;
        bool exponentDigit = false;
        for (; pos < str.size(); ++pos) {
            const char ch = str[pos];
            if (ch < '0' || ch > '9')
                throw std::invalid_argument("invalid numeric exponent");
            exponentDigit = true;
            const int digit = ch - '0';
            if (magnitude >
                (std::numeric_limits<int64_t>::max() - digit) / 10) {
                throw std::invalid_argument("numeric exponent out of range");
            }
            magnitude = magnitude * 10 + digit;
        }
        if (!exponentDigit)
            throw std::invalid_argument("invalid numeric exponent");
        exponent = exponentSign > 0 ? magnitude : -magnitude;
    }

    // Drop leading zeros from integer part.
    size_t leading = 0;
    while (leading + 1 < intPart.size() && intPart[leading] == '0') ++leading;
    intPart = intPart.substr(leading);

    n.digits_.reserve(intPart.size() + fracPart.size());
    for (char ch : intPart) n.digits_.push_back(static_cast<uint8_t>(ch - '0'));
    for (char ch : fracPart) n.digits_.push_back(static_cast<uint8_t>(ch - '0'));

    stripLeadingZeros(n.digits_);

    if (fracPart.size() > static_cast<size_t>(
                                  std::numeric_limits<int64_t>::max() -
                                  kMaxPrecision)) {
        throw std::invalid_argument("numeric scale exceeds maximum");
    }
    const int64_t fractionalScale =
        static_cast<int64_t>(fracPart.size());
    if (isAllZero(n.digits_) && exponent >= fractionalScale) {
        // Positive exponents cannot allocate significant digits for zero.
        n.scale_ = 0;
        n.normalize();
        return n;
    }
    if (exponent > fractionalScale + kMaxPrecision ||
        exponent < fractionalScale - kMaxPrecision) {
        throw std::invalid_argument("numeric exponent out of range");
    }
    const int64_t resultingScale = fractionalScale - exponent;
    if (resultingScale < 0) {
        const uint64_t zeros = static_cast<uint64_t>(-(resultingScale + 1)) + 1;
        const uint64_t availableDigits = n.digits_.size() < kMaxPrecision
            ? static_cast<uint64_t>(kMaxPrecision - n.digits_.size()) : 0;
        if (zeros > availableDigits) {
            throw std::invalid_argument("numeric precision exceeds maximum");
        }
        n.digits_.insert(n.digits_.end(), static_cast<size_t>(zeros), 0);
        n.scale_ = 0;
    } else {
        if (resultingScale > kMaxPrecision)
            throw std::invalid_argument("numeric scale exceeds maximum");
        n.scale_ = static_cast<int>(resultingScale);
    }

    n.normalize();
    if (n.precision_ > kMaxPrecision) {
        throw std::invalid_argument("numeric precision exceeds maximum");
    }
    return n;
}

Numeric Numeric::nan() { return Numeric(true, false, 1); }
Numeric Numeric::infinity(int sign) {
    if (sign == 0) sign = 1;
    return Numeric(false, true, sign > 0 ? 1 : -1);
}

void Numeric::normalize() {
    if (nan_ || inf_) return;
    // Strip leading zeros, but keep at least one digit.
    size_t lead = 0;
    while (lead + 1 < digits_.size() && digits_[lead] == 0) ++lead;
    if (lead > 0) digits_.erase(digits_.begin(), digits_.begin() + lead);

    if (digits_.empty() || isAllZero(digits_)) {
        digits_ = {0};
        sign_ = 1;
    }
    precision_ = static_cast<int>(digits_.size());
    if (precision_ > kMaxPrecision)
        throw std::invalid_argument("numeric precision exceeds maximum");
}

int Numeric::sign() const {
    if (nan_ || inf_) return 0;
    if (digits_.size() == 1 && digits_[0] == 0) return 0;
    return sign_;
}

std::string Numeric::toString() const {
    if (nan_) return "NaN";
    if (inf_) return (sign_ > 0) ? "Infinity" : "-Infinity";
    std::string res;
    if (sign_ < 0) res.push_back('-');

    int len = static_cast<int>(digits_.size());
    int intLen = len - scale_;
    if (intLen <= 0) {
        res.push_back('0');
    } else {
        for (int i = 0; i < intLen; ++i) res.push_back(static_cast<char>('0' + digits_[i]));
    }
    if (scale_ > 0) {
        res.push_back('.');
        int fracStart = std::max(0, intLen);
        int fracDigits = len - fracStart;
        int leadingZeros = scale_ - fracDigits;
        res.append(leadingZeros, '0');
        for (int i = fracStart; i < len; ++i) res.push_back(static_cast<char>('0' + digits_[i]));
    }
    return res;
}

Numeric Numeric::operator-() const {
    if (nan_) return *this;
    if (!inf_ && sign() == 0) return *this;
    Numeric r = *this;
    r.sign_ = -r.sign_;
    return r;
}

int Numeric::compare(const Numeric& rhs) const {
    if (nan_ || rhs.nan_) {
        // Treat NaN as unordered: less than everything.
        if (nan_ && rhs.nan_) return 0;
        return nan_ ? 1 : -1;
    }
    if (inf_ || rhs.inf_) {
        if (inf_ && rhs.inf_) {
            if (sign_ == rhs.sign_) return 0;
            return sign_ > rhs.sign_ ? 1 : -1;
        }
        if (inf_) return sign_ > 0 ? 1 : -1;
        return rhs.sign_ > 0 ? -1 : 1;
    }
    int s1 = sign();
    int s2 = rhs.sign();
    if (s1 != s2) return s1 > s2 ? 1 : -1;
    if (s1 == 0) return 0;
    int cm = compareMagnitudes(*this, rhs);
    return (s1 > 0) ? cm : -cm;
}

bool Numeric::operator==(const Numeric& rhs) const {
    return compare(rhs) == 0;
}

Numeric Numeric::operator+(const Numeric& rhs) const {
    if (nan_ || rhs.nan_) return nan();
    if (inf_ || rhs.inf_) {
        if (inf_ && rhs.inf_) {
            if (sign_ == rhs.sign_) return *this;
            return nan();
        }
        return inf_ ? *this : rhs;
    }
    if (sign_ == rhs.sign_) {
        Numeric r = addMagnitudes(*this, rhs);
        r.sign_ = sign_;
        return r;
    }
    int cm = compareMagnitudes(*this, rhs);
    if (cm == 0) {
        Numeric result(0);
        result.scale_ = std::max(scale_, rhs.scale_);
        return result;
    }
    if (cm > 0) {
        Numeric r = subMagnitudes(*this, rhs);
        r.sign_ = sign_;
        return r;
    }
    Numeric r = subMagnitudes(rhs, *this);
    r.sign_ = rhs.sign_;
    return r;
}

Numeric Numeric::operator-(const Numeric& rhs) const {
    return *this + (-rhs);
}

Numeric Numeric::operator*(const Numeric& rhs) const {
    if (nan_ || rhs.nan_) return nan();
    if (inf_ || rhs.inf_) {
        if (inf_ && rhs.inf_) {
            return infinity(sign_ * rhs.sign_);
        }
        const Numeric* o = inf_ ? &rhs : this;
        if (o->sign() == 0) return nan();
        return infinity(sign_ * rhs.sign_);
    }
    Numeric r = multiplyMagnitudes(*this, rhs);
    r.sign_ = r.sign() == 0 ? 1 : sign_ * rhs.sign_;
    return r;
}

Numeric Numeric::operator/(const Numeric& rhs) const {
    if (nan_ || rhs.nan_) return nan();
    if (inf_ || rhs.inf_) {
        if (inf_ && rhs.inf_) return nan();
        if (inf_) {
            if (rhs.sign() == 0) return nan();
            return infinity(sign_ * rhs.sign_);
        }
        // rhs is infinite, this is finite
        return Numeric(0);
    }
    if (rhs.sign() == 0) {
        if (sign() == 0) return nan();
        return infinity(sign_ * rhs.sign_);
    }
    // Recover the normalized base-10000 leading group from our decimal
    // digits. PG18 select_div_scale chooses scale from these input groups,
    // not from a rounded quotient or a 16-versus-20 digit heuristic.
    struct LeadingGroup { int weight; int digit; };
    const auto leadingGroup = [](const Numeric& value) {
        if (value.sign() == 0) return LeadingGroup{0, 0};
        const int exponent = static_cast<int>(value.digits_.size()) -
                             value.scale_ - 1;
        const int weight = exponent >= 0 ? exponent / 4 : (exponent - 3) / 4;
        const int width = exponent - weight * 4 + 1;
        int digit = 0;
        for (int i = 0; i < width; ++i) {
            digit = digit * 10 +
                (i < static_cast<int>(value.digits_.size()) ? value.digits_[i] : 0);
        }
        return LeadingGroup{weight, digit};
    };
    const auto left = leadingGroup(*this);
    const auto right = leadingGroup(rhs);
    const int quotientWeight = left.weight - right.weight -
                               (left.digit <= right.digit ? 1 : 0);
    const int resultScale = std::min(kMaxPrecision,
        std::max({0, 16 - quotientWeight * 4, scale_, rhs.scale_}));
    const int extraScale = resultScale - scale_ + rhs.scale_;
    Numeric r = divideMagnitudes(*this, rhs, extraScale);
    r.sign_ = r.sign() == 0 ? 1 : sign_ * rhs.sign_;
    return r;
}

Numeric Numeric::addMagnitudes(const Numeric& a, const Numeric& b) {
    int targetScale = std::max(a.scale_, b.scale_);
    std::vector<uint8_t> av = a.digits_;
    std::vector<uint8_t> bv = b.digits_;
    if (a.scale_ < targetScale) av.insert(av.end(), targetScale - a.scale_, 0);
    if (b.scale_ < targetScale) bv.insert(bv.end(), targetScale - b.scale_, 0);

    Numeric r;
    r.digits_ = addDigitVectors(av, bv);
    r.scale_ = targetScale;
    r.normalize();
    return r;
}

Numeric Numeric::subMagnitudes(const Numeric& a, const Numeric& b) {
    int targetScale = std::max(a.scale_, b.scale_);
    std::vector<uint8_t> av = a.digits_;
    std::vector<uint8_t> bv = b.digits_;
    if (a.scale_ < targetScale) av.insert(av.end(), targetScale - a.scale_, 0);
    if (b.scale_ < targetScale) bv.insert(bv.end(), targetScale - b.scale_, 0);

    Numeric r;
    r.digits_ = subDigitVectors(av, bv);
    r.scale_ = targetScale;
    r.normalize();
    return r;
}

int Numeric::compareMagnitudes(const Numeric& a, const Numeric& b) {
    int targetScale = std::max(a.scale_, b.scale_);
    std::vector<uint8_t> av = a.digits_;
    std::vector<uint8_t> bv = b.digits_;
    if (a.scale_ < targetScale) av.insert(av.end(), targetScale - a.scale_, 0);
    if (b.scale_ < targetScale) bv.insert(bv.end(), targetScale - b.scale_, 0);
    return cmpDigitVectors(av, bv);
}

Numeric Numeric::multiplyMagnitudes(const Numeric& a, const Numeric& b) {
    Numeric r;
    r.digits_ = mulDigitVectors(a.digits_, b.digits_);
    r.scale_ = a.scale_ + b.scale_;
    r.normalize();
    return r;
}

Numeric Numeric::divideMagnitudes(const Numeric& a, const Numeric& b, int extraScale) {
    // Scale either magnitude before a single rounded integer division.
    // A negative extraScale shifts the divisor; truncating the dividend
    // first would lose the remainder needed for correct final rounding.
    std::vector<uint8_t> D = a.digits_;
    std::vector<uint8_t> V = b.digits_;
    if (extraScale >= 0) D.insert(D.end(), extraScale, 0);
    else V.insert(V.end(), -extraScale, 0);
    std::vector<uint8_t> Q, R;
    divDigitVectors(D, V, Q, R);

    // Round half-up using the full remainder and the scaled divisor.
    int carry = 0;
    for (int i = static_cast<int>(R.size()) - 1; i >= 0; --i) {
        int v = R[i] * 2 + carry;
        R[i] = static_cast<uint8_t>(v % 10);
        carry = v / 10;
    }
    if (carry) R.insert(R.begin(), 1);
    if (cmpDigitVectors(R, V) >= 0) {
        incrementDigitVector(Q);
    }

    Numeric r;
    r.digits_ = std::move(Q);
    r.scale_ = extraScale + a.scale_ - b.scale_;
    if (r.scale_ < 0) {
        // Result is an integer with no fractional digits requested; pad right.
        r.digits_.insert(r.digits_.end(), -r.scale_, 0);
        r.scale_ = 0;
    }
    r.normalize();
    return r;
}

Numeric Numeric::withScale(int newScale, RoundingMode mode) const {
    (void)mode; // only half-up implemented
    if (nan_ || inf_) return *this;
    Numeric r;
    if (newScale >= scale_) {
        const int64_t addedDigits =
            static_cast<int64_t>(newScale) - scale_;
        if (addedDigits > kMaxPrecision -
                              static_cast<int64_t>(digits_.size())) {
            throw std::invalid_argument(
                "numeric precision exceeds maximum");
        }
        r.digits_ = digits_;
        r.digits_.insert(r.digits_.end(),
                         static_cast<size_t>(addedDigits), 0);
        r.scale_ = newScale;
    } else {
        const int64_t droppedDigits =
            static_cast<int64_t>(scale_) - newScale;
        if (droppedDigits > static_cast<int64_t>(digits_.size())) {
            Numeric zero(0);
            zero.scale_ = std::max(0, newScale);
            return zero;
        }
        r.digits_ = divideByPowerOf10(
            digits_, static_cast<int>(droppedDigits));
        if (newScale < 0 && !isAllZero(r.digits_)) {
            const int64_t integerZeros = -static_cast<int64_t>(newScale);
            if (integerZeros > kMaxPrecision -
                                   static_cast<int64_t>(r.digits_.size())) {
                throw std::invalid_argument(
                    "numeric precision exceeds maximum");
            }
            r.digits_.insert(r.digits_.end(),
                             static_cast<size_t>(integerZeros), 0);
        }
        r.scale_ = std::max(0, newScale);
    }
    r.sign_ = sign_;
    r.normalize();
    return r;
}

Numeric Numeric::withPrecision(int maxPrecision, RoundingMode mode) const {
    (void)mode;
    if (nan_ || inf_) return *this;
    if (precision_ <= maxPrecision) return *this;
    int drop = precision_ - maxPrecision;
    Numeric r;
    r.digits_ = divideByPowerOf10(digits_, drop);
    r.scale_ = std::max(0, scale_ - drop);
    r.sign_ = sign_;
    r.normalize();
    return r;
}

} // namespace dbms
