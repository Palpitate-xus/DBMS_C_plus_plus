#include "types/numeric.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <iomanip>
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
// Both D and V are most-significant-digit-first magnitude vectors.
void divDigitVectors(const std::vector<uint8_t>& D,
                     const std::vector<uint8_t>& V,
                     std::vector<uint8_t>& quotient,
                     std::vector<uint8_t>& remainder) {
    if (isAllZero(V)) throw std::runtime_error("division by zero");
    quotient.assign(D.size(), 0);
    std::vector<uint8_t> rem;
    rem.reserve(std::min(D.size(), V.size() + 1));
    // Keep the significant suffix in place. Each completed division step has
    // remainder < V; periodic compaction bounds storage by O(V.size()).
    size_t head = 0;
    const auto trim = [&]() {
        while (head + 1 < rem.size() && rem[head] == 0) ++head;
    };
    const auto compare = [&]() {
        const size_t n = rem.size() - head;
        if (n != V.size()) return n < V.size() ? -1 : 1;
        for (size_t i = 0; i < n; ++i)
            if (rem[head + i] != V[i]) return rem[head + i] < V[i] ? -1 : 1;
        return 0;
    };
    for (size_t idx = 0; idx < D.size(); ++idx) {
        rem.push_back(D[idx]);
        trim();
        // The incoming remainder is below V, hence after appending a digit
        // the quotient digit is at most nine for a normalized nonzero V.
        int q = 0;
        while (compare() >= 0) {
            // rem >= V, so borrow cannot propagate before the active suffix.
            size_t i = rem.size(), j = V.size();
            int borrow = 0;
            while (j || borrow) {
                const int v = static_cast<int>(rem[--i]) - borrow -
                              (j ? static_cast<int>(V[--j]) : 0);
                borrow = v < 0;
                rem[i] = static_cast<uint8_t>(v + (borrow ? 10 : 0));
            }
            trim();
            ++q;
            if (q > 9) break;
        }
        quotient[idx] = static_cast<uint8_t>(q);
        if (head > V.size()) {
            rem.erase(rem.begin(), rem.begin() + head);
            head = 0;
        }
    }
    if (rem.empty()) remainder = {0};
    else remainder.assign(rem.begin() + head, rem.end());
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
        precision_ = 1;
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
                throw NumericRangeError("numeric exponent out of range");
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
        throw NumericRangeError("numeric scale exceeds maximum");
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
        throw NumericRangeError("numeric exponent out of range");
    }
    const int64_t resultingScale = fractionalScale - exponent;
    if (resultingScale < 0) {
        const uint64_t zeros = static_cast<uint64_t>(-(resultingScale + 1)) + 1;
        const uint64_t availableDigits = n.digits_.size() < kMaxPrecision
            ? static_cast<uint64_t>(kMaxPrecision - n.digits_.size()) : 0;
        if (zeros > availableDigits) {
            throw NumericRangeError("numeric precision exceeds maximum");
        }
        n.digits_.insert(n.digits_.end(), static_cast<size_t>(zeros), 0);
        n.scale_ = 0;
    } else {
        if (resultingScale > kMaxPrecision)
            throw NumericRangeError("numeric scale exceeds maximum");
        n.scale_ = static_cast<int>(resultingScale);
    }

    n.normalize();
    if (n.precision_ > kMaxPrecision) {
        throw NumericRangeError("numeric precision exceeds maximum");
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
        throw NumericRangeError("numeric precision exceeds maximum");
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

Numeric Numeric::truncDivide(const Numeric& rhs) const {
    // Keep the native special-value and zero-divisor behavior of operator/.
    // SQL callers independently implement their existing division-by-zero rule.
    if (!isFinite() || !rhs.isFinite() || rhs.sign() == 0)
        return *this / rhs;

    // |a| / |b| = (a_digits * 10^b_scale) / (b_digits * 10^a_scale).
    // Apply only the scale difference and divide integer magnitudes once.
    if (sign() == 0) return Numeric(0);
    // Native Numeric products may have much larger scales than parsed inputs.
    // Decide the obvious outcomes by length before allocating shifted digits.
    const int64_t shift = static_cast<int64_t>(rhs.scale_) - scale_;
    const int64_t dividendSize = static_cast<int64_t>(digits_.size()) + std::max<int64_t>(shift, 0);
    const int64_t divisorSize = static_cast<int64_t>(rhs.digits_.size()) + std::max<int64_t>(-shift, 0);
    if (dividendSize < divisorSize) return Numeric(0);
    if (dividendSize - divisorSize > kMaxPrecision)
        throw NumericRangeError("numeric precision exceeds maximum");
    std::vector<uint8_t> dividend = digits_;
    std::vector<uint8_t> divisor = rhs.digits_;
    if (shift > 0) dividend.insert(dividend.end(), static_cast<size_t>(shift), 0);
    else if (shift < 0) divisor.insert(divisor.end(), static_cast<size_t>(-shift), 0);
    std::vector<uint8_t> quotient, remainder;
    divDigitVectors(dividend, divisor, quotient, remainder);
    Numeric result;
    result.digits_ = std::move(quotient);
    result.scale_ = 0;
    result.sign_ = sign_ * rhs.sign_;
    result.normalize(); // Reject only an oversized final integer quotient.
    return result;
}

Numeric Numeric::exactRemainder(const Numeric& rhs) const {
    if (isNaN() || rhs.isNaN()) return nan();
    if (rhs.isFinite() && rhs.sign() == 0)
        throw std::domain_error("division by zero");
    if (isInfinite()) return nan();
    if (rhs.isInfinite()) return *this;

    const int commonScale = std::max(scale_, rhs.scale_);
    if (sign() == 0) {
        Numeric zero(0);
        zero.scale_ = commonScale;
        return zero;
    }
    const int64_t leftShift = static_cast<int64_t>(commonScale) - scale_;
    const int64_t rightShift = static_cast<int64_t>(commonScale) - rhs.scale_;
    const int64_t leftSize = static_cast<int64_t>(digits_.size()) + leftShift;
    const int64_t rightSize = static_cast<int64_t>(rhs.digits_.size()) + rightShift;
    if (leftSize < rightSize) return withScale(commonScale);
    // Parsed SQL operands fit this bound. Native callers can construct huge
    // scales via multiplication; reject rather than allocate unbounded zeros.
    constexpr int64_t maxAlignedDigits = 2LL * kMaxPrecision;
    if (leftSize > maxAlignedDigits || rightSize > maxAlignedDigits)
        throw NumericRangeError("numeric scale exceeds maximum");
    std::vector<uint8_t> dividend = digits_;
    std::vector<uint8_t> divisor = rhs.digits_;
    dividend.insert(dividend.end(), static_cast<size_t>(leftShift), 0);
    divisor.insert(divisor.end(), static_cast<size_t>(rightShift), 0);
    std::vector<uint8_t> quotient, remainder;
    divDigitVectors(dividend, divisor, quotient, remainder);
    Numeric result;
    result.digits_ = std::move(remainder);
    result.scale_ = commonScale;
    result.sign_ = sign_;
    result.normalize();
    return result;
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
    const int64_t scale = static_cast<int64_t>(a.scale_) + b.scale_;
    if (scale > std::numeric_limits<int>::max())
        throw NumericRangeError("numeric scale exceeds maximum");
    Numeric r;
    r.digits_ = mulDigitVectors(a.digits_, b.digits_);
    r.scale_ = static_cast<int>(scale);
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
    // Zero keeps a single magnitude digit. Within the parser scale domain,
    // padding display zeros cannot overflow its significant-digit budget.
    // Larger scales still follow the existing admission checks below.
    if (newScale <= kMaxPrecision && isAllZero(digits_)) {
        Numeric zero(0);
        zero.scale_ = std::max(0, newScale);
        return zero;
    }
    Numeric r;
    if (newScale >= scale_) {
        const int64_t addedDigits =
            static_cast<int64_t>(newScale) - scale_;
        if (addedDigits > kMaxPrecision -
                              static_cast<int64_t>(digits_.size())) {
            throw NumericRangeError(
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
                throw NumericRangeError(
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
    if (maxPrecision < 1 || maxPrecision > kMaxPrecision)
        throw std::invalid_argument("numeric precision must be between 1 and 1000");
    if (nan_ || inf_) return *this;
    if (precision_ <= maxPrecision) return *this;
    const int drop = precision_ - maxPrecision;
    // A negative target scale rounds integer places. Clamping it to zero
    // after dropping digits would shrink 12345 to 123 rather than 12300.
    // withScale handles rounding carry, restores integer zeros and checks
    // the implementation's precision limit before publishing the result.
    return withScale(scale_ - drop, mode);
}

Numeric Numeric::squareRoot() const {
    if (nan_) return *this;
    if (sign_ < 0 && (inf_ || !isAllZero(digits_)))
        throw std::domain_error("cannot take square root of a negative number");
    if (inf_) return *this;

    // PG numeric's weight counts base-10000 digits. Floor, not truncation,
    // is necessary for values below one; zero has normalized weight zero.
    const int decimalWeight = isAllZero(digits_) ? 0
        : static_cast<int>(digits_.size()) - scale_ - 1;
    const int weight = decimalWeight >= 0 ? decimalWeight / 4
        : -((-decimalWeight + 3) / 4);
    const int resultScale = std::min(kMaxPrecision,
        std::max({0, scale_, 16 - (2 * weight + 1)}));

    // Calculate one guard digit. sqrt(floor(x)) and sqrt(x) have the same
    // integer floor, so fractional digits below this precision may be cut.
    // The longhand algorithm uses only decimal digit vectors: no double,
    // floating-to-integer conversion, or bounded integer sum is involved.
    auto radicand = digits_;
    const int shift = 2 * (resultScale + 1) - scale_;
    if (shift >= 0) radicand.insert(radicand.end(), shift, 0);
    else if (static_cast<size_t>(-shift) >= radicand.size()) radicand = {0};
    else radicand.resize(radicand.size() - static_cast<size_t>(-shift));
    stripLeadingZeros(radicand);
    std::vector<uint8_t> root{0}, remainder{0};
    size_t position = 0;
    size_t pairWidth = radicand.size() % 2 ? 1 : 2;
    while (position < radicand.size()) {
        remainder.insert(remainder.end(), pairWidth, 0);
        const std::vector<uint8_t> pair(radicand.begin() + position,
                                       radicand.begin() + position + pairWidth);
        remainder = addDigitVectors(remainder, pair);
        stripLeadingZeros(remainder);
        const auto twentyRoot = mulDigitVectors(root, {2, 0});
        uint8_t digit = 9;
        std::vector<uint8_t> candidate;
        for (;;) {
            candidate = mulDigitVectors(addDigitVectors(twentyRoot, {digit}), {digit});
            if (cmpDigitVectors(candidate, remainder) <= 0) break;
            --digit;
        }
        remainder = subDigitVectors(remainder, candidate);
        root.push_back(digit);
        stripLeadingZeros(root);
        position += pairWidth;
        pairWidth = 2;
    }
    Numeric result;
    result.digits_ = divideByPowerOf10(root, 1);
    result.scale_ = resultScale;
    result.normalize();
    return result;
}

} // namespace dbms

// Decimal exponential adapted from PostgreSQL REL_18_6 numeric.c.
// Copyright (c) 1998-2025, PostgreSQL Global Development Group.
// See THIRD_PARTY_NOTICES for the PostgreSQL license.
namespace dbms {
namespace {
namespace exp_detail {

using V = std::vector<uint32_t>; // little-endian base-10000 digits
constexpr uint32_t B = 10000;

void trim(V& v) {
    while (v.size() > 1 && v.back() == 0) v.pop_back();
}

V parse(const std::string& s) {
    V v;
    for (int end = static_cast<int>(s.size()); end > 0; end -= 4) {
        const int begin = std::max(0, end - 4);
        v.push_back(static_cast<uint32_t>(std::stoul(s.substr(begin, end - begin))));
    }
    if (v.empty()) v.push_back(0);
    trim(v);
    return v;
}

std::string str(const V& v) {
    std::ostringstream out;
    out << v.back();
    for (int i = static_cast<int>(v.size()) - 2; i >= 0; --i)
        out << std::setw(4) << std::setfill('0') << v[i];
    return out.str();
}

void add(V& a, const V& b) {
    a.resize(std::max(a.size(), b.size()), 0);
    uint32_t carry = 0;
    for (size_t i = 0; i < a.size(); ++i) {
        const uint32_t n = a[i] + (i < b.size() ? b[i] : 0) + carry;
        a[i] = n % B;
        carry = n / B;
    }
    if (carry) a.push_back(carry);
}

void sub(V& a, const V& b) {
    int borrow = 0;
    for (size_t i = 0; i < a.size(); ++i) {
        const int n = static_cast<int>(a[i]) - (i < b.size() ? static_cast<int>(b[i]) : 0) - borrow;
        borrow = n < 0;
        a[i] = static_cast<uint32_t>(n + (borrow ? static_cast<int>(B) : 0));
    }
    if (borrow) throw NumericRangeError("numeric exponential negative intermediate");
    trim(a);
}

void divsmall(V& a, uint32_t divisor) {
    uint64_t remainder = 0;
    for (size_t i = a.size(); i-- > 0;) {
        const uint64_t current = remainder * B + a[i];
        a[i] = current / divisor;
        remainder = current % divisor;
    }
    trim(a);
}

// PostgreSQL mul_var bounds output in pairs of base-10000 digits, retaining
// two guard digits. Drop the same cross-products before the final rounding.
V pg_mul(const V& aa, const V& bb, int work, int rscale) {
    auto digits = [](const V& v) {
        V d = v;
        trim(d);
        while (d.size() > 1 && d.front() == 0) d.erase(d.begin());
        std::reverse(d.begin(), d.end());
        return d;
    };
    if ((aa.size() == 1 && aa[0] == 0) || (bb.size() == 1 && bb[0] == 0))
        return V{0};
    const size_t limbLimit = static_cast<size_t>((work + 1024) / 4 + 2);
    if (aa.size() > limbLimit || bb.size() > limbLimit)
        throw NumericRangeError("numeric exponential operand limit exceeded");
    V a = digits(aa), b = digits(bb);
    int aw = static_cast<int>(aa.size()) - 1 - work / 4;
    int bw = static_cast<int>(bb.size()) - 1 - work / 4;
    if (a.size() > b.size()) { std::swap(a, b); std::swap(aw, bw); }
    const int na = static_cast<int>(a.size()), nb = static_cast<int>(b.size());
    int pa = (na + 1) / 2, pb = (nb + 1) / 2;
    const int nd = na + nb;
    int rp = nd / 2 + 1;
    const int offset = rp - pa - pb + 1;
    const int rw = aw + bw + 1 + 2 * rp - nd - (na & 1) - (nb & 1);
    const int maxdigits = rw + 1 + (rscale + 3) / 4 + 2;
    rp = std::min(rp, maxdigits / 2 + 1);
    if (rp <= offset) return V{0};
    pa = std::min(pa, rp - offset);
    pb = std::min(pb, rp - offset);
    std::vector<uint64_t> d(rp);
    for (int i = 0; i < pa; ++i) {
        const uint64_t av = uint64_t(a[2 * i]) * B + (2 * i + 1 < na ? a[2 * i + 1] : 0);
        for (int j = 0; j < pb && i + j + offset < rp; ++j) {
            const uint64_t bv = uint64_t(b[2 * j]) * B + (2 * j + 1 < nb ? b[2 * j + 1] : 0);
            const uint64_t product = av * bv;
            uint64_t& cell = d[i + j + offset];
            if (product > std::numeric_limits<uint64_t>::max() - cell)
                throw NumericRangeError("numeric exponential accumulator limit exceeded");
            cell += product;
        }
    }
    uint64_t carry = 0;
    V out(1, 0);
    for (int i = rp - 1; i >= 0; --i) {
        if (carry > std::numeric_limits<uint64_t>::max() - d[i])
            throw NumericRangeError("numeric exponential carry limit exceeded");
        uint64_t z = d[i] + carry;
        carry = z / (uint64_t(B) * B);
        z %= uint64_t(B) * B;
        for (int k = 0; k < 2; ++k) {
            const int j = 2 * i + k, pos = work / 4 + rw - j;
            const uint32_t limb = k ? z % B : z / B;
            if (pos >= 0) {
                if (static_cast<int>(out.size()) <= pos) out.resize(pos + 1);
                out[pos] = limb;
            } else if (limb) {
                throw NumericRangeError("numeric exponential working precision insufficient");
            }
        }
    }
    if (carry) throw NumericRangeError("numeric exponential carry overflow");
    trim(out);
    return out;
}

void roundscale(V& a, int work, int scale) {
    if (scale >= work) return;
    const int n = work - scale, limbs = n / 4, digits = n % 4;
    uint32_t p = 1;
    for (int i = 0; i < digits; ++i) p *= 10;
    bool up = false;
    if (digits)
        up = limbs < static_cast<int>(a.size()) && (a[limbs] % p) * 2 >= p;
    else if (limbs > 0)
        up = limbs - 1 < static_cast<int>(a.size()) && a[limbs - 1] >= B / 2;
    for (int i = 0; i < std::min(limbs, static_cast<int>(a.size())); ++i) a[i] = 0;
    if (digits && limbs < static_cast<int>(a.size())) a[limbs] -= a[limbs] % p;
    if (up) { V increment(limbs + 1, 0); increment[limbs] = p; add(a, increment); }
    trim(a);
}

int weight(const V& a, int work) {
    const std::string s = str(a);
    const int dp = static_cast<int>(s.size()) - work - 1;
    return dp >= 0 ? dp / 4 : -((-dp + 3) / 4);
}

} // namespace exp_detail
} // namespace
} // namespace dbms

namespace dbms {
Numeric Numeric::exponential() const {
    using namespace exp_detail;
    if (nan_) return Numeric::nan();
    if (inf_) return sign_ < 0 ? Numeric(int64_t{0}) : Numeric::infinity();
    if (*this <= Numeric(int64_t{-2304}))
        return Numeric::fromString("0." + std::string(kMaxPrecision, '0'));
    if (*this >= Numeric(int64_t{2304}))
        throw NumericRangeError("numeric exponential result exceeds maximum precision");

    // The input is already a validated Numeric, not arbitrary SQL text.
    // PostgreSQL uses strtod (including underflow to zero) only for scale
    // selection and range reduction, never to calculate the decimal value.
    const std::string canonical = toString();
    char* end = nullptr;
    const double xd = std::strtod(canonical.c_str(), &end);
    if (!end || *end != '\0' || !std::isfinite(xd))
        throw NumericRangeError("numeric exponential scale conversion failed");
    const int scale = scale_;
    const int rs = std::min(kMaxPrecision, std::max({0, scale,
                             16 - static_cast<int>(xd * 0.434294481903252)}));
    const int dw = static_cast<int>(xd * 0.434294481903252);
    double v = std::abs(xd);
    int halves = 0;
    while (v > 0.01) { v /= 2; ++halves; }
    const int sig = std::max(1 + dw + rs +
                           static_cast<int>(halves * 0.301029995663981), 0) + 8;
    int work = std::max({scale + halves + 4, rs + 8,
                         sig + std::max(0, -2 * dw) + 32, 32});
    work = (work + 3) / 4 * 4;
    // Absolute caps are internal safety limits, not claims of PG coverage.
    if (work > 4096 || halves > 20)
        throw NumericRangeError("numeric exponential work limit exceeded");
    const size_t shift = static_cast<size_t>(work / 4);
    std::string inputDigits;
    inputDigits.reserve(digits_.size());
    for (auto d : digits_) inputDigits.push_back(static_cast<char>('0' + d));
    V x = parse(inputDigits);
    x.insert(x.begin(), (work - scale) / 4, 0);
    for (int j = 0; j < (work - scale) % 4; ++j) {
        uint32_t c = 0;
        for (auto& digit : x) {
            const uint32_t z = digit * 10 + c;
            digit = z % B; c = z / B;
        }
        if (c) x.push_back(c);
    }
    if (halves) divsmall(x, 1u << halves);
    V one(shift + 1); one[shift] = 1;
    V sum = one;
    if (sign_ < 0) sub(sum, x); else add(sum, x);
    int local = sig - 1;
    V term = pg_mul(x, x, work, local);
    roundscale(term, work, local);
    divsmall(term, 2);
    roundscale(term, work, local);
    uint32_t n = 2;
    while (!(term.size() == 1 && term[0] == 0)) {
        if (sign_ < 0 && n % 2) sub(sum, term); else add(sum, term);
        term = pg_mul(term, x, work, local);
        roundscale(term, work, local);
        ++n;
        if (n > 2048)
            throw NumericRangeError("numeric exponential iteration limit exceeded");
        divsmall(term, n);
        roundscale(term, work, local);
    }
    while (halves--) {
        local = std::max(0, sig - weight(sum, work) * 8);
        sum = pg_mul(sum, sum, work, local);
        roundscale(sum, work, local);
    }
    roundscale(sum, work, rs);
    std::string out = str(sum);
    if (out.size() <= static_cast<size_t>(work))
        out.insert(0, static_cast<size_t>(work) + 1 - out.size(), '0');
    out.resize(out.size() - static_cast<size_t>(work) + static_cast<size_t>(rs));
    if (rs) out.insert(out.size() - static_cast<size_t>(rs), 1, '.');
    // fromString enforces the project's 1000 significant-digit limit.
    return Numeric::fromString(out);
}
} // namespace dbms
