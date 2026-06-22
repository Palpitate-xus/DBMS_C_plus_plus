#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace dbms {

// Lightweight arbitrary-precision decimal (numeric/decimal) with NaN and
// signed Infinity support.  Digits are stored in sign-magnitude form as a
// sequence of decimal digits; `scale` is the number of digits after the
// decimal point.  Values are kept normalized (no leading/trailing zeros).
class Numeric {
public:
    enum class RoundingMode { HalfUp };

    Numeric() = default;
    explicit Numeric(int64_t v);
    explicit Numeric(const std::string& s);

    static Numeric fromString(const std::string& s);
    static Numeric nan();
    static Numeric infinity(int sign = 1);

    bool isNaN() const { return nan_; }
    bool isInfinite() const { return inf_; }
    bool isFinite() const { return !nan_ && !inf_; }
    int sign() const;

    int precision() const { return precision_; }
    int scale() const { return scale_; }

    std::string toString() const;

    Numeric operator-() const;
    Numeric operator+(const Numeric& rhs) const;
    Numeric operator-(const Numeric& rhs) const;
    Numeric operator*(const Numeric& rhs) const;
    Numeric operator/(const Numeric& rhs) const;

    Numeric& operator+=(const Numeric& rhs) { *this = *this + rhs; return *this; }
    Numeric& operator-=(const Numeric& rhs) { *this = *this - rhs; return *this; }
    Numeric& operator*=(const Numeric& rhs) { *this = *this * rhs; return *this; }
    Numeric& operator/=(const Numeric& rhs) { *this = *this / rhs; return *this; }

    bool operator==(const Numeric& rhs) const;
    bool operator!=(const Numeric& rhs) const { return !(*this == rhs); }
    bool operator<(const Numeric& rhs) const  { return compare(rhs) < 0; }
    bool operator<=(const Numeric& rhs) const { return compare(rhs) <= 0; }
    bool operator>(const Numeric& rhs) const  { return compare(rhs) > 0; }
    bool operator>=(const Numeric& rhs) const { return compare(rhs) >= 0; }

    // Round/truncate to the requested number of fractional digits.
    Numeric withScale(int newScale, RoundingMode mode = RoundingMode::HalfUp) const;

    // Clamp the total number of significant digits to `maxPrecision`.
    Numeric withPrecision(int maxPrecision, RoundingMode mode = RoundingMode::HalfUp) const;

    static constexpr int kMaxPrecision = 1000;

private:
    bool nan_ = false;
    bool inf_ = false;
    int sign_ = 1;                       // +1 or -1, valid only when finite
    std::vector<uint8_t> digits_;        // most significant digit first
    int scale_ = 0;                      // digits after decimal point
    int precision_ = 0;                  // significant digits

    Numeric(bool nan, bool inf, int sign)
        : nan_(nan), inf_(inf), sign_(sign) {}

    void normalize();
    int compare(const Numeric& rhs) const;

    static Numeric addMagnitudes(const Numeric& a, const Numeric& b);
    static Numeric subMagnitudes(const Numeric& a, const Numeric& b);
    static int compareMagnitudes(const Numeric& a, const Numeric& b);
    static Numeric multiplyMagnitudes(const Numeric& a, const Numeric& b);
    static Numeric divideMagnitudes(const Numeric& a, const Numeric& b, int resultScale);
};

} // namespace dbms
