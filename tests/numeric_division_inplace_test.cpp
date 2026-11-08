#include "types/numeric.h"
#include <cstdint>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

using dbms::Numeric;
using dbms::NumericRangeError;
namespace {
unsigned checks = 0;
void require(bool condition, const std::string& description) {
    ++checks;
    if (!condition) throw std::runtime_error(description);
}
struct Case { const char *a, *b, *q, *r, *rounded; };
}
int main() {
    try {
        const Case cases[] = {
            {"100000000000000000001", "9999999999", "10000000001", "2", "10000000001.00000000"},
            {"90000000000000000009", "99999999999", "900000000", "900000009", "900000000.00900000"},
            {"1000000000000000000000000000001", "999999999999999", "1000000000000001", "2", "1000000000000001.0000"},
            {"999999999999999999999999999999", "123456789123456789", "8100000065610", "74710000073709", "8100000065610.0006"},
            {"1", "3", "0", "1", "0.33333333333333333333"},
            {"-1", "6", "0", "-1", "-0.16666666666666666667"},
            {"10", "8", "1", "2", "1.2500000000000000"},
            {"-12", "-5", "2", "-2", "2.4000000000000000"},
            {"999999999999999999", "97", "10309278350515463", "88", "10309278350515464"},
            {"1000000000000000000", "1000000000000000001", "0", "1000000000000000000", "0.99999999999999999900"},
        };
        for (const auto& c : cases) {
            const Numeric a(c.a), b(c.b);
            require(a.truncDivide(b).toString() == c.q, std::string("quotient ")+c.a+" / "+c.b);
            require(a.exactRemainder(b).toString() == c.r, std::string("remainder ")+c.a+" % "+c.b);
            require((a/b).toString() == c.rounded, std::string("rounded divide ")+c.a+" / "+c.b);
        }
        std::mt19937_64 random(719223);
        for (int i = 0; i < 5000; ++i) {
            int64_t a = static_cast<int64_t>(random() % UINT64_C(1000000000000000000));
            int64_t b = static_cast<int64_t>(random() % UINT64_C(1000000000000000000) + 1);
            if (random() & 1) a = -a;
            if (random() & 1) b = -b;
            const Numeric x(a), y(b);
            require(x.truncDivide(y).toString() == std::to_string(a/b), "integer quotient property");
            require(x.exactRemainder(y).toString() == std::to_string(a%b), "integer remainder property");
        }
        const std::string big(1000,'9'), half(500,'9');
        require(Numeric(big).truncDivide(Numeric(half)).toString() == "1"+std::string(499,'0')+"1", "1000-digit quotient with zero run");
        require(Numeric(big).exactRemainder(Numeric(half)).toString() == "0", "1000-digit exact remainder");
        const std::string power="1"+std::string(999,'0'), shift="1"+std::string(499,'0');
        require(Numeric(power).truncDivide(Numeric(half)).toString() == shift, "long borrow quotient");
        require(Numeric(power).exactRemainder(Numeric(half)).toString() == shift, "long borrow remainder");
        require((Numeric("-0.000")/Numeric("7")).sign()==0,"negative zero normalized");
        require(Numeric("NaN").truncDivide(Numeric(0)).isNaN(),"NaN truncation precedence");
        require(Numeric("Infinity").exactRemainder(Numeric(1)).isNaN(),"infinite remainder");
        require(Numeric("-Infinity").truncDivide(Numeric("-2")).toString()=="Infinity","infinite quotient sign");
        bool zero=false;try { (void)Numeric(1).exactRemainder(Numeric(0)); }catch (const std::domain_error&) { zero=true; }
        require(zero,"native remainder zero divisor raises domain_error");
        Numeric tiny("0.1");for(int i=0;i<30;++i)tiny=tiny*tiny;
        require(tiny.scale()==(1<<30),"extreme scale");
        require(tiny.truncDivide(tiny).toString()=="1","extreme scales cancel");
        require(tiny.truncDivide(Numeric(1)).toString()=="0","extreme small quotient");
        require(tiny.exactRemainder(tiny).sign()==0,"extreme equal remainder");
        require(tiny.exactRemainder(Numeric(1)).scale()==tiny.scale(),"extreme small remainder scale");
        bool range=false;try { (void)Numeric(1).truncDivide(tiny); }catch (const NumericRangeError&) { range=true; }
        require(range,"extreme quotient fails before allocation");
        std::cout<<"NUMERIC_DIVISION_INPLACE_CHECKED="<<checks<<" FAILED=0\n";return 0;
    } catch (const std::exception& e) {
        std::cerr<<"NUMERIC_DIVISION_INPLACE_FAILURE="<<checks<<" "<<e.what()<<'\n';return 1;
    }
}
