#include "types/numeric.h"
#include <cassert>
#include <iostream>

int main() {
    using dbms::Numeric;
    assert(Numeric("-0.00").toString() == "0.00");
    assert(Numeric("0.000").toString() == "0.000");
    assert((-Numeric("0.000")).toString() == "0.000");
    assert((Numeric("0.00") + Numeric("1.0")).toString() == "1.00");
    assert((Numeric("1.0") + Numeric("0.00")).toString() == "1.00");
    assert((Numeric("0.00") + Numeric("0.000")).toString() == "0.000");
    assert((Numeric("1.25") - Numeric("1.25")).toString() == "0.00");
    assert((Numeric("0.00") * Numeric("1.0")).toString() == "0.000");
    assert((Numeric("1.0") * Numeric("0.00")).toString() == "0.000");
    assert((Numeric("0.00") * Numeric("-1.0")).toString() == "0.000");
    assert(Numeric("0.00001").withScale(2).toString() == "0.00");
    assert(Numeric("0.00000").withScale(2).toString() == "0.00");
    assert(Numeric("0").withScale(2).toString() == "0.00");
    assert(Numeric("0.001").withScale(2).toString() == "0.00");
    assert(Numeric("0.00").scale() == 2);
    assert(Numeric("-0.00") == Numeric("0"));
    assert(!(Numeric("0.000") < Numeric("0.00")));
    std::cout << "[NUMERIC ZERO SCALE] passed\n";
}
