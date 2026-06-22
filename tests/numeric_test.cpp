#include "types/numeric.h"
#include <cassert>
#include <iostream>
#include <string>

using dbms::Numeric;

static void test_basic_io() {
    assert(Numeric(0).toString() == "0");
    assert(Numeric(123).toString() == "123");
    assert(Numeric(-456).toString() == "-456");
    assert(Numeric("123.45").toString() == "123.45");
    assert(Numeric("-0.0012300").toString() == "-0.0012300");
    assert(Numeric(".5").toString() == "0.5");
    assert(Numeric("00042.00").toString() == "42.00");
    assert(Numeric("NaN").toString() == "NaN");
    assert(Numeric("Infinity").toString() == "Infinity");
    assert(Numeric("-Infinity").toString() == "-Infinity");
    std::cout << "[NUMERIC] basic I/O OK" << std::endl;
}

static void test_addition() {
    assert((Numeric("1.23") + Numeric("2.77")).toString() == "4.00");
    assert((Numeric("0.1") + Numeric("0.2")).toString() == "0.3");
    assert((Numeric("999.99") + Numeric("0.01")).toString() == "1000.00");
    assert((Numeric("-5") + Numeric("3")).toString() == "-2");
    assert((Numeric("-5") + Numeric("5")).toString() == "0");
    assert((Numeric("123.456") + Numeric("0")).toString() == "123.456");
    std::cout << "[NUMERIC] addition OK" << std::endl;
}

static void test_subtraction() {
    assert((Numeric("5") - Numeric("3")).toString() == "2");
    assert((Numeric("1.0") - Numeric("0.9")).toString() == "0.1");
    assert((Numeric("0.001") - Numeric("0.0001")).toString() == "0.0009");
    assert((Numeric("-3") - Numeric("-7")).toString() == "4");
    std::cout << "[NUMERIC] subtraction OK" << std::endl;
}

static void test_multiplication() {
    assert((Numeric("2") * Numeric("3")).toString() == "6");
    assert((Numeric("1.5") * Numeric("2.5")).toString() == "3.75");
    assert((Numeric("0.1") * Numeric("0.02")).toString() == "0.002");
    assert((Numeric("-1.1") * Numeric("1.1")).toString() == "-1.21");
    std::cout << "[NUMERIC] multiplication OK" << std::endl;
}

static void test_division() {
    assert((Numeric("10") / Numeric("4")).toString() == "2.5000");
    assert((Numeric("1") / Numeric("3")).toString() == "0.3333");
    assert((Numeric("22") / Numeric("7")).toString() == "3.1429");
    assert((Numeric("-15") / Numeric("3")).toString() == "-5.0000");
    std::cout << "[NUMERIC] division OK" << std::endl;
}

static void test_comparison() {
    assert(Numeric("1.0") == Numeric("1"));
    assert(Numeric("1.0") != Numeric("1.1"));
    assert(Numeric("0.3") > Numeric("0.299"));
    assert(Numeric("-1") < Numeric("1"));
    assert(Numeric("-2") < Numeric("-1"));
    assert(Numeric("0") >= Numeric("-0.001"));
    std::cout << "[NUMERIC] comparison OK" << std::endl;
}

static void test_scale_precision() {
    assert(Numeric("123.456").withScale(2).toString() == "123.46");
    assert(Numeric("123.454").withScale(2).toString() == "123.45");
    assert(Numeric("123.456").withScale(5).toString() == "123.45600");
    assert(Numeric("123.456").withPrecision(4).toString() == "123.5");
    assert(Numeric("0.00123").withPrecision(2).toString() == "0.0012");
    std::cout << "[NUMERIC] scale/precision OK" << std::endl;
}

static void test_nan_inf() {
    Numeric nan = Numeric::nan();
    Numeric inf = Numeric::infinity();
    Numeric ninf = Numeric::infinity(-1);
    assert(nan.isNaN());
    assert(inf.isInfinite());
    assert((inf + Numeric("1")).isInfinite());
    assert((inf + ninf).isNaN());
    assert((Numeric("1") / Numeric("0")).isInfinite());
    assert((Numeric("0") / Numeric("0")).isNaN());
    assert((Numeric("1") * Numeric("0")).toString() == "0");
    std::cout << "[NUMERIC] NaN/Inf OK" << std::endl;
}

int main() {
    test_basic_io();
    test_addition();
    test_subtraction();
    test_multiplication();
    test_division();
    test_comparison();
    test_scale_precision();
    test_nan_inf();
    std::cout << "[NUMERIC] all passed" << std::endl;
    return 0;
}
