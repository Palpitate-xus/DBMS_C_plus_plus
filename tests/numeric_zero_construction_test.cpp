// test_sources: src/types/numeric.cpp
#include "types/numeric.h"

#include <array>
#include <cstdint>
#include <exception>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <typeinfo>
#include <utility>
#include <vector>

using dbms::Numeric;

static Numeric makeZero(unsigned factory) {
    switch (factory) {
    case 0: { Numeric value; return value; }
    case 1: return Numeric{};
    case 2: { std::array<Numeric, 2> values{}; return values[1]; }
    case 3: { std::vector<Numeric> values(2); return values[1]; }
    case 4: { std::map<int, Numeric> values; return values[42]; }
    case 5: return Numeric(int64_t{0});
    case 6: return Numeric("0");
    default: return Numeric("-0.000");
    }
}

static Numeric transfer(Numeric value, unsigned kind) {
    if (kind == 0) return value;
    if (kind == 1) { Numeric copy(value); return copy; }
    Numeric destination("987.5");
    destination = std::move(value);
    return destination;
}

static std::string state(const Numeric& value) {
    std::ostringstream out;
    out << value.toString() << '|' << value.sign() << '|' << value.precision()
        << '|' << value.scale() << '|' << value.isFinite() << '|'
        << value.isNaN() << '|' << value.isInfinite();
    return out.str();
}

static std::string operation(const Numeric& zero, const Numeric& operand, unsigned op) {
    try {
        if (op == 0) {
            std::string result;
            for (bool value : {zero == operand, zero != operand, zero < operand,
                               zero <= operand, zero > operand, zero >= operand,
                               operand == zero, operand != zero, operand < zero,
                               operand <= zero, operand > zero, operand >= zero})
                result.push_back(value ? '1' : '0');
            return result;
        }
        if (op == 1) return state(zero + operand);
        if (op == 2) return state(operand + zero);
        if (op == 3) return state(zero - operand);
        if (op == 4) return state(operand - zero);
        if (op == 5) return state(zero * operand);
        if (op == 6) return state(operand * zero);
        if (op == 7) return state(zero / operand);
        return state(operand / zero);
    } catch (const std::exception& error) {
        return std::string("EXCEPTION|") + typeid(error).name() + '|' + error.what();
    }
}

static std::string method(const Numeric& zero, unsigned op) {
    try {
        if (op < 3) return state(zero.withScale(op == 0 ? -2 : op == 1 ? 0 : 2));
        if (op < 6) return state(zero.withPrecision(op == 3 ? 1 : op == 4 ? 3 : 1000));
        return state(zero.squareRoot());
    } catch (const std::exception& error) {
        return std::string("EXCEPTION|") + typeid(error).name() + '|' + error.what();
    }
}

int main() {
    int controls = 0, failures = 0, invariantFailures = 0, operationFailures = 0, methodFailures = 0;
    std::array<int, 8> failedByFactory{};
    try {
        const std::vector<std::string> values = {"-Infinity", "-999.25", "-1", "-0.000",
            "0", "0.00", "0.001", "1", "1.25", "999.25", "Infinity", "NaN"};
        for (unsigned factory = 0; factory < 8; ++factory) {
            const std::string expectedText = factory == 7 ? "0.000" : "0";
            const Numeric canonical(expectedText);
            for (unsigned kind = 0; kind < 3; ++kind) {
                const Numeric zero = transfer(makeZero(factory), kind);
                ++controls;
                const bool invariant = zero.isFinite() && !zero.isNaN() && !zero.isInfinite() &&
                    zero.sign() == 0 && zero.precision() == 1 && zero.scale() == (factory == 7 ? 3 : 0) &&
                    zero.toString() == expectedText && (-zero).toString() == expectedText &&
                    (-zero).sign() == 0 && zero == canonical;
                if (!invariant) {
                    ++failures; ++invariantFailures; ++failedByFactory[factory];
                    std::cout << "ZERO_INVARIANT_FAILED factory=" << factory << " transfer=" << kind
                              << " state=" << state(zero) << '\n';
                }
                for (unsigned op = 0; op < 7; ++op) {
                    ++controls;
                    const auto actual = method(zero, op);
                    const auto expected = method(canonical, op);
                    if (actual != expected) {
                        ++failures; ++methodFailures; ++failedByFactory[factory];
                        std::cout << "ZERO_METHOD_FAILED factory=" << factory << " transfer=" << kind
                                  << " op=" << op << " actual=" << actual << " expected=" << expected << '\n';
                    }
                }
                for (const std::string& text : values) {
                    const Numeric operand(text);
                    for (unsigned op = 0; op < 9; ++op) {
                        ++controls;
                        const auto actual = operation(zero, operand, op);
                        const auto expected = operation(canonical, operand, op);
                        if (actual != expected) {
                            ++failures; ++operationFailures; ++failedByFactory[factory];
                            std::cout << "ZERO_OPERATION_FAILED factory=" << factory << " transfer=" << kind
                                      << " operand=" << text << " op=" << op << " actual=" << actual
                                      << " expected=" << expected << '\n';
                        }
                    }
                }
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "CONTROL_ERROR " << error.what() << '\n';
        return 2;
    }
    if (controls != 2784) { std::cerr << "CONTROL_ERROR fixed matrix size\n"; return 2; }
    for (unsigned factory = 0; factory < 8; ++factory)
        std::cout << "ZERO_FACTORY_FAILURES " << factory << ' ' << failedByFactory[factory] << '\n';
    std::cout << "NUMERIC_ZERO_CONSTRUCTION_COMPLETE " << controls << " FAILURES " << failures
              << " INVARIANT_FAILURES " << invariantFailures << " OPERATION_FAILURES " << operationFailures
              << " METHOD_FAILURES " << methodFailures << '\n';
    return failures ? 1 : 0;
}
