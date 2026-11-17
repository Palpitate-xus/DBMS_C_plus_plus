#include "storage/VariableRow.h"

#include <functional>
#include <iostream>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

using dbms::detail::variableFieldSpan;

static size_t checks = 0;
static void need(bool condition) {
    ++checks;
    if (!condition) throw std::runtime_error("variable row directory regression");
}

template<class T>
static void put(std::string& row, size_t offset, T value) {
    std::memcpy(row.data() + offset, &value, sizeof(value));
}

static std::string encode(const std::vector<std::string>& values, bool wide) {
    // Independent encoder for the documented existing formats. Fixed data
    // deliberately contains bytes that could be mistaken for a text value.
    const size_t begin = 4 + values.size() * 4;
    const size_t entries = wide ? begin + 8 : 4;
    std::string row(wide ? entries + values.size() * 8 : begin, '\0');
    put<uint32_t>(row, 0, 42);
    if (wide) {
        put<uint16_t>(row, 4, 65535);
        put<uint16_t>(row, 6, 65535);
        put<uint32_t>(row, begin, 0x31525654u);
        put<uint32_t>(row, begin + 4, static_cast<uint32_t>(values.size()));
    }
    for (size_t i = 0; i < values.size(); ++i) {
        if (wide) {
            put<uint32_t>(row, entries + i * 8, static_cast<uint32_t>(row.size()));
            put<uint32_t>(row, entries + i * 8 + 4, static_cast<uint32_t>(values[i].size()));
        } else {
            put<uint16_t>(row, entries + i * 4, static_cast<uint16_t>(row.size()));
            put<uint16_t>(row, entries + i * 4 + 2, static_cast<uint16_t>(values[i].size()));
        }
        row += values[i];
    }
    return row;
}

static void corrupt(const std::function<void()>& action) {
    bool caught = false;
    try { action(); }
    catch (const dbms::DbError& error) { caught = error.sqlState() == "XX001"; }
    need(caught);
}

int main() {
    try {
        for (bool wide : {false, true}) {
            const std::vector<std::string> values{"alpha", "", std::string("a\0b", 3)};
            const auto healthy = encode(values, wide);
            const size_t entries = wide ? 4 + values.size() * 4 + 8 : 4;
            const size_t width = wide ? 8 : 4;
            for (size_t i = 0; i < values.size(); ++i) {
                const auto span = variableFieldSpan(healthy, 4, values.size(), i);
                need(healthy.substr(span.offset, span.length) == values[i]);
                need(span.resolved == wide);
            }
            // Every truncated prefix is malformed, including header and
            // directory cuts, as well as truncation within the last value.
            for (size_t n = 0; n < healthy.size(); ++n)
                corrupt([&] { variableFieldSpan(std::string_view(healthy.data(), n), 4, 3, 0); });

            for (size_t selected = 0; selected < values.size(); ++selected) {
                for (unsigned fault = 0; fault < 7; ++fault) {
                    auto bad = healthy;
                    const size_t pos = entries + selected * width;
                    const auto span = variableFieldSpan(healthy, 4, 3, selected);
                    // Past end, excessive length, fixed-data/directory alias,
                    // another attribute's payload, gap and trailing bytes.
                    const size_t offset = fault == 0 ? 65535 : fault == 2 ? 0 :
                        fault == 3 ? entries : fault == 4 ? entries + 3 * width :
                        fault == 5 ? span.offset + 1 : span.offset;
                    const size_t length = fault == 1 ? 65535 : span.length;
                    if (fault == 6) bad += 'x';
                    else if (wide) {
                        put<uint32_t>(bad, pos, static_cast<uint32_t>(offset));
                        put<uint32_t>(bad, pos + 4, static_cast<uint32_t>(length));
                    } else {
                        put<uint16_t>(bad, pos, static_cast<uint16_t>(offset));
                        put<uint16_t>(bad, pos + 2, static_cast<uint16_t>(length));
                    }
                    // Aliasing the first value to itself is the healthy case.
                    if (fault == 4 && selected == 0) continue;
                    corrupt([&] { variableFieldSpan(bad, 4, 3, selected); });
                }
            }
            corrupt([&] { variableFieldSpan(healthy, SIZE_MAX, 3, 0); });
            corrupt([&] { variableFieldSpan(healthy, 4, SIZE_MAX, 0); });
            corrupt([&] { variableFieldSpan(healthy, 4, 3, 3); });
            if (wide) {
                for (size_t pos : {size_t(16), size_t(20)}) {
                    auto bad = healthy; put<uint32_t>(bad, pos, 0);
                    corrupt([&] { variableFieldSpan(bad, 4, 3, 0); });
                }
                auto bad = healthy; put<uint32_t>(bad, entries, UINT32_MAX);
                corrupt([&] { variableFieldSpan(bad, 4, 3, 0); });
            }
        }
        // Exercise different column counts and arbitrary bytes against the
        // original values, rather than a second implementation of validation.
        std::mt19937 random(0x918264u);
        for (size_t n = 1; n <= 64; ++n) {
            std::vector<std::string> values;
            for (size_t i = 0; i < n; ++i) {
                std::string value(random() % 48, '\0');
                for (char& ch : value) ch = static_cast<char>(random());
                values.push_back(std::move(value));
            }
            for (bool wide : {false, true}) {
                const auto row = encode(values, wide);
                for (size_t i = 0; i < n; ++i) {
                    const auto span = variableFieldSpan(row, 4, n, i);
                    need(row.substr(span.offset, span.length) == values[i]);
                }
            }
        }
        const std::vector<std::string> large{std::string(70000, 'a'), "", std::string(80000, 'b')};
        const auto resolved = encode(large, true);
        for (size_t i = 0; i < large.size(); ++i) {
            const auto span = variableFieldSpan(resolved, 4, 3, i);
            need(resolved.substr(span.offset, span.length) == large[i]);
        }
        std::cout << "VARIABLE_ROW_DIRECTORY_PASS checks=" << checks << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
