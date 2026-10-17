#include "common/TemporalStorageCodec.h"

#include <cstdint>
#include <iostream>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

int main() {
    using namespace dbms::temporal_storage_detail;
    size_t controls = 0, failures = 0;
    const auto check = [&](bool good, const std::string& label) {
        ++controls;
        if (!good) {
            ++failures;
            std::cerr << "TEMPORAL_MICRO_CODEC_FAILURE " << label << '\n';
        }
    };
    const std::vector<std::pair<int64_t, std::string>> times = {
        {0, "00:00:00"}, {1, "00:00:00.000001"}, {999999, "00:00:00.999999"},
        {1000000, "00:00:01"}, {43200123456, "12:00:00.123456"},
        {MicrosPerDay - 1, "23:59:59.999999"}, {MicrosPerDay, "24:00:00"}
    };
    for (const auto& [value, text] : times) {
        check(parseTimeMicros(text) == value, text + " parse");
        check(formatTimeMicros(value) == text, text + " format");
        check(parseTimeMicros(formatTimeMicros(value)) == value, text + " roundtrip");
    }
    const std::vector<std::pair<int64_t, std::string>> timestamps = {
        {0, "2000-01-01 00:00:00"}, {1, "2000-01-01 00:00:00.000001"},
        {-1, "1999-12-31 23:59:59.999999"}, {-1000000, "1999-12-31 23:59:59"},
        {1000001, "2000-01-01 00:00:01.000001"},
        {MicrosPerDay + 1, "2000-01-02 00:00:00.000001"},
        {-MicrosPerDay + 1, "1999-12-31 00:00:00.000001"},
        {-946684800000000, "1970-01-01 00:00:00"},
        {-63082281600000000LL, "0001-01-01 00:00:00"},
        {252455615999999999LL, "9999-12-31 23:59:59.999999"},
        {TIMESTAMP_POSITIVE_INFINITY, "infinity"},
        {TIMESTAMP_NEGATIVE_INFINITY, "-infinity"}
    };
    for (const auto& [value, text] : timestamps) {
        check(parseTimestampMicros(text) == value, text + " parse");
        check(formatTimestampMicros(value) == text, text + " format");
        check(parseTimestampMicros(formatTimestampMicros(value)) == value, text + " roundtrip");
        check(parseTimestampMicros(formatTimestampMicros(value, true)) == value, text + " zoned roundtrip");
    }
    check(parseTimestampMicros("2000-01-01 05:45:00.000001+05:45") == 1, "offset fraction parse");
    check(parseTimestampMicros("1999-12-31 19:00:00-05") == 0, "negative offset epoch parse");
    check(formatTimestampMicrosWithOffset(1, 345) == "2000-01-01 05:45:00.000001+05:45", "offset fraction format");
    check(formatTimestampMicrosWithOffset(0, -300) == "1999-12-31 19:00:00-05", "negative offset epoch format");
    for (const auto& [value, rounded] : std::vector<std::pair<int64_t,int64_t>>{
            {-1500,-2000},{-500,-1000},{-499,0},{0,0},{499,0},{500,1000},{1500,2000}})
        check(roundTimestampMicros(value,3) == rounded, "PG epoch tie " + std::to_string(value));

    for (const auto* text : {"", "25:00:00", "24:00:00.000001", "12:60:00", "12:00:60", "12:00",
                            "12.1:00:00", "12:00:00.1234567", "12:00:00+00", "12:00:00.",
                            "-00:00:01", "12:00:00.000x"})
        check(!parseTimeMicros(text), std::string("invalid canonical time ") + text);
    for (const auto* text : {"", "2000-13-01 00:00:00", "2000-01-32 00:00:00", "0000-01-01 00:00:00",
                            "10000-01-01 00:00:00", "2000-01-01 24:00:00", "2000-01-01 00:00:60",
                            "2000-01-01 00:00:00.1234567", "2000-01-01 00:00:00+00:00.1",
                            "2000-01-01 00:00:00.", "2000-01-01  00:00:00", "2000-01-01"})
        check(!parseTimestampMicros(text), std::string("invalid canonical timestamp ") + text);

    for (int sample = -50; sample < 50; ++sample)
        for (int precision = -1; precision <= 6; ++precision) {
            const int64_t value = sample * 123457LL + (sample < 0 ? -500 : 500);
            const auto rounded = roundTimestampMicros(value, precision);
            int64_t scale = 1;
            if (precision >= 0)
                for (int digit = precision; digit < 6; ++digit) scale *= 10;
            const int64_t error = rounded ? (*rounded > value ? *rounded - value : value - *rounded) : 0;
            check(rounded && *rounded % scale == 0 && error <= scale / 2 &&
                      roundTimestampMicros(*rounded, precision) == rounded,
                  "precision grid/nearest/idempotence");
        }
    check(formatTimeMicros(-1).empty(), "time NULL sentinel is not a value");
    check(formatTimeMicros(MicrosPerDay + 1).empty(), "time range");
    check(formatTimestampMicros(INT64_MIN).empty(), "timestamp NULL sentinel is not -infinity");
    check(formatTimestampMicros(INT64_MAX - 1).empty(), "timestamp unsupported civil range");
    check(!roundTimestampMicros(0,-2), "invalid negative precision");
    check(!roundTimestampMicros(0,7), "invalid excess stored precision");
    check(!roundTimestampMicros(INT64_MAX - 1,0), "round overflow is not infinity");
    check(roundTimeMicros(MicrosPerDay - 1,0) == MicrosPerDay, "time precision may reach 24h");
    check(formatTimestampMicrosWithOffset(0,960).empty(), "invalid output offset");
    check(roundTimestampMicros(TIMESTAMP_NEGATIVE_INFINITY,3) == TIMESTAMP_NEGATIVE_INFINITY,
          "infinity precision unchanged");
    check(!roundTimestampMicros(INT64_MIN,-1), "NULL sentinel cannot bypass default precision");
    check(!roundTimestampMicros(INT64_MIN,6), "NULL sentinel cannot bypass maximum precision");

    std::cout << "TEMPORAL_MICRO_CODEC_COMPLETE " << controls << " FAILURES " << failures << '\n';
    return controls == 916 && failures == 0 ? 0 : 1;
}
