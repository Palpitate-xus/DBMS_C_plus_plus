#pragma once
#include <string>

namespace dbms {
// Calendar fields are checked as int32, time as signed int64 microseconds.
// These wider carriers allow checked arithmetic before narrowing.
struct IntervalInputParts {
    long long months = 0;
    long long days = 0;
    long long micros = 0;
    bool ok = false;
    bool outOfRange = false;
    bool combinedOutOfRange = false;
    bool numericFieldTooLong = false;
};

// Pure datum parser: never evaluates SQL, touches storage, or changes sessions.
IntervalInputParts parseIntervalInput(const std::string& value);
std::string formatIntervalInput(long long months, long long days, long long micros,
                                bool trimFractionZeros = false);
// Returns the structured input error, or an empty string for a valid datum.
std::string intervalInputSqlState(const IntervalInputParts& parsed);
}

