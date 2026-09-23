#include "common/DateType.h"
#include "common/TimeZoneRules.h"

#include <cassert>
#include <iostream>

int main() {
    const int64_t winter =
        parseTimestampToSeconds("2026-01-17 10:00:00+00");
    const int64_t summer =
        parseTimestampToSeconds("2026-08-17 10:00:00+00");
#ifdef HAS_ICU
    assert(dbms::ianaTimezoneOffsetMinutes("America/New_York", winter) == -300);
    assert(dbms::ianaTimezoneOffsetMinutes("America/New_York", summer) == -240);
    assert(dbms::ianaTimezoneOffsetMinutes("Europe/London", winter) == 0);
    assert(dbms::ianaTimezoneOffsetMinutes("Europe/London", summer) == 60);
    assert(dbms::ianaTimezoneOffsetMinutes("Europe/Kyiv", summer) == 180);
    assert(dbms::ianaTimezoneOffsetMinutes("America/New_York", winter, true) == -300);
    assert(dbms::ianaTimezoneOffsetMinutes("America/New_York", summer, true) == -240);
    assert(!dbms::ianaTimezoneOffsetMinutes("Mars/Phobos", summer));
#else
    assert(!dbms::ianaTimezoneOffsetMinutes("America/New_York", summer));
#endif
    std::cout << "[TIMEZONE RULES] named-zone offsets OK\n";
}
