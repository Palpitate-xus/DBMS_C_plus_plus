#pragma once

#include "DateType.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#ifdef HAS_ICU
#include <unicode/timezone.h>
#include <unicode/unistr.h>
#endif

namespace dbms {

// DateType timestamps count seconds from the civil year-one epoch; ICU UDate
// counts milliseconds from 1970-01-01. Keep this conversion in one place.
inline std::optional<int> ianaTimezoneOffsetMinutes(
    const std::string& zone, int64_t timestampSeconds, bool localTime = false) {
#ifdef HAS_ICU
    if (zone.empty() || isInfiniteTimestamp(timestampSeconds))
        return std::nullopt;
    const icu::UnicodeString id = icu::UnicodeString::fromUTF8(zone);
    icu::UnicodeString canonical;
    UBool systemZone = false;
    UErrorCode status = U_ZERO_ERROR;
    icu::TimeZone::getCanonicalID(id, canonical, systemZone, status);
    if (U_FAILURE(status) || !systemZone) return std::nullopt;
    std::unique_ptr<icu::TimeZone> rules(icu::TimeZone::createTimeZone(id));
    if (!rules) return std::nullopt;
    const int64_t unixEpoch = Date(1970, 1, 1).convert() * 86400LL;
    const UDate millis = static_cast<UDate>(timestampSeconds - unixEpoch) *
        1000.0;
    int32_t rawOffset = 0, dstOffset = 0;
    status = U_ZERO_ERROR;
    rules->getOffset(millis, localTime, rawOffset, dstOffset, status);
    if (U_FAILURE(status)) return std::nullopt;
    // The storage/output format currently has minute precision. Historical
    // second-resolution zone offsets remain a separate TYPE-06 limitation.
    return (rawOffset + dstOffset) / 60000;
#else
    (void)zone;
    (void)timestampSeconds;
    (void)localTime;
    return std::nullopt;
#endif
}

} // namespace dbms
