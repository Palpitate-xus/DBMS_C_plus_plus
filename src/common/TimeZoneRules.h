#pragma once

#include "DateType.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#ifdef HAS_ICU
#include <unicode/strenum.h>
#include <unicode/timezone.h>
#include <unicode/unistr.h>
#endif

namespace dbms {

// PostgreSQL accepts timezone IDs case-insensitively but preserves the
// database's spelling (including aliases such as US/Eastern) in SHOW.
inline std::optional<std::string> resolveIanaTimezoneName(
    const std::string& zone) {
#ifdef HAS_ICU
    if (zone.empty()) return std::nullopt;
    const icu::UnicodeString id = icu::UnicodeString::fromUTF8(zone);
    icu::UnicodeString canonical;
    UBool systemZone = false;
    UErrorCode status = U_ZERO_ERROR;
    icu::TimeZone::getCanonicalID(id, canonical, systemZone, status);
    if (U_SUCCESS(status) && systemZone) return zone;

    status = U_ZERO_ERROR;
    std::unique_ptr<icu::StringEnumeration> names(
        icu::TimeZone::createEnumeration(status));
    if (U_FAILURE(status) || !names) return std::nullopt;
    while (const icu::UnicodeString* candidate = names->snext(status)) {
        if (id.caseCompare(*candidate, 0) != 0) continue;
        canonical.remove();
        systemZone = false;
        UErrorCode candidateStatus = U_ZERO_ERROR;
        icu::TimeZone::getCanonicalID(
            *candidate, canonical, systemZone, candidateStatus);
        if (U_SUCCESS(candidateStatus) && systemZone) {
            std::string spelling;
            candidate->toUTF8String(spelling);
            return spelling;
        }
    }
    return std::nullopt;
#else
    (void)zone;
    return std::nullopt;
#endif
}

// DateType timestamps count seconds from the civil year-one epoch; ICU UDate
// counts milliseconds from 1970-01-01. Keep this conversion in one place.
inline std::optional<int> ianaTimezoneOffsetMinutes(
    const std::string& zone, int64_t timestampSeconds, bool localTime = false) {
#ifdef HAS_ICU
    if (zone.empty() || isInfiniteTimestamp(timestampSeconds))
        return std::nullopt;
    // A session repeats the same zone for many rows. Cache one ICU rule object
    // per thread instead of enumerating IDs and allocating it for every cell.
    thread_local std::string cachedZone;
    thread_local std::unique_ptr<icu::TimeZone> cachedRules;
    if (cachedZone != zone) {
        cachedZone = zone;
        cachedRules.reset();
        if (const auto resolved = resolveIanaTimezoneName(zone)) {
            cachedRules.reset(icu::TimeZone::createTimeZone(
                icu::UnicodeString::fromUTF8(*resolved)));
        }
    }
    if (!cachedRules) return std::nullopt;
    const int64_t unixEpoch = Date(1970, 1, 1).convert() * 86400LL;
    const UDate millis = static_cast<UDate>(timestampSeconds - unixEpoch) *
        1000.0;
    int32_t rawOffset = 0, dstOffset = 0;
    UErrorCode status = U_ZERO_ERROR;
    cachedRules->getOffset(millis, localTime, rawOffset, dstOffset, status);
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
