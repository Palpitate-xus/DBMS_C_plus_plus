// Feature gate implementation (gap DIV-14 / CAT-22).
// See src/common/FeatureGate.h for the contract.
#include "common/FeatureGate.h"

#include <cstdlib>
#include <string>
#include <unordered_set>

namespace dbms {

bool isExtendedCompatMode(const std::string& mode) {
    return mode == kCompatModeExtended;
}

std::string defaultCompatibilityMode() {
    static const std::string cached = [] {
        const char* env = std::getenv("DBMS_COMPATIBILITY_MODE");
        if (env && std::string(env) == kCompatModeExtended) {
            return std::string(kCompatModeExtended);
        }
        return std::string(kCompatModePostgresql18);
    }();
    return cached;
}

bool compatKindHasRuntime(const std::string& kind) {
    (void)kind;
    // A real handler must consume the command before generic compatibility
    // dispatch.  Falling through here proves that this specific operation
    // has no runtime, even if another verb for the same kind does (for
    // example CREATE/DROP PUBLICATION versus ALTER PUBLICATION).
    return false;
}

bool compatKindAlwaysUnsupported(const std::string& kind) {
    static const std::unordered_set<std::string> kNeverKinds = {
        // DIV-08: PostgreSQL 18 itself has no SQL ASSERTION implementation;
        // neither compatibility mode may fake one.
        "assertion",
    };
    return kNeverKinds.count(kind) > 0;
}

std::string featureNotSupportedError(const std::string& command) {
    return "ERROR: feature not supported: " + command +
           " is not implemented (SQLSTATE 0A000)";
}

std::string postgresSyntaxError(const std::string& command) {
    return "ERROR: syntax error: " + command +
           " is not PostgreSQL syntax (SQLSTATE 42601)";
}

std::string unrecognizedConfigurationParameterError(
    const std::string& parameter) {
    return "ERROR: unrecognized configuration parameter \"" + parameter +
           "\" (SQLSTATE 42704)";
}

} // namespace dbms
