// Feature gate implementation (gap DIV-14 / CAT-22).
// See src/common/FeatureGate.h for the contract.
#include "common/FeatureGate.h"

#include <cstdlib>
#include <string>

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

bool compatKindAlwaysUnsupported(const std::string& kind) {
    // DIV-08: PostgreSQL 18 itself has no SQL ASSERTION implementation;
    // neither compatibility mode may fake one.  Keep this as a named
    // contract even if other generic compatibility objects gain runtimes.
    return kind == kCompatKindAssertion;
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
