// Feature gate for PostgreSQL 18 compatibility (gap DIV-14 / CAT-22).
//
// Commands that previously only stored a record in .pg_compat_objects (or
// another sidecar) while reporting success must instead fail with
// SQLSTATE 0A000 (feature_not_supported) until a real runtime exists.
// This module is the single place that decides, per object kind, whether a
// runtime implementation exists.  The default compatibility mode is
// "postgresql18".  The explicit "extended" mode enables project-native SQL
// extensions, but never turns an unimplemented PostgreSQL object into a
// successful compatibility-record write.
#pragma once

#include <string>

namespace dbms {

// Canonical compatibility modes.
inline constexpr const char* kCompatModePostgresql18 = "postgresql18";
inline constexpr const char* kCompatModeExtended = "extended";
inline constexpr const char* kCompatKindAssertion = "assertion";

// Returns true when mode names the extended compatibility mode.
bool isExtendedCompatMode(const std::string& mode);

// Session-start default compatibility mode.  Reads the
// DBMS_COMPATIBILITY_MODE environment variable once (values:
// postgresql18 | extended); invalid or unset falls back to postgresql18.
// This is the documented opt-in for project tooling and E2E tests that
// rely on extended-mode commands.
std::string defaultCompatibilityMode();

// Kinds that must stay feature_not_supported in every compatibility mode
// because PostgreSQL 18 itself does not implement them (DIV-08: SQL
// assertions) or because no honest runtime can exist yet.
bool compatKindAlwaysUnsupported(const std::string& kind);

// Builds the standard feature-not-supported error line for a command.
// Example: featureNotSupportedError("CREATE EXTENSION") produces
//   "ERROR: feature not supported: CREATE EXTENSION is not implemented (SQLSTATE 0A000)"
std::string featureNotSupportedError(const std::string& command);

// Reject project-only SQL in postgresql18 mode using the SQLSTATE that the
// PostgreSQL parser would expose, while retaining an actionable hint.
std::string postgresSyntaxError(const std::string& command);

// SHOW <project-name> is parsed by PostgreSQL as a GUC lookup, so an unknown
// single-token name is an undefined object rather than unsupported syntax.
std::string unrecognizedConfigurationParameterError(
    const std::string& parameter);

} // namespace dbms
