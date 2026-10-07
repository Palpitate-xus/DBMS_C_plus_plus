#pragma once

#include <string>
#include <vector>

namespace dbms {
namespace collation {

// Normalize a collation name (lowercase, strip quotes/extra spaces).
std::string normalizeName(const std::string& name);

// Return true if `name` is a known built-in collation.
bool isValid(const std::string& name);

// Return true for binary ordering (empty/C/POSIX/ucs_basic/C.utf8). C.utf8
// retains Unicode character classification/case folding for SQL patterns.
// The explicit SQL `default` collation follows the database locale.
bool isBinary(const std::string& name);

// Return true when the C++ runtime can construct the named system locale.
// Built-in pseudo-collations such as "nocase" and "reverse" are not system
// locale names and should continue through compare() instead.
bool isLocaleAvailable(const std::string& name);

// Compare two strings under `collation`.
// Returns < 0 if a < b, 0 if equal, > 0 if a > b.
int compare(const std::string& a, const std::string& b, const std::string& collation);

// Compare through a previously validated system locale.  Unlike compare(),
// this does not silently turn an unavailable locale into binary semantics.
int compareLocale(const std::string& a, const std::string& b,
                  const std::string& localeName);

// List known collation names (for pg_collation/catalog introspection).
std::vector<std::string> listBuiltins();

} // namespace collation
} // namespace dbms
