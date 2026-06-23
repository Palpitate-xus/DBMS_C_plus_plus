#pragma once

#include <string>
#include <vector>

namespace dbms {
namespace collation {

// Normalize a collation name (lowercase, strip quotes/extra spaces).
std::string normalizeName(const std::string& name);

// Return true if `name` is a known built-in collation.
bool isValid(const std::string& name);

// Return true if `name` is a binary-equivalent collation (empty/default/C/POSIX/ucs_basic).
bool isBinary(const std::string& name);

// Compare two strings under `collation`.
// Returns < 0 if a < b, 0 if equal, > 0 if a > b.
int compare(const std::string& a, const std::string& b, const std::string& collation);

// List known collation names (for pg_collation/catalog introspection).
std::vector<std::string> listBuiltins();

} // namespace collation
} // namespace dbms
