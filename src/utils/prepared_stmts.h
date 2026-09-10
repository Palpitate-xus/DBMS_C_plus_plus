#pragma once

#include <string>
#include <vector>

namespace dbms {

std::string ps_trim(const std::string& s);

// Top-level " AS " finder for PREPARE (quote/paren/dollar-quote aware).
size_t ps_findPrepareAs(const std::string& rest);

// "name" or "name(type, ...)" head parser.
bool ps_parsePrepareHead(const std::string& head, std::string& name,
                         std::vector<std::string>& paramTypes);

// Find the highest $n parameter reference outside SQL literals, quoted
// identifiers and comments.  PostgreSQL permits gaps (for example $2 with an
// unused $1), so count is the highest referenced parameter number.
bool ps_analyzeDollarParams(const std::string& sql, size_t& count,
                            std::string& error);

// Top-level comma split for EXECUTE argument lists.
std::vector<std::string> ps_splitExecuteArgs(const std::string& in);

// $n substitution outside literals/comments; false + error on an invalid or
// out-of-range parameter reference.
bool ps_substituteDollarParams(const std::string& in,
                               const std::vector<std::string>& values,
                               std::string& out, std::string& error);

} // namespace dbms
