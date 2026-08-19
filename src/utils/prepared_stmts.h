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

// Top-level comma split for EXECUTE argument lists.
std::vector<std::string> ps_splitExecuteArgs(const std::string& in);

// $n substitution; false + error on out-of-range parameter.
bool ps_substituteDollarParams(const std::string& in,
                               const std::vector<std::string>& values,
                               std::string& out, std::string& error);

} // namespace dbms
