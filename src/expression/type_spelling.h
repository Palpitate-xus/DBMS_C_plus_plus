#pragma once
#include <cctype>
#include <string>

namespace dbms::type_spelling_detail {
// Pure spelling normalization, not catalog resolution. Quoted identifiers
// own their bytes; only an unquoted '(' starts a modifier list. Callers keep
// ownership of array decoration and the actual declared type lookup.
inline std::string scalarIdentity(const std::string& input) {
    std::string result;
    bool quoted=false;
    for(size_t i=0;i<input.size();++i) {
        const char c=input[i];
        if(c=='"') {
            result+=c;
            if(quoted && i+1<input.size() && input[i+1]=='"') {
                result+=input[++i];
            } else quoted=!quoted;
        } else if(!quoted && c=='(') {
            break;
        } else result+=quoted?c:static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    const auto first=result.find_first_not_of(" \t\r\n");
    if(first==std::string::npos)return {};
    return result.substr(first,result.find_last_not_of(" \t\r\n")-first+1);
}
} // namespace dbms::type_spelling_detail
