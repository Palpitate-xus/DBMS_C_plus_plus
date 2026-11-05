#include "executor/LikePrefix.h"
#include <iostream>

int main() {
    using dbms::planner_pattern::likePrefix;
    using dbms::planner_pattern::prefixCeiling;
    size_t checked = 0, failed = 0;
    const auto require = [&](bool pass, const char* role) {
        ++checked;
        if (!pass) { ++failed; std::cerr << "LIKE_PREFIX_FAILED " << role << '\n'; }
    };
    struct Case { std::string pattern, escape; std::optional<std::string> prefix; };
    const Case cases[] = {
        {"abc%", "\\", "abc"}, {"abc%tail", "\\", "abc"},
        {"a_c%", "\\", "a"}, {"界_尾%", "\\", "界"},
        {"a\\_%", "\\", "a_"}, {"a!%%", "!", "a%"},
        {"a界_%", "界", "a_"}, {"a\\_%", "", "a\\"},
        {"abc", "\\", "abc"}, {"", "\\", {}}, {"%", "\\", {}},
        {"_x", "\\", {}}, {"a%\\", "\\", {}}, {"a\\", "\\", {}},
        {"abc%", "xy", {}}, {std::string("a\xff", 2), "\\", {}},
        {"abc%", std::string("\xff", 1), {}}, {"a\n'b%", "\\", "a\n'b"}
    };
    for (const auto& c : cases) require(likePrefix(c.pattern,c.escape)==c.prefix, "fixed prefix / malformed demand");
    require(prefixCeiling("abc")==std::optional<std::string>("abd"), "byte ceiling");
    require(!prefixCeiling(std::string("a\xff",2)), "invalid text ceiling declined");
    require(!prefixCeiling(std::string(2,char(255))) && !prefixCeiling(""), "no finite ceiling");
    for(const auto& pair:std::vector<std::pair<std::string,std::string>>{
        {"ÿ","Ā"},{"߿","ࠀ"},{"퟿",""},{"😀","😁"},{"a􏿿","b"}}) {
        require(prefixCeiling(pair.first)==std::optional<std::string>(pair.second),
                "ceiling is a valid UTF8 text bound across codepoint boundaries");
    }
    require(!prefixCeiling("􏿿"),"maximum Unicode prefix has no finite text ceiling");
    std::vector<std::string> values{""};
    const std::vector<std::string> alphabet{"a","b","界","%","_","!","\\","\n"};
    for (size_t width=1; width<=3; ++width) {
        const auto previous=values;
        for (const auto& prefix:previous) {
            // Generate each exact length once.
            if(dbms::sql_pattern::characters(prefix).size()!=width-1) continue;
            for(const auto& character:alphabet)values.push_back(prefix+character);
        }
    }
    const std::vector<std::string> patterns{
        "a%","a_","a%b","a_界%","界%","界_b","a!_%","a!%%","a\\_%",
        "a界_%","%a","_%","a","a\n%","a%\\","a\\","a!!%","a界界%"};
    for(const auto& escape:std::vector<std::string>{"\\","!","界",""})
        for(const auto& pattern:patterns) {
            const auto prefix=likePrefix(pattern,escape);
            if(!prefix)continue;
            const auto upper=prefixCeiling(*prefix);
            for(const auto& value:values) {
                const bool match=dbms::sql_pattern::like(value,pattern,escape);
                require(!match || value.compare(0,prefix->size(),*prefix)==0,"every true LIKE satisfies derived prefix");
                require(!match || !upper || value<*upper,"every true LIKE below exclusive ceiling");
            }
        }
    std::cout<<"LIKE_PREFIX_CHECKED="<<checked<<" FAILED="<<failed<<'\n';
    return failed?1:0;
}
