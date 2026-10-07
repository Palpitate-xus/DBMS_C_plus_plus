#include "expression/SqlPattern.h"
#include <cassert>
#include <iostream>
#include <tuple>

int main() {
    using namespace dbms::sql_pattern;
    for (const auto& test : std::vector<std::tuple<std::string,std::string,bool>>{
        {"é","_",true},{"中","_",true},{"éé","_{2}",true},
        {"abc","a.c",false},{"a.c","a.c",true},{"^a$","^a$",true},
        {"a\nb","a_b",true},{"a\nb","a%b",true},{"é","[éê]",true},
        {"ê","[é-ê]",true},{"a","[^é]",true},{"中","[[:alpha:]]",true},
        {"٣","\\d",false},{"éé","(é|ê){2}",true},
        {"abc","(ab|a)c",true},{"","(a|)",true},{"aaaa","a{2,4}",true},
        {"aaaaa","a{2,4}",false},{"a","a\\",true},{"{}","{}",true},
        {"a","\\ma\\M",true},{"1","\\d",true},{"a","\\w",true}}) {
        const auto& [text,pattern,expected] = test;
        const bool actual = similar(text,pattern);
        if (actual != expected) std::cerr << "SIMILAR mismatch: " << text << " / " << pattern << '\n';
        assert(actual == expected);
    }
    assert(similar("a_","aé_","é"));
    assert(similar("a%","aé%","é"));
    assert(similar("a\\b","a\\b",""));
    assert(similar("aa","#\"a#\"#1","#"));
    assert(similar("abb","a#\"b#\"#1","#"));
    assert(!similar("ab","#\"a|b#\"#1","#"));
    assert(similar("éé","#\"é#\"#1","#"));
    assert(!similar("é","[[:alpha:]]","\\",true));
    assert(similar("a_","[a[b]_"));
    assert(!similar("aa","[a[b]_"));
    assert(similar("a%","[a[b]%"));
    assert(!similar("a","[a[b]%"));
    assert(similar("ab","[a[b]."));
    assert(similar("a","[a[b]$"));
    assert(similar("a\"","[a[b]#\"","#"));
    assert(similar("a","[[:<:]]a[[:>:]]"));
    assert(similar("中","[[:<:]]中[[:>:]]"));
    bool separatorsRejected = false;
    try { (void)similar("a","#\"#\"#\"","#"); }
    catch (const dbms::DbError& error) { separatorsRejected = error.sqlState() == "2200C"; }
    assert(separatorsRejected);
    for (const std::string pattern : {"[","(","[z-a]","[a-b-c]","a{3,2}","a{256}","a{1x}","a++","\\q","(a)\\1","\\m*","\\A?"}) {
        bool rejected = false;
        try { (void)similar("a",pattern); }
        catch (const dbms::DbError& error) { rejected = error.sqlState() == "2201B"; }
        if (!rejected) std::cerr << "invalid regex accepted: " << pattern << '\n';
        assert(rejected);
    }
    for (const auto& test : std::vector<std::tuple<std::string,std::string,bool>>{
        {"a","a\\",false},{"b","a\\",false},{"","%\\",false},
        {"a","_\\",false},{"é","_",true},{"é","__",false}}) {
        const auto& [text,pattern,expected] = test;
        assert(like(text,pattern) == expected);
    }
    for (const auto& test : std::vector<std::pair<std::string,std::string>>{
        {"ab","a\\"},{"a","%\\"},{"a","%_\\"}}) {
        bool rejected = false;
        try { (void)like(test.first,test.second); }
        catch (const dbms::DbError& error) { rejected = error.sqlState() == "22025"; }
        assert(rejected);
    }
    assert(like("É","é","\\",true));
    assert(like("ẞ","ß","\\",true));
    assert(like("İ","i","\\",true));
    assert(!like("ΟΣ","ος","\\",true));
    assert(!like("ß","ss","\\",true));
    assert(!like("É","é","\\",true,false,true));
    assert(!like("é","_","\\",false,true));
    assert(like("é","__","\\",false,true));
    std::cout << "[SQL PATTERN UNICODE] passed\n";
}
