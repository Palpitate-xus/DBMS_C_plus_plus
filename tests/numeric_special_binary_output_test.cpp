// Exact output bytes are frozen from PostgreSQL 18.6 numeric_send (control 83).
#include "network/PostgresNumeric.h"
#include "types/numeric.h"
#include <cstdint>
#include <functional>
#include <iostream>
#include <string>
#include <vector>

namespace {
struct Case {const char* text;const char* hex;};
const Case cases[]={
    {"1.00","00010000000000020001"},
    {"-0.0020","0001ffff400000040014"},
    {"NaN","00000000c0000000"},
    {"Infinity","00000000d0000020"},
    {"-Infinity","00000000f0000020"}
};
std::vector<uint8_t> bytes(const std::string& hex) {
    std::vector<uint8_t> out;
    for(size_t i=0;i<hex.size();i+=2)out.push_back(static_cast<uint8_t>(std::stoul(hex.substr(i,2),nullptr,16)));
    return out;
}
}
int main() {
    size_t checked=0,failed=0;
    const auto check=[&](const std::string& label,const std::function<bool()>& action) {
        ++checked;bool good=false;
        try{good=action();}catch(const std::exception& error){std::cerr<<error.what()<<'\n';}
        failed+=!good;std::cout<<"NUMERIC_SPECIAL_BINARY "<<checked<<' '<<label<<" good="<<good<<'\n';
    };
    for(const auto& item:cases)check(std::string("exact PG18.6 numeric_send ")+item.text,[&] {
        std::vector<uint8_t> actual;std::string text;
        return dbms::encodePostgresNumeric(item.text,actual) && actual==bytes(item.hex) &&
            dbms::decodePostgresNumeric(actual,text) && text==item.text;
    }); //1..5
    check("reused output vector never retains previous digit/header bytes",[&] {
        std::vector<uint8_t> actual(8192,0xff);
        for(const auto& item:cases)
            if(!dbms::encodePostgresNumeric(item.text,actual) || actual!=bytes(item.hex))return false;
        return true;
    }); //6
    check("ordinary Numeric values and scales unchanged by sender",[&] {
        for(const auto& item:cases)if(dbms::Numeric(item.text).toString()!=item.text)return false;
        return true;
    }); //7
    check("existing normalized special input still uses exact output bytes",[&] {
        for(const auto& item:std::vector<Case>{{"+INFINITY","00000000d0000020"},
                                              {"-INFINITY","00000000f0000020"},
                                              {"nan","00000000c0000000"}}) {
            std::vector<uint8_t> actual;
            if(!dbms::encodePostgresNumeric(item.text,actual) || actual!=bytes(item.hex))return false;
        }
        return true;
    }); //8
    for(bool negative:{false,true})check("original special binary receive accepts scale0 and32 sign="+std::to_string(negative),[&] {
        for(const auto* scale:{"0000","0020"}) {
            const auto raw=bytes(std::string("00000000")+(negative?"f000":"d000")+scale);
            std::string text;
            if(!dbms::decodePostgresNumeric(raw,text) || text!=(negative?"-Infinity":"Infinity"))return false;
        }
        return true;
    }); //9..10 (receive policy retained, not widened)
    if(checked!=10)++failed;
    std::cout<<"NUMERIC_SPECIAL_BINARY_COMPLETE "<<checked<<" FAILURES "<<failed<<'\n';return failed?1:0;
}
