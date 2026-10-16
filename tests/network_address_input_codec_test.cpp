#include "common/NetworkValue.h"

#include <iostream>
#include <string>

int main() {
    struct Case { const char* input; const char* inet; const char* cidr; };
    const Case cases[] = {
        {"0",nullptr,"0.0.0.0/8"},
        {"1",nullptr,"1.0.0.0/8"},
        {"10",nullptr,"10.0.0.0/8"},
        {"127",nullptr,"127.0.0.0/8"},
        {"128",nullptr,"128.0.0.0/16"},
        {"223",nullptr,"223.0.0.0/24"},
        {"224",nullptr,"224.0.0.0/4"},
        {"225",nullptr,"225.0.0.0/8"},
        {"239",nullptr,"239.0.0.0/8"},
        {"240",nullptr,"240.0.0.0/32"},
        {"255",nullptr,"255.0.0.0/32"},
        {"10/8","10.0.0.0/8","10.0.0.0/8"},
        {"10.1",nullptr,"10.1.0.0/16"},
        {"10.1/16","10.1.0.0/16","10.1.0.0/16"},
        {"10.1.2",nullptr,"10.1.2.0/24"},
        {"10.1.2/24","10.1.2.0/24","10.1.2.0/24"},
        {"10.1.2.3","10.1.2.3","10.1.2.3/32"},
        {"10.1.2.3/24","10.1.2.3/24",nullptr},
        {"10/9","10.0.0.0/9","10.0.0.0/9"},
        {"10.1/8","10.1.0.0/8",nullptr},
        {"10.1/17","10.1.0.0/17","10.1.0.0/17"},
        {"10.00.1.0/24","10.0.1.0/24","10.0.1.0/24"},
        {"010.1.2.3","10.1.2.3","10.1.2.3/32"},
        {"10.1.2.3/032","10.1.2.3","10.1.2.3/32"},
        {"192.168.1",nullptr,"192.168.1.0/24"},
        {"192.168.1/24","192.168.1.0/24","192.168.1.0/24"},
        {"192.168.1.0/255.255.255.0",nullptr,nullptr},
        {"0/0","0.0.0.0/0","0.0.0.0/0"},
        {"::1","::1","::1/128"},
        {"::1/64","::1/64",nullptr},
        {"::ffff:192.168.1.1/128","::ffff:192.168.1.1","::ffff:192.168.1.1/128"},
        {"2001:db8::/32","2001:db8::/32","2001:db8::/32"},
        {"1.2.3.4/33",nullptr,nullptr},
        {"1.2.3.4/0","1.2.3.4/0",nullptr},
        {"256.1.2.3",nullptr,nullptr},
        {"10.1.2.3 ",nullptr,nullptr},
        {"10.1.2.3\t",nullptr,nullptr},
        {"10..1",nullptr,nullptr},
        {"10/",nullptr,nullptr},
        {"",nullptr,nullptr},
        {"::ffff:010.1.2.3",nullptr,nullptr},
        {"10/15","10.0.0.0/15","10.0.0.0/15"},
        {"10/16",nullptr,"10.0.0.0/16"},
        {"10.1/23","10.1.0.0/23","10.1.0.0/23"},
        {"10.1/24",nullptr,"10.1.0.0/24"},
        {"192.168",nullptr,"192.168.0.0/24"},
        {"224.1",nullptr,"224.1.0.0/16"},
        {"240.1",nullptr,"240.1.0.0/32"},
        {"255.255.255.255","255.255.255.255","255.255.255.255/32"},
        {"0xa",nullptr,"160.0.0.0/16"},
        {"0x0a",nullptr,"10.0.0.0/8"},
        {"0xC0A8",nullptr,"192.168.0.0/24"},
        {"0xC0A801",nullptr,"192.168.1.0/24"},
        {"0xC0A80100/24",nullptr,"192.168.1.0/24"},
        {"0xc0000200/24",nullptr,"192.0.2.0/24"},
        {"0xc0000201/24",nullptr,nullptr},
        {"0x0",nullptr,"0.0.0.0/8"},
        {"0Xc00002/24",nullptr,"192.0.2.0/24"},
        {"0x",nullptr,nullptr},
        {"0x12g",nullptr,nullptr},
        {"0x123456789",nullptr,nullptr},
        {"1.2.3.4.5",nullptr,nullptr},
        {"1.2.3.",nullptr,nullptr},
        {"1.2.3.4/00032","1.2.3.4","1.2.3.4/32"},
        {"1.2.3.4/+24",nullptr,nullptr},
        {"1.2.3.4/-1",nullptr,nullptr},
        {"::/00",nullptr,nullptr},
        {"::1/0128",nullptr,nullptr},
        {"::/0","::/0","::/0"},
        {"::1/001",nullptr,nullptr},
        {"::/129",nullptr,nullptr},
        {"::192.0.2.1","::192.0.2.1","::192.0.2.1/128"},
        {"0:0:0:0:0:192.0.2.1",nullptr,nullptr},
        {"2001:0db8::/032",nullptr,nullptr},
        {"::ffff:192.168.1.1/0128",nullptr,nullptr},
        {"10.",nullptr,nullptr},
        {"10./8","10.0.0.0/8",nullptr},
        {"10./9","10.0.0.0/9",nullptr},
        {"10./16",nullptr,nullptr},
        {"10.1.",nullptr,nullptr},
        {"10.1./16","10.1.0.0/16",nullptr},
        {"10.1.2./24","10.1.2.0/24",nullptr},
        {"10.1.2.3.","10.1.2.3",nullptr},
        {"10.1.2.3./24","10.1.2.3/24",nullptr},
        {"10.1.2.3..",nullptr,nullptr},
        {"10..1/8",nullptr,nullptr},
        {"10.1.2.3./32","10.1.2.3",nullptr},
        {"10.1.2.3./33",nullptr,nullptr},
        {"0xc0000200.",nullptr,nullptr},
        {"0xc0000200./24",nullptr,nullptr},
    };
    size_t checks=0,failures=0;
    const auto check=[&](bool ok,const std::string& label) {
        ++checks;
        if(!ok){++failures;std::cerr << "INET_INPUT_FAILURE " << label << '\n';}
    };
    for(const auto& item:cases) {
        for(bool cidr:{false,true}) {
            const char* expected=cidr?item.cidr:item.inet;
            dbms::NetworkAddressValue address;
            const bool accepted=dbms::parseNetworkAddress(item.input,address,cidr);
            check(accepted==(expected!=nullptr) &&
                  (!expected || (address.toString(cidr)==expected &&
                   address.family==(std::string(item.input).find(':')==std::string::npos
                       ?dbms::kPgNetworkFamilyInet:dbms::kPgNetworkFamilyInet6))),
                  std::string(cidr?"cidr ":"inet ")+item.input);
        }
    }
    for(bool cidr:{false,true}) {
        for(const auto& prefix:{std::string("192.0.2.0"),std::string("::"),std::string("::ffff:192.0.2.0")}) {
            dbms::NetworkAddressValue address;
            check(!dbms::parseNetworkAddress(prefix+char(0)+"suffix",address,cidr),
                  std::string(cidr?"cidr":"inet")+" embedded NUL");
        }
    }
    dbms::NetworkAddressValue address;
    check(!dbms::parseNetworkAddress(std::string(1000,'9')+".0.0.0",address),"bounded octet accumulation");
    check(!dbms::parseNetworkAddress("10.0.0.0/"+std::string(1000,'9'),address),"bounded prefix accumulation");
    std::cout << "INET_INPUT_COMPLETE " << checks << " FAILURES " << failures << '\n';
    return failures?1:0;
}
