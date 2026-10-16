#include "common/NetworkValue.h"

#include <array>
#include <iostream>
#include <string>

int main() {
    struct Case { size_t width; const char* input; const char* expected; };
    const Case cases[] = {
        {6,"08:00:2b:01:02:03","08:00:2b:01:02:03"},
        {6,"08-00-2b-01-02-03","08:00:2b:01:02:03"},
        {6,"08002b:010203","08:00:2b:01:02:03"},
        {6,"08002b-010203","08:00:2b:01:02:03"},
        {6,"0800.2b01.0203","08:00:2b:01:02:03"},
        {6,"0800-2b01-0203","08:00:2b:01:02:03"},
        {6,"08002b010203","08:00:2b:01:02:03"},
        {6,"8:0:2b:1:2:3","08:00:2b:01:02:03"},
        {6," 08:00:2b:01:02:03 \t","08:00:2b:01:02:03"},
        {6,"08:00:2b: 01:02:03","08:00:2b:01:02:03"},
        {6,"+8:0:2b:1:2:3","08:00:2b:01:02:03"},
        {6,"0x08:0:2b:1:2:3","08:00:2b:01:02:03"},
        {6,"08:00-2b:01:02:03",nullptr},
        {6,"::08002b010203",nullptr},
        {6,"0:8002b010203",nullptr},
        {6,"08:00:2b:01:02:03:",nullptr},
        {6,"08::00:2b:01:02:03",nullptr},
        {6,"100:00:2b:01:02:03",nullptr},
        {6,"-1:00:2b:01:02:03",nullptr},
        {6,"08:00:2b:01:02",nullptr},
        {6,"08:00:2b:01:02:03:04",nullptr},
        {6,"08002b010203Z",nullptr},
        {8,"08:00:2b:01:02:03:04:05","08:00:2b:01:02:03:04:05"},
        {8,"08-00-2b-01-02-03-04-05","08:00:2b:01:02:03:04:05"},
        {8,"08002b:0102030405","08:00:2b:01:02:03:04:05"},
        {8,"0800.2b01.0203.0405","08:00:2b:01:02:03:04:05"},
        {8,"08002b01:02030405","08:00:2b:01:02:03:04:05"},
        {8,"08002b0102030405","08:00:2b:01:02:03:04:05"},
        {8," 08:00:2b:01:02:03:04:05 \t","08:00:2b:01:02:03:04:05"},
        {8,"08:00:2b:01:02:03","08:00:2b:ff:fe:01:02:03"},
        {8,"08-00-2b-01-02-03","08:00:2b:ff:fe:01:02:03"},
        {8,"08002b:010203","08:00:2b:ff:fe:01:02:03"},
        {8,"0800.2b01.0203","08:00:2b:ff:fe:01:02:03"},
        {8,"08002b010203","08:00:2b:ff:fe:01:02:03"},
        {8,"08:00:2b:01:02:03:","08:00:2b:ff:fe:01:02:03"},
        {8,"08002b010203Z","08:00:2b:ff:fe:01:02:03"},
        {8,"08002b0102030405Z","08:00:2b:01:02:03:04:05"},
        {8,"08:00-2b:01:02:03:04:05",nullptr},
        {8,"::08002b0102030405",nullptr},
        {8,"0:8002b0102030405",nullptr},
        {8,"08::00:2b:01:02:03:04:05",nullptr},
        {8,"08:00:2b: 01:02:03:04:05",nullptr},
        {8,"8:0:2b:1:2:3",nullptr},
        {8,"08:00:2b:01:02:03:04",nullptr},
        {8,"08:00:2b:01:02:03:04:05:06",nullptr},
    };
    size_t checks = 0, failures = 0;
    const auto check = [&](bool ok, const std::string& label) {
        ++checks;
        if (!ok) { ++failures; std::cerr << "MAC_INPUT_FAILURE " << label << '\n'; }
    };
    for (const auto& item : cases) {
        std::array<uint8_t,8> bytes{};
        const bool accepted = dbms::parseMacAddress(item.input,item.width,bytes);
        check(accepted == (item.expected != nullptr),std::string(item.input) + " acceptance");
        if (item.expected)
            check(accepted && dbms::formatMacAddress(bytes.data(),item.width) == item.expected,
                  std::string(item.input) + " canonical bytes");
    }
    std::array<uint8_t,8> bytes{};
    check(!dbms::parseMacAddress("08002b010203",7,bytes),"invalid API width");
    check(!dbms::parseMacAddress(std::string("08002b010203\0x",14),6,bytes),"NUL is not a byte separator");
    check(dbms::parseMacAddress(std::string(1000,'0') + "8:0:2b:1:2:3",6,bytes) &&
          dbms::formatMacAddress(bytes.data(),6) == "08:00:2b:01:02:03", "long leading zero octet");
    std::cout << "MAC_INPUT_COMPLETE " << checks << " FAILURES " << failures << '\n';
    return failures == 0 ? 0 : 1;
}
