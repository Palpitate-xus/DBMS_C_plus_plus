#include "common/GeometryValue.h"

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

// This test can also be linked with GeometryValue.cpp alone: it deliberately
// has no engine, catalog, connection pool, static database, or filesystem I/O.
int main() {
    struct Case {
        const char* type;
        uint32_t oid;
        const char* input;
        const char* expected;  // nullptr means invalid text, not SQL NULL.
    };
    const Case cases[] = {
        {"point",600,"(1,2)","(1,2)"},
        {"point",600," 1 , 2 ","(1,2)"},
        {"point",600,"(1),2",nullptr},
        {"point",600,"1,(2)",nullptr},
        {"point",600,"()1,2",nullptr},
        {"point",600,"1,2()",nullptr},
        {"point",600,"((1,2))",nullptr},
        {"point",600,"(1,2]",nullptr},
        {"line",628,"{1,2,3}","{1,2,3}"},
        {"line",628,"[(1,2),(3,4)]","{1,-1,1}"},
        {"line",628,"1,2,(3,4)","{1,-1,1}"},
        {"line",628,"{1},2,3",nullptr},
        {"line",628,"{1,2},3",nullptr},
        {"line",628,"[(1),2,(3),4]",nullptr},
        {"line",628,"(1,2),3,4",nullptr},
        {"line",628,"{1,2,3}4",nullptr},
        {"lseg",601,"[(1,2),(3,4)]","[(1,2),(3,4)]"},
        {"lseg",601,"[1,2,3,4]","[(1,2),(3,4)]"},
        {"lseg",601,"1,2,(3,4)","[(1,2),(3,4)]"},
        {"lseg",601,"[(1,2),(3,4))","[(1,2),(3,4)]"},
        {"lseg",601,"[(1),2,(3),4]",nullptr},
        {"lseg",601,"(1),2,(3),4",nullptr},
        {"lseg",601,"(1,2),3,4",nullptr},
        {"lseg",601,"1,(2,3),4",nullptr},
        {"box",603,"((1,2),(3,4))","(3,4),(1,2)"},
        {"box",603,"(1,2,3,4)","(3,4),(1,2)"},
        {"box",603,"1,2,(3,4)","(3,4),(1,2)"},
        {"box",603,"(1),2,(3),4",nullptr},
        {"box",603,"(1,2),3,4",nullptr},
        {"box",603,"[1,2,3,4]",nullptr},
        {"box",603,"(1,2),(3,4)()",nullptr},
        {"path",602,"[(1,2),(3,4)]","[(1,2),(3,4)]"},
        {"path",602,"(1,2,3,4)","((1,2),(3,4))"},
        {"path",602,"1,2,(3,4)","((1,2),(3,4))"},
        {"path",602,"(1,2)","((1,2))"},
        {"path",602,"[(1,2),(3,4))","[(1,2),(3,4)]"},
        {"path",602,"[(1),2,(3),4]",nullptr},
        {"path",602,"(1,2),3,4",nullptr},
        {"path",602,"[(1,2)),(3,4)]",nullptr},
        {"path",602,"[(1,2),]",nullptr},
        {"polygon",604,"((1,2),(3,4))","((1,2),(3,4))"},
        {"polygon",604,"1,2,(3,4)","((1,2),(3,4))"},
        {"polygon",604,"(1,2)","((1,2))"},
        {"polygon",604,"(1),2,(3),4",nullptr},
        {"polygon",604,"(1,2),3,4",nullptr},
        {"polygon",604,"[(1,2),(3,4)]",nullptr},
        {"polygon",604,"((1,2),(3,4))()",nullptr},
        {"circle",718,"<(1,2),3>","<(1,2),3>"},
        {"circle",718,"((1,2),3)","<(1,2),3>"},
        {"circle",718,"(1,2),3","<(1,2),3>"},
        {"circle",718,"1,2,3","<(1,2),3>"},
        {"circle",718,"<(1,2),3)","<(1,2),3>"},
        {"circle",718,"((1,2),3>","<(1,2),3>"},
        {"circle",718,"<(1),2,3>",nullptr},
        {"circle",718,"<1,2>,3",nullptr},
        {"circle",718,"1,(2,3)",nullptr},
        {"circle",718,"(1,2,3)",nullptr},
        {"circle",718,"<(1,2),3>()",nullptr},
    };
    size_t checks = 0, failures = 0;
    const auto check = [&](bool ok, const Case& item, const char* label) {
        ++checks;
        if (!ok) {
            ++failures;
            std::cerr << "GEOMETRIC_DELIMITER_FAILURE " << item.type << ' '
                      << item.input << ' ' << label << '\n';
        }
    };
    for (const auto& item : cases) {
        const bool valid = item.expected != nullptr;
        std::string normalized = "unchanged";
        check(dbms::normalizeGeometryText(item.input,item.type,normalized) == valid,
              item,"text acceptance");
        check(normalized == (valid ? item.expected : "unchanged"),item,"text output");
        std::vector<uint8_t> encoded{0x12,0x34};
        check(dbms::encodeGeometryBinary(item.input,item.oid,encoded) == valid,
              item,"binary input acceptance");
        if (valid) {
            std::string decoded;
            check(dbms::decodeGeometryBinary(item.oid,encoded,decoded) &&
                  decoded == item.expected,item,"binary roundtrip");
        } else {
            check(encoded == std::vector<uint8_t>({0x12,0x34}),item,"failed encode unchanged");
        }
    }
    std::cout << "GEOMETRIC_DELIMITER_COMPLETE " << checks << " FAILURES " << failures << '\n';
    return failures == 0 ? 0 : 1;
}
