#include "common/WorkMemSetting.h"

#include <cassert>
#include <iostream>

int main() {
    using namespace dbms;
    size_t checked = 0, failures = 0;
    const auto check = [&](const char* label, bool good) {
        ++checked; failures += !good;
        if (!good) std::cout << "WORK_MEM_SETTING failed=" << label << '\n';
    };
    // Direct PostgreSQL 18.6 SET/SHOW observations. Decimal ties use rint;
    // GB/TB fractions first round to the next smaller unit, not directly kB.
    const struct { const char* input; size_t kb; } accepted[] = {
        {"64",64}, {"63.5",64}, {"64.5",64}, {"65.5",66},
        {"127.5",128}, {"128.5",128}, {"64.4999",64}, {"64.5001",65},
        {"+64",64}, {"64.0",64}, {"6.4e1",64}, {"64e+0",64},
        {"0x40",64}, {"0100",64}, {"0101",65}, {"0b1000000",64},
        {"64kB",64}, {"64 kB",64}, {"  64 kB  ",64}, {"64\tkB",64},
        {"64MB",65536}, {"0.0625MB",64}, {"0.5MB",512},
        {"0.001GB",1024}, {"0.001TB",1048576},
        {"1.00048828125MB",1024}, {"1.00146484375MB",1026},
        {"1.000000476837158203125GB",1048576},
        {"65536B",64}, {"65024B",64}, {"66048B",64}, {"66560B",65},
        {"64.00048828125kB",64}, {"64.00146484375kB",64},
        {"64.5 kB",64}, {"65.5 kB",66}, {"64 MB\t",65536},
        {"1e9B",976562}, {"2147483647",2147483647},
        {"2147483647.4",2147483647}, {"2097151MB",2147482624},
        {"1TB",1073741824}, {"0x40.0",64}, {"0100.0",100},
        {"00064.0",64}, {"0x40kB",64}
    };
    for (const auto& value : accepted) check(value.input, parseWorkMemKb(value.input) == value.kb);
    const char* rejected[] = {
        "63", "63.4", "-64", "077", "08", "09", "0o100",
        "64KB", "64kb", "64kb ", "64 Kb", "64k", "64M", "0.062MB",
        "65023B", "64B", "64 MB x", "64 MB\nx", "64MBMB", "64 k B",
        "", " ", "NaN", "Infinity", "inf", "1e1000", "1e-1000",
        "2147483647.5", "2147483648", "2097152MB", "2TB", "1.999999TB",
        "00064", "64_000", "0x1p6", "0x1p6MB"
    };
    for (const auto* value : rejected) check(value, !parseWorkMemKb(value));
    // A SQL/protocol string cannot supply a C-string terminator followed by
    // hidden units. This native API must inspect the entire owning string.
    check("embedded NUL rejected", !parseWorkMemKb(std::string("64\0kB",5)));
    const struct { size_t kb; const char* output; } formatted[] = {
        {64,"64kB"}, {65,"65kB"}, {1024,"1MB"}, {4096,"4MB"},
        {1048576,"1GB"}, {1073741824,"1TB"}, {2147483647,"2147483647kB"},
        {1536,"1536kB"}, {2147482624,"2097151MB"}
    };
    for (const auto& value : formatted) check(value.output, formatWorkMemKb(value.kb) == value.output);

    SessionWorkMem setting;
    check("inherit initial instance default", setting.effective(4096) == 4096);
    check("unassigned setting follows reload", setting.effective(8192) == 8192);
    setting.assign(1024,false);
    check("explicit SET survives reload", setting.effective(16384) == 1024);
    setting.assign(64,true);
    check("SET LOCAL outside transaction has no effect", setting.effective(16384) == 1024);
    setting.assign(std::nullopt,false);
    check("RESET dynamically inherits", setting.effective(16384) == 16384);
    setting.assign(setting.effective(16384),false);
    check("FROM CURRENT becomes explicit", setting.effective(32768) == 16384);
    setting.assign(std::nullopt,false);
    check("reset FROM CURRENT inherits again", setting.effective(65536) == 65536);
    setting.begin(); setting.assign(2048,false);
    check("transaction sees regular SET", setting.effective(65536) == 2048);
    setting.finish(true);
    check("regular SET persists after commit", setting.effective(65536) == 2048);
    setting.begin(); setting.assign(128,true); setting.finish(true);
    check("LOCAL expires at commit", setting.effective(65536) == 2048);
    setting.begin(); setting.assign(256,true); setting.finish(false);
    check("LOCAL expires at rollback", setting.effective(65536) == 2048);
    setting.begin(); setting.assign(512,false); setting.assign(64,true);
    check("LOCAL masks transaction SET", setting.effective(65536) == 64);
    setting.finish(true);
    check("commit retains preceding regular SET", setting.effective(65536) == 512);
    setting.begin(); setting.assign(64,true); setting.assign(1024,false);
    check("regular SET supersedes LOCAL", setting.effective(65536) == 1024);
    setting.finish(true);
    check("superseding regular SET persists", setting.effective(65536) == 1024);
    setting.begin(); setting.assign(2048,false); setting.finish(false);
    check("aborted regular SET restores prior", setting.effective(65536) == 1024);
    setting.begin(); setting.assign(std::nullopt,false);
    check("transaction RESET sees current default", setting.effective(65536) == 65536);
    setting.finish(false);
    check("aborted RESET restores explicit value", setting.effective(65536) == 1024);
    setting.begin(); setting.assign(std::nullopt,true); setting.finish(true);
    check("LOCAL DEFAULT does not erase session SET", setting.effective(65536) == 1024);
    setting.begin(); setting.assign(std::nullopt,false); setting.finish(true);
    check("committed RESET inherits", setting.effective(65536) == 65536);
    check("committed RESET follows later reload", setting.effective(32768) == 32768);
    setting.begin(); setting.assign(256,false); setting.begin(); setting.finish(false);
    check("repeated begin preserves inherited rollback source", setting.effective(131072) == 131072);

    setting.assign(512,false); setting.begin(); setting.save("a"); setting.assign(1024,false);
    setting.save("b"); setting.assign(64,true);
    check("nested savepoint sees local value", setting.effective(4096) == 64);
    setting.rollback("b");
    check("rollback restores visible and persistent snapshots", setting.effective(4096) == 1024);
    setting.assign(128,true); setting.release("b");
    check("release preserves LOCAL", setting.effective(4096) == 128);
    setting.rollback("a");
    check("outer rollback cancels released subtransaction SET", setting.effective(4096) == 512);
    setting.assign(256,false); setting.finish(true);
    check("post-rollback SET commits", setting.effective(4096) == 256);
    setting.begin(); setting.save("same"); setting.assign(512,false); setting.save("same");
    setting.assign(64,true); setting.rollback("same");
    check("duplicate savepoint name selects newest", setting.effective(4096) == 512);
    setting.assign(128,true); setting.rollback("same");
    check("rollback target remains reusable", setting.effective(4096) == 512);
    setting.release("same"); setting.assign(1024,false); setting.rollback("same");
    check("release reveals older duplicate savepoint", setting.effective(4096) == 256);
    setting.finish(true);
    check("savepoint rollback also restores commit value", setting.effective(4096) == 256);
    setting.begin(); setting.save("outer"); setting.assign(1024,false); setting.save("inner");
    setting.assign(64,true); setting.release("outer");
    check("release outer preserves inner LOCAL and SET", setting.effective(4096) == 64);
    setting.finish(true);
    check("outer release commits regular but expires local", setting.effective(4096) == 1024);
    setting.begin(); setting.assign(512,false); setting.save("a"); setting.assign(64,true);
    setting.rollback("missing"); setting.release("missing");
    check("unmatched helper savepoint leaves state unchanged", setting.effective(4096) == 64);
    setting.finish(false);
    check("full abort restores pretransaction value", setting.effective(4096) == 1024);
    setting.begin(); setting.assign(64,true); setting.finish(true); setting.finish(false);
    check("finish outside transaction has no effect", setting.effective(4096) == 1024);

    setting.setStartup(8192);
    check("startup option resets session base", setting.effective(4096) == 8192);
    setting.assign(1024,false); setting.assign(std::nullopt,false);
    check("RESET retains startup reset source", setting.effective(16384) == 8192);
    setting.begin(); setting.assign(128,true); setting.finish(true);
    check("LOCAL restores startup option", setting.effective(16384) == 8192);
    setting.begin(); setting.assign(std::nullopt,false); setting.finish(true);
    check("committed RESET retains startup option", setting.effective(32768) == 8192);
    setting.assign(setting.effective(32768),false);
    check("startup FROM CURRENT remains explicit", setting.effective(65536) == 8192);
    setting.setStartup(32768);
    check("new startup replaces old base and override", setting.effective(4096) == 32768);
    auto peer = setting; peer.assign(64,false);
    check("session copies have independent overrides", setting.effective(4096) == 32768 && peer.effective(4096) == 64);
    peer.assign(std::nullopt,false);
    check("copy RESET retains its own startup base", peer.effective(4096) == 32768);

    assert(checked == 136);
    std::cout << "WORK_MEM_SETTING_COMPLETE " << checked << " FAILURES " << failures << '\n';
    return failures ? 1 : 0;
}
