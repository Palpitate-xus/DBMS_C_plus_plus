#include "expression/ExprEvaluator.h"
#include "parser/ast.h"
#include "types/bytea.h"
#include <cctype>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// Run through scripts/build_common.sh's dbms_run_isolated_test. Its owned
// cwd is established before this executable's global initializers; do not
// attempt late chdir/global cleanup in main. This fixture creates no DB.
// Oracle: PostgreSQL REL_18_6 src/backend/utils/adt/encode.c.
namespace {
using namespace dbms;
struct Checks {
    size_t controls=0,failures=0;
    template<class Action> void test(const std::string& label,Action action) {
        ++controls;
        try {action();}
        catch(const std::exception& error){++failures;std::cerr<<"ENCODING_BOUNDARIES_FAIL "<<controls<<' '<<label<<" : "<<error.what()<<'\n';}
        catch(...){++failures;std::cerr<<"ENCODING_BOUNDARIES_FAIL "<<controls<<' '<<label<<" : nonstandard exception\n";}
    }
};
void require(bool value,const std::string& message){if(!value)throw std::runtime_error(message);}
ExprValue text(const std::string& value){return ExprValue("text",value,false);}
ExprValue bytes(const std::string& value){return ExprValue("bytea",ByteaValue::fromBytes(value).toString(),false);}
ExprValue call(const std::string& name,const std::vector<ExprValue>& values) {
    FunctionCallExpr expression;expression.funcName=name;
    for(size_t index=0;index<values.size();++index) {
        auto parameter=std::make_unique<ParameterExpr>();parameter->slot=index;
        parameter->declaredType=values[index].typeName;parameter->origin=ParameterOrigin::RuntimeCell;
        expression.args.push_back(std::move(parameter));
    }
    RowContext row;row.setParameters(values);ExprEvaluator evaluator;
    return evaluator.eval(&expression,row);
}
void matches(const ExprValue& actual,const ExprValue& expected) {
    require(actual.typeName==expected.typeName && actual.isNull==expected.isNull,"result declaration/NULL differs");
    if(!expected.isNull)require(actual.value==expected.value,"result bytes differ");
}
template<class Action> void rejects(const std::string& state,Action action) {
    bool good=false;
    try {(void)action();}
    catch(const std::exception& error){good=std::string(error.what()).find("SQLSTATE "+state)!=std::string::npos;}
    require(good,"expected SQLSTATE "+state);
}
std::string escape(const std::string& raw) {
    std::string result;
    for(unsigned char byte:raw) {
        if(byte==0 || byte>=128) {
            result+='\\';result+=static_cast<char>('0'+(byte>>6));
            result+=static_cast<char>('0'+((byte>>3)&7));result+=static_cast<char>('0'+(byte&7));
        } else if(byte=='\\')result+="\\\\";
        else result+=static_cast<char>(byte);
    }
    return result;
}
std::string base64As(size_t length) {
    // Independent fixed alphabet vector: every full triple is "aaa".
    // PG emits LF after each 19 full triples, even at EOF. The final
    // padded quartet never creates an additional line break.
    std::string result;
    for(size_t group=0;group<length/3;++group){result+="YWFh";if((group+1)%19==0)result+='\n';}
    if(length%3==1)result+="YQ==";else if(length%3==2)result+="YWE=";
    return result;
}
struct HexCase {std::string input,output;bool valid=true;};
std::vector<HexCase> hexCases() {
    std::vector<HexCase> result={{"",""},{"aAbBcCdDeEfF00","aabbccddeeff00"},
        {"61 62\t63\r64\n65","6162636465"},{"0","",false},{"616","",false},
        {"zz","",false},{"0x61","",false},{"\\x61","",false}};
    for(char whitespace:std::string(" \t\n\r")) {
        for(size_t position=0;position<=4;++position) {
            std::string input="6162";input.insert(position,1,whitespace);
            result.push_back({input,"6162",position%2==0});
        }
        const std::string gap(1,whitespace);
        result.push_back({gap+"61"+gap+"62"+gap,"6162"});
        result.push_back({gap,""});
    }
    for(char whitespace:std::string("\v\f"))for(size_t position=0;position<=4;++position) {
        std::string input="6162";input.insert(position,1,whitespace);result.push_back({input,"",false});
    }
    return result;
}
const std::vector<size_t> lengths={0,1,2,3,54,55,56,57,58,59,60,111,112,113,114,115,116,117,170,171,172};
const std::vector<std::pair<std::string,std::string>> valid64={{"",""},{"TQ==","M"},{"TWE=","Ma"},
    {"TWFu","Man"},{"T Q\n=\t=","M"},{" \t\r\n",""},{"YQ==\n","a"},{"TWE=\r\n","Ma"}};
const std::vector<std::string> invalid64={"TQ","TWE","A===","=AAA","A=AA","TQ===", "TQ==!",
    "TQ=\v=","TQ=\f=","TWFu$","AAAA=","===="};
const std::vector<std::string> invalidEscape={"\\","\\12","\\128","\\400","\\x","\\08x"};
}

int main() {
    using namespace dbms;
    Checks checks;
    // The first entry exercises actual typed RuntimeCell inputs. No registry
    // bootstrap, current Session, database creation, or global-engine use is
    // introduced by the fixture before or after these pure codec controls.
    for(unsigned byte=0;byte<256;++byte) {
        const std::string raw(1,static_cast<char>(byte));const auto encoded=escape(raw);
        checks.test("escape byte "+std::to_string(byte),[&]{matches(call("encode",{bytes(raw),text("escape")}),text(encoded));});
        checks.test("escape roundtrip byte "+std::to_string(byte),[&]{matches(call("decode",{text(encoded),text("escape")}),bytes(raw));});
    }
    std::string all;for(unsigned byte=0;byte<256;++byte)all+=static_cast<char>(byte);
    checks.test("escape full 256-byte payload",[&]{matches(call("encode",{bytes(all),text("escape")}),text(escape(all)));});
    checks.test("decode full 256-byte payload",[&]{matches(call("decode",{text(escape(all)),text("escape")}),bytes(all));});
    checks.test("full 256-byte nested roundtrip",[&]{matches(call("encode",{call("decode",{text(escape(all)),text("escape")}),text("escape")}),text(escape(all)));});

    size_t index=0;
    for(const auto& item:hexCases())checks.test("hex adjacency/whitespace "+std::to_string(index++),[&]{
        if(item.valid)matches(call("decode",{text(item.input),text("hex")}),ExprValue("bytea","\\x"+item.output,false));
        else rejects("22023",[&]{return call("decode",{text(item.input),text("hex")});});
    });
    for(size_t length:lengths) {
        const std::string raw(length,'a');const auto encoded=base64As(length);
        checks.test("base64 exact wrap "+std::to_string(length),[&]{matches(call("encode",{bytes(raw),text("base64")}),text(encoded));});
        checks.test("base64 wrapped decode "+std::to_string(length),[&]{matches(call("decode",{text(encoded),text("base64")}),bytes(raw));});
        checks.test("base64 wrapped roundtrip "+std::to_string(length),[&]{matches(call("encode",{call("decode",{text(encoded),text("base64")}),text("base64")}),text(encoded));});
    }
    for(const auto& item:valid64)checks.test("base64 valid padding/whitespace",[&]{matches(call("decode",{text(item.first),text("base64")}),bytes(item.second));});
    for(const auto& input:invalid64)checks.test("base64 invalid padding/whitespace",[&]{rejects("22023",[&]{return call("decode",{text(input),text("base64")});});});

    for(const auto& format:std::vector<std::string>{"hex","base64","escape"}) {
        const ExprValue nullBytes("bytea","",true),nullText("text","",true);
        checks.test("empty encode "+format,[&]{matches(call("encode",{bytes(""),text(format)}),text(""));});
        checks.test("empty decode "+format,[&]{matches(call("decode",{text(""),text(format)}),bytes(""));});
        checks.test("NULL encode data "+format,[&]{matches(call("encode",{nullBytes,text(format)}),nullText);});
        checks.test("NULL encode format "+format,[&]{matches(call("encode",{bytes("abc"),nullText}),nullText);});
        checks.test("NULL decode data "+format,[&]{matches(call("decode",{nullText,text(format)}),nullBytes);});
        checks.test("NULL decode format "+format,[&]{matches(call("decode",{text("abc"),nullText}),nullBytes);});
        std::string upper=format;for(char& byte:upper)byte=static_cast<char>(std::toupper(static_cast<unsigned char>(byte)));
        std::string mixed=format;mixed[0]=static_cast<char>(std::toupper(static_cast<unsigned char>(mixed[0])));
        const std::string encoded=format=="hex"?"616263":format=="base64"?"YWJj":"abc";
        checks.test("uppercase format "+format,[&]{matches(call("encode",{bytes("abc"),text(upper)}),text(encoded));});
        checks.test("mixed case decode format "+format,[&]{matches(call("decode",{text(encoded),text(mixed)}),bytes("abc"));});
    }
    checks.test("invalid encode format",[&]{rejects("22023",[&]{return call("encode",{bytes("abc"),text("rot13")});});});
    checks.test("invalid decode format",[&]{rejects("22023",[&]{return call("decode",{text("abc"),text("rot13")});});});
    for(const auto& input:invalidEscape)checks.test("invalid escape sequence",[&]{rejects("22P02",[&]{return call("decode",{text(input),text("escape")});});});

    // Escape256*2+3=515 + hex46 + base64 boundaries21*3=63
    // + base64 decoding8+12=20 + format/empty/NULL3*8+2=26 + bad escape6 = 676.
    constexpr size_t expected=676;
    if(checks.controls!=expected){++checks.failures;std::cerr<<"ENCODING_BOUNDARIES_INVENTORY expected "<<expected<<" got "<<checks.controls<<'\n';}
    std::cout<<"ENCODING_BOUNDARIES_NATIVE_COMPLETE "<<checks.controls<<" EXPECTED "<<expected<<" FAILURES "<<checks.failures<<'\n';
    return checks.failures?1:0;
}
