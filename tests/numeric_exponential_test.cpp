// test_sources: src/types/numeric.cpp
#include "types/numeric.h"
#include <iostream>
#include <stdexcept>
#include <string>
using dbms::Numeric;
void eq(const std::string& x, const std::string& expected) {
    auto actual=Numeric::fromString(x).exponential().toString();
    if(actual!=expected) throw std::runtime_error("mismatch: "+x+" => "+actual);
}
void range(const std::string& x) {
    try { (void)Numeric::fromString(x).exponential(); }
    catch(const dbms::NumericRangeError&) { return; }
    throw std::runtime_error("expected NumericRangeError: "+x);
}
int main(){
 eq("NaN","NaN");eq("Infinity","Infinity");eq("-Infinity","0");
 eq("0","1.0000000000000000");eq("-0.00000000000000000","1.00000000000000000");
 eq("1e2","26881171418161354484126255515800135873611119");
 eq("-0.0000000100000000","0.9999999900000001");
 eq("-2304","0."+std::string(1000,'0'));
 eq("-2303.9","0."+std::string(1000,'0'));
 eq("-2303","0."+std::string(999,'0')+"1");
 eq("1e-999","1."+std::string(998,'0')+"1");
 eq("-1e-999","0."+std::string(999,'9'));
 range("1e-1000"); range("2303"); range("2304");
 range("1e-1001");range("0."+std::string(1001,'0'));
 // Scale 998 half-way inputs: PG's retained quadratic term rounds both up.
 std::string h="0."+std::string(498,'0')+"1"+std::string(499,'0');
 std::string plus="1."+std::string(498,'0')+"1"+std::string(498,'0')+"1";
 std::string minus="0."+std::string(498,'9')+"9"+std::string(498,'0')+"1";
 eq(h,plus);eq("-"+h,minus);
 std::cout<<"component assertions passed\n";
}
