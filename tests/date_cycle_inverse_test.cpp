#include "common/DateType.h"

#include <cstdint>
#include <iostream>
#include <limits>
#include <vector>

int main() {
    const int monthLengths[]={31,28,31,30,31,30,31,31,30,31,30,31};
    int64_t ordinal=0;
    // Independent Gregorian enumeration: do not derive expected dates by
    // calling convert(), DISCONV(), dateAddDayOffset(), or timestamp code.
    for(int year=1;year<=9999;++year) {
        const bool leap=year%4==0 && (year%100!=0 || year%400==0);
        for(int month=1;month<=12;++month) {
            const int length=monthLengths[month-1]+(month==2 && leap);
            for(int day=1;day<=length;++day) {
                ++ordinal;
                const Date actual=DISCONV(ordinal);
                if(actual.year!=year || actual.month!=month || actual.day!=day) {
                    std::cerr<<"DATE_CYCLE_FIRST_MISMATCH ordinal="<<ordinal
                             <<" expected="<<year<<'-'<<month<<'-'<<day
                             <<" actual_year="<<actual.year<<" actual_month="<<actual.month<<'\n';
                    return 1; // Do not index a malformed returned month again.
                }
                if(actual.convert()!=ordinal || Date(year,month,day).convert()!=ordinal) {
                    std::cerr<<"DATE_CYCLE_FORWARD_MISMATCH ordinal="<<ordinal<<'\n';
                    return 1;
                }
            }
        }
    }
    size_t controls=0,failures=0;
    const auto require=[&](bool good) {
        ++controls;
        if(!good){++failures;std::cerr<<"DATE_CYCLE_BOUNDARY_FAILURE "<<controls<<'\n';}
    };
    for(const int64_t invalid:{std::numeric_limits<int64_t>::min(),int64_t{-1},int64_t{0},
            int64_t{3652060},int64_t{3652061},std::numeric_limits<int64_t>::max()})
        require(DISCONV(invalid).year==0);
    struct Shift { int year,month,day,days,resultYear,resultMonth,resultDay; };
    for(const Shift shift:std::vector<Shift>{
            {399,12,31,1,400,1,1},{1599,12,31,1,1600,1,1},
            {1999,12,31,1,2000,1,1},{2399,12,31,1,2400,1,1},
            {2000,2,28,1,2000,2,29},{1900,2,28,1,1900,3,1}}) {
        const Date start(shift.year,shift.month,shift.day);
        const Date wanted(shift.resultYear,shift.resultMonth,shift.resultDay);
        require(start+shift.days==wanted);
        require(wanted-shift.days==start);
    }
    require((Date(1,1,1)-1).year==0);
    require((Date(9999,12,31)+1).year==0);
    require((Date(2000,1,1)+std::numeric_limits<int64_t>::max()).year==0);
    require((Date(2000,1,1)-std::numeric_limits<int64_t>::min()).year==0);
    std::cout<<"DATE_CYCLE_INVERSE_COMPLETE DAYS "<<ordinal<<" BOUNDARY_SHIFT_CONTROLS "
             <<controls<<" FAILURES "<<failures<<'\n';
    return ordinal==3652059 && controls==22 && failures==0?0:1;
}
