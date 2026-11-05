#pragma once

#include "catalog/AnalyzedRangeStatistics.h"
#include "common/TemporalStorageCodec.h"
#include <cctype>

namespace dbms::analyzed_statistics::range_input {
struct BuiltinRange {
    uint32_t oid, subtype;
    const char* name;
};
inline std::optional<BuiltinRange> builtin(uint32_t oid) {
    switch (oid) {
        case 3904: return BuiltinRange{3904,23,"int4range"};
        case 3926: return BuiltinRange{3926,20,"int8range"};
        case 3906: return BuiltinRange{3906,1700,"numrange"};
        case 3912: return BuiltinRange{3912,1082,"daterange"};
        case 3908: return BuiltinRange{3908,1114,"tsrange"};
        case 3910: return BuiltinRange{3910,1184,"tstzrange"};
        default: return {};
    }
}
[[noreturn]] inline void invalid() {
    throw DbError("XX001", "invalid actual analyzed builtin range storage input");
}
inline std::string trim(std::string text) {
    const auto space=[](unsigned char c) { return c==' ' || c=='\t' || c=='\n' || c=='\r' || c=='\f' || c=='\v'; };
    size_t begin=0,end=text.size();
    while(begin<end && space(text[begin]))++begin;
    while(end>begin && space(text[end-1]))--end;
    return text.substr(begin,end-begin);
}
inline std::string keyword(std::string text) {
    for(char& c:text)if(c>='A'&&c<='Z')c+= 'a'-'A';
    return text;
}
inline std::string unquote(std::string text) {
    text=trim(std::move(text));
    if(text.empty())return text;
    if(text.front()!='"') {
        if(text.find_first_of("\"\\")!=std::string::npos)invalid();
        return text;
    }
    std::string value;
    for(size_t i=1;i<text.size();++i) {
        if(text[i]=='\\') {
            if(++i>=text.size())invalid();
            value+=text[i];
        } else if(text[i]=='"') {
            if(i+1==text.size())return value;
            if(text[i+1]!='"')invalid();
            value+='"';++i;
        } else value+=text[i];
    }
    invalid();
}
inline int64_t timestampKey(std::string value,bool withTimeZone) {
    const auto special=keyword(value);
    if(special=="infinity")return INT64_MAX;
    if(special=="-infinity")return INT64_MIN;
    const auto separator=value.find_first_of(" Tt");
    if(separator==std::string::npos)value+=" 00:00:00";
    else value[separator]=' ';
    const size_t start=value.find(' ')+1;
    if(!withTimeZone) {
        const auto zone=value.find_first_of("+-Zz",start);
        if(zone!=std::string::npos)value.erase(zone);
    }
    int64_t micros=0;
    const auto dot=value.find('.',start);
    if(dot!=std::string::npos) {
        size_t end=dot+1;
        while(end<value.size()&&value[end]>='0'&&value[end]<='9')++end;
        const size_t digits=end-dot-1;
        if(!digits)invalid();
        for(size_t i=0;i<6;++i)micros=micros*10+(i<digits?value[dot+1+i]-'0':0);
        if(digits>6) {
            bool tail=false;
            for(size_t i=dot+8;i<end;++i)tail|=value[i]!='0';
            if(value[dot+7]>'5'||(value[dot+7]=='5'&&(tail||(micros&1))))++micros;
        }
        value.erase(dot,digits+1);
    }
    const int64_t seconds=parseTimestampToSeconds(value);
    if(!seconds||isInfiniteTimestamp(seconds))invalid();
    const __int128 key=(static_cast<__int128>(seconds)-temporal_storage_detail::postgresEpochSeconds())*
        temporal_storage_detail::MicrosPerSecond+micros;
    if(key<=INT64_MIN||key>=INT64_MAX)invalid();
    return static_cast<int64_t>(key);
}
inline int64_t integerKey(const BuiltinRange& type,const RangeBound& bound) {
    if(bound.integer)return *bound.integer;
    if(type.subtype==1114||type.subtype==1184)return timestampKey(bound.text,type.subtype==1184);
    if(type.subtype==1082) {
        const auto special=keyword(bound.text);
        if(special=="infinity")return INT32_MAX;
        if(special=="-infinity")return INT32_MIN;
        const Date date(bound.text.c_str());
        if(!date.year)invalid();
        return date.convert()-Date(2000,1,1).convert();
    }
    try {
        size_t size=0;const auto value=std::stoll(bound.text,&size);
        if(size!=bound.text.size()||(type.subtype==23&&(value<INT32_MIN||value>INT32_MAX)))invalid();
        return value;
    } catch(const std::invalid_argument&) { invalid(); }
      catch(const std::out_of_range&) { invalid(); }
}
inline Numeric numericKey(const RangeBound& bound) {
    try { return Numeric(bound.text); }
    catch(const std::invalid_argument&) { invalid(); }
    catch(const std::overflow_error&) { invalid(); }
}
inline int compare(const BuiltinRange& type,const RangeBound& a,const RangeBound& b) {
    if(a.text==b.text)return 0;
    if(type.subtype==1700) {
        std::optional<Numeric> x,y;
        const Numeric& left=a.numeric?*a.numeric:x.emplace(numericKey(a));
        const Numeric& right=b.numeric?*b.numeric:y.emplace(numericKey(b));
        return (left>right)-(left<right);
    }
    const auto left=integerKey(type,a),right=integerKey(type,b);
    return (left>right)-(left<right);
}
inline double distance(const BuiltinRange& type,const RangeBound& upper,const RangeBound& lower) {
    if(type.subtype==1700) {
        std::optional<Numeric> a,b;
        const Numeric& x=upper.numeric?*upper.numeric:a.emplace(numericKey(upper));
        const Numeric& y=lower.numeric?*lower.numeric:b.emplace(numericKey(lower));
        const Numeric difference=x-y;
        if(difference.isNaN())return std::numeric_limits<double>::quiet_NaN();
        if(difference.isInfinite())return difference.sign()<0?
            -std::numeric_limits<double>::infinity():std::numeric_limits<double>::infinity();
        std::istringstream input(difference.toString());input.imbue(std::locale::classic());
        double result=0;input>>result;
        if(input.fail()||!input.eof()||!std::isfinite(result)||(result==0&&difference.sign()!=0))
            throw DbError("22003","numeric range length is out of range for double precision");
        return result;
    }
    // PG's builtin subdiff converts each operand to float8 before subtraction,
    // including int8 and the actual timestamp infinity sentinel values.
    const double result=double(integerKey(type,upper))-double(integerKey(type,lower));
    return type.subtype==1114||type.subtype==1184?result/1000000.0:result;
}
inline std::string quote(const std::string& text) {
    if(text.find_first_of(" \t\n\r,\"\\()[]")==std::string::npos)return text;
    std::string result="\"";
    for(char c:text){if(c=='"'||c=='\\')result+='\\';result+=c;}
    return result+'"';
}
inline ExprValue construct(const BuiltinRange& type,const RangeBound& lower,const RangeBound& upper) {
    return ExprValue(type.name,std::string(lower.inclusive?"[":"(")+
        (lower.unbounded?"":quote(lower.text))+","+(upper.unbounded?"":quote(upper.text))+
        (upper.inclusive?"]":")"),false);
}

// Only the actual physical-input owner may call this decoder. It is separate
// from SQL input/casts and must never reconstruct a rendered query result.
inline RangeSample decode(const BuiltinRange& type,const std::string& raw,bool isNull,
    uint64_t ordinal,uint32_t storedWidth,bool prepareKeys=false) {
    RangeSample row;row.ordinal=ordinal;row.storedWidth=storedWidth;row.isNull=isNull;
    if(isNull)return row;
    const auto text=trim(raw);
    if(keyword(text)=="empty"){row.empty=true;return row;}
    if(text.size()<3||(text.front()!='['&&text.front()!='(')||
       (text.back()!=']'&&text.back()!=')'))invalid();
    const std::string body=text.substr(1,text.size()-2);
    bool quoted=false;size_t comma=std::string::npos;
    for(size_t i=0;i<body.size();++i) {
        if(body[i]=='\\'&&quoted){if(++i==body.size())invalid();}
        else if(body[i]=='"')quoted=!quoted;
        else if(body[i]==','&&!quoted){if(comma!=std::string::npos)invalid();comma=i;}
    }
    if(quoted||comma==std::string::npos)invalid();
    row.lower.text=unquote(body.substr(0,comma));row.upper.text=unquote(body.substr(comma+1));
    row.lower.lower=true;row.upper.lower=false;
    row.lower.unbounded=row.lower.text.empty();row.upper.unbounded=row.upper.text.empty();
    row.lower.inclusive=text.front()=='[';row.upper.inclusive=text.back()==']';
    if((row.lower.unbounded&&row.lower.inclusive)||(row.upper.unbounded&&row.upper.inclusive))invalid();
    for(auto* bound:{&row.lower,&row.upper})if(!bound->unbounded) {
        if(type.subtype==1700) {
            auto value=numericKey(*bound);
            if(prepareKeys)bound->numeric=std::make_shared<Numeric>(std::move(value));
        } else {
            const auto value=integerKey(type,*bound);
            if(prepareKeys)bound->integer=value;
        }
    }
    if(compareRangeBounds(row.lower,row.upper,[&](const auto& a,const auto& b){return compare(type,a,b);})>0)invalid();
    return row;
}
} // namespace dbms::analyzed_statistics::range_input
