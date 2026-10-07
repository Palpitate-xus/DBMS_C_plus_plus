#pragma once

#include "common/DbError.h"
#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace dbms::sql_array_text {
struct Dimension {
    int32_t lower = 1;
    int32_t length = 0;
    bool operator==(const Dimension& other) const {
        return lower == other.lower && length == other.length;
    }
};
struct Literal {
    std::string body;
    std::vector<std::string> elements;
    std::vector<Dimension> dimensions;
};
inline bool space(unsigned char ch) {
    return ch==' ' || ch=='\t' || ch=='\n' || ch=='\r' || ch=='\f' || ch=='\v';
}
inline std::string quote(const std::string& value) {
    std::string lower=value;
    for(char& ch:lower)if(ch>='A' && ch<='Z')ch=static_cast<char>(ch-'A'+'a');
    bool quoted=value.empty() || lower=="null";
    for(unsigned char ch:value)quoted=quoted || space(ch) || ch==',' || ch=='{' || ch=='}' || ch=='"' || ch=='\\';
    if(!quoted)return value;
    std::string output="\"";
    for(char ch:value){if(ch=='"' || ch=='\\')output+='\\';output+=ch;}
    return output+'"';
}
inline std::string prefix(const std::vector<Dimension>& dimensions) {
    const bool required=std::any_of(dimensions.begin(),dimensions.end(),[](const auto& dimension){return dimension.lower!=1;});
    std::string output;
    for(const auto& dimension:dimensions) {
        const int64_t upper=int64_t(dimension.lower)+dimension.length-1;
        if(dimension.length<=0 || upper>=std::numeric_limits<int32_t>::max())
            throw DbError("54000","array upper bound is too large");
        if(required)output+='['+std::to_string(dimension.lower)+':'+std::to_string(upper)+']';
    }
    return output.empty()?output:output+'=';
}
inline std::string render(const Literal& literal) {
    return literal.dimensions.empty()?"{}":prefix(literal.dimensions)+literal.body;
}

class Parser {
    const std::string& input_;
    size_t position_=0;
    [[noreturn]] void malformed() const {throw DbError("22P02","malformed array literal: "+input_);}
    void whitespace(){while(position_<input_.size() && space(static_cast<unsigned char>(input_[position_])))++position_;}
    int32_t bound() {
        bool negative=false;
        if(position_<input_.size() && (input_[position_]=='-' || input_[position_]=='+'))negative=input_[position_++]=='-';
        if(position_==input_.size() || input_[position_]<'0' || input_[position_]>'9')malformed();
        uint64_t magnitude=0;
        const uint64_t limit=negative?uint64_t(2147483648):uint64_t(2147483647);
        while(position_<input_.size() && input_[position_]>='0' && input_[position_]<='9') {
            const unsigned digit=static_cast<unsigned>(input_[position_++]-'0');
            if(magnitude>(limit-digit)/10)throw DbError("54000","array bound is out of integer range");
            magnitude=magnitude*10+digit;
        }
        return static_cast<int32_t>(negative?-static_cast<int64_t>(magnitude):static_cast<int64_t>(magnitude));
    }
    std::string scalar() {
        std::string value;
        bool quoted=false,escaped=false;
        if(position_<input_.size() && input_[position_]=='"') {
            quoted=true;++position_;
            bool closed=false;
            while(position_<input_.size()) {
                char ch=input_[position_++];
                if(ch=='"'){closed=true;break;}
                if(ch=='\\'){if(position_==input_.size())malformed();ch=input_[position_++];}
                if(ch=='\0')malformed();value+=ch;
            }
            if(!closed)malformed();
            whitespace();
        } else {
            std::vector<bool> escapedBytes;
            while(position_<input_.size() && input_[position_]!=',' && input_[position_]!='}') {
                char ch=input_[position_++];bool protectedByte=false;
                if(ch=='{' || ch=='"' || ch=='\0')malformed();
                if(ch=='\\'){if(position_==input_.size())malformed();ch=input_[position_++];protectedByte=true;escaped=true;}
                value+=ch;escapedBytes.push_back(protectedByte);
            }
            size_t first=0,last=value.size();
            while(first<last && space(static_cast<unsigned char>(value[first])) && !escapedBytes[first])++first;
            while(last>first && space(static_cast<unsigned char>(value[last-1])) && !escapedBytes[last-1])--last;
            value=value.substr(first,last-first);
            if(value.empty())malformed();
        }
        if(position_==input_.size() || (input_[position_]!=',' && input_[position_]!='}'))malformed();
        std::string lower=value;
        for(char& ch:lower)if(ch>='A' && ch<='Z')ch=static_cast<char>(ch-'A'+'a');
        if(!quoted && !escaped && lower=="null")return "NULL";
        return quote(value);
    }
    Literal body(size_t depth) {
        if(depth>=6)throw DbError("54000","number of array dimensions exceeds the maximum allowed (6)");
        whitespace();
        if(position_==input_.size() || input_[position_]!='{')malformed();
        ++position_;whitespace();
        Literal output;
        std::vector<Dimension> children;
        bool first=true,nested=false;
        if(position_<input_.size() && input_[position_]=='}'){++position_;output.body="{}";output.dimensions.push_back({1,0});return output;}
        while(true) {
            whitespace();
            if(position_==input_.size())malformed();
            const bool childArray=input_[position_]=='{';
            if(!first && nested!=childArray)malformed();
            if(childArray) {
                auto child=body(depth+1);
                if(!first && children!=child.dimensions)malformed();
                children=child.dimensions;output.elements.push_back(child.body);
            } else output.elements.push_back(scalar());
            first=false;nested=childArray;whitespace();
            if(position_==input_.size())malformed();
            if(input_[position_]=='}'){++position_;break;}
            if(input_[position_]!=',')malformed();
            ++position_;whitespace();
            if(position_==input_.size() || input_[position_]=='}')malformed();
        }
        if(output.elements.size()>static_cast<size_t>(std::numeric_limits<int32_t>::max()))
            throw DbError("54000","array size exceeds the maximum allowed");
        output.dimensions.push_back({1,static_cast<int32_t>(output.elements.size())});
        output.dimensions.insert(output.dimensions.end(),children.begin(),children.end());
        output.body="{";
        for(size_t i=0;i<output.elements.size();++i){if(i)output.body+=',';output.body+=output.elements[i];}
        output.body+='}';
        return output;
    }
public:
    explicit Parser(const std::string& input):input_(input){}
    Literal parse() {
        whitespace();
        std::vector<Dimension> declared;
        while(position_<input_.size() && input_[position_]=='[') {
            if(declared.size()>=6)throw DbError("54000","number of array dimensions exceeds the maximum allowed (6)");
            ++position_;int32_t lower=1,upper=bound();
            if(position_<input_.size() && input_[position_]==':'){lower=upper;++position_;upper=bound();}
            if(position_==input_.size() || input_[position_++]!=']')malformed();
            if(upper<lower)throw DbError("2202E","upper bound cannot be less than lower bound");
            const int64_t extent=int64_t(upper)-lower+1;
            if(upper==std::numeric_limits<int32_t>::max() || extent>std::numeric_limits<int32_t>::max())
                throw DbError("54000","array bound or size exceeds the maximum allowed");
            declared.push_back({lower,static_cast<int32_t>(extent)});whitespace();
        }
        if(!declared.empty()){if(position_==input_.size() || input_[position_++]!='=')malformed();whitespace();}
        auto output=body(0);whitespace();
        if(position_!=input_.size())malformed();
        if(!declared.empty()) {
            if(declared.size()!=output.dimensions.size())malformed();
            for(size_t i=0;i<declared.size();++i)if(declared[i].length!=output.dimensions[i].length)malformed();
            output.dimensions=std::move(declared);
        }
        if(std::any_of(output.dimensions.begin(),output.dimensions.end(),[](const auto& dimension){return dimension.length==0;})) {
            output.body="{}";output.elements.clear();output.dimensions.clear();
        }
        return output;
    }
};
inline Literal parse(const std::string& input){return Parser(input).parse();}
inline std::string compose(const std::vector<std::string>& elements,const std::vector<Dimension>& dimensions) {
    if(elements.empty())return "{}";
    std::string body="{";
    for(size_t i=0;i<elements.size();++i){if(i)body+=',';body+=elements[i];}
    return prefix(dimensions)+body+'}';
}
inline std::string concatenate(const std::string& left,const std::string& right) {
    auto a=parse(left),b=parse(right);
    if(a.dimensions.empty())return render(b);
    if(b.dimensions.empty())return render(a);
    auto dimensions=a.dimensions;
    auto elements=a.elements;
    const auto sameTail=[](const auto& small,const auto& large,size_t offset) {
        return small.size()+offset==large.size() && std::equal(small.begin(),small.end(),large.begin()+offset);
    };
    if(a.dimensions.size()==b.dimensions.size()) {
        if(!std::equal(a.dimensions.begin()+1,a.dimensions.end(),b.dimensions.begin()+1))
            throw DbError("2202E","cannot concatenate incompatible arrays");
        elements.insert(elements.end(),b.elements.begin(),b.elements.end());
    } else if(a.dimensions.size()+1==b.dimensions.size()) {
        if(!sameTail(a.dimensions,b.dimensions,1))throw DbError("2202E","cannot concatenate incompatible arrays");
        elements={a.body};elements.insert(elements.end(),b.elements.begin(),b.elements.end());dimensions=b.dimensions;
    } else if(b.dimensions.size()+1==a.dimensions.size()) {
        if(!sameTail(b.dimensions,a.dimensions,1))throw DbError("2202E","cannot concatenate incompatible arrays");
        elements.push_back(b.body);
    } else throw DbError("2202E","cannot concatenate incompatible arrays");
    if(elements.size()>static_cast<size_t>(std::numeric_limits<int32_t>::max()))throw DbError("54000","array size exceeds the maximum allowed");
    dimensions.front().length=static_cast<int32_t>(elements.size());
    return compose(elements,dimensions);
}
} // namespace dbms::sql_array_text
