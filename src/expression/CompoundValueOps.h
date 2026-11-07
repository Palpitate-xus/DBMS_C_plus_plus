#pragma once
#include "expression/ExprEvaluator.h"
#include "expression/common_type.h"
#include "common/SqlArrayText.h"
#include <algorithm>
#include <limits>
#include <iterator>
#include <memory>

namespace dbms::compound_value_detail {
inline std::string unquote(const std::string& token) {
    if(token.size()<2 || token.front()!='"')return token;
    std::string value;value.reserve(token.size()-2);
    for(size_t i=1;i+1<token.size();++i){if(token[i]=='\\')++i;value+=token[i];}
    return value;
}
inline size_t elementCount(const std::vector<DatumArrayDimension>& dims) {
    if(dims.size()>6)throw DbError("54000","number of array dimensions exceeds the maximum allowed");
    size_t count=dims.empty()?0:1;
    for(const auto& d:dims) {
        if(d.length<=0 || int64_t(d.lower)+d.length-1>=INT32_MAX ||
           count>static_cast<size_t>(INT32_MAX)/static_cast<size_t>(d.length))
            throw DbError("54000","array size or bound exceeds the maximum allowed");
        count*=static_cast<size_t>(d.length);
    }
    return count;
}
inline std::vector<DatumArrayDimension> dimensions(const sql_array_text::Literal& literal) {
    std::vector<DatumArrayDimension> result;result.reserve(literal.dimensions.size());
    for(const auto& d:literal.dimensions)result.push_back({d.length,d.lower});
    return result;
}
inline const std::vector<ExprValue>* owningElements(const ExprValue& value) {
    return value.compound && value.compound->arrayElements?&*value.compound->arrayElements:nullptr;
}
inline std::vector<ExprValue> elements(const ExprValue& value) {
    if(value.isNull)return {};
    if(const auto* owned=owningElements(value)) {
        if(elementCount(value.compound->dimensions)!=owned->size())
            throw DbError("XX000","array owning values lost their dimensions");
        return *owned;
    }
    const auto type=common_type_detail::canonical(value.typeName);
    const auto element=common_type_detail::array(type)?type.substr(0,type.size()-2):std::string("unknown");
    const auto literal=sql_array_text::parse(value.value);
    std::vector<ExprValue> result;
    const auto dims=dimensions(literal);result.reserve(elementCount(dims));
    const auto visit=[&](const auto& self,const sql_array_text::Literal& part,size_t depth)->void {
        for(const auto& token:part.elements) {
            if(depth+1<dims.size()){self(self,sql_array_text::parse(token),depth+1);continue;}
            const bool null=token=="NULL";
            // A rendered record cannot recover field declarations, NULLs or
            // owning nested values. Typed record arrays must keep their datum.
            if(!null && element=="record")
                throw DbError("0A000","input of anonymous composite types is not implemented");
            ExprValue cell(element,null?std::string{}:unquote(token),null);
            if(result.size()<value.elementOids.size())cell.objectOid=value.elementOids[result.size()];
            cell.collation=value.collation;result.push_back(std::move(cell));
        }
    };
    visit(visit,literal,0);
    if(result.size()!=elementCount(dims))throw DbError("XX000","array text and dimensions disagree");
    return result;
}
inline std::vector<DatumArrayDimension> arrayDimensions(const ExprValue& value) {
    if(value.isNull)return {};
    return owningElements(value)?value.compound->dimensions:dimensions(sql_array_text::parse(value.value));
}
inline std::optional<bool> bitmap(const ExprValue& value) {
    if(value.compound && value.compound->arrayNullBitmap)return value.compound->arrayNullBitmap;
    if(value.isNull)return {};
    if(const auto* values=owningElements(value)) {
        if(value.compound->dimensions.empty())return false;
        if(std::any_of(values->begin(),values->end(),[](const ExprValue& x){return x.isNull;}))return true;
        return {};
    }
    const auto values=elements(value);
    if(std::any_of(values.begin(),values.end(),[](const ExprValue& x){return x.isNull;}))return true;
    // Text alone cannot prove that a NULL-free array has no retained bitmap.
    return {};
}
inline void attach(ExprValue& value,std::vector<ExprValue> items,
                   std::vector<DatumArrayDimension> dims,DatumTypePtr type={},
                   std::optional<bool> sourceBitmap={},bool preserveUnknownBitmap=false) {
    if(elementCount(dims)!=items.size())throw DbError("XX000","array owning payload cardinality mismatch");
    const bool hasNull=std::any_of(items.begin(),items.end(),[](const ExprValue& x){return x.isNull;});
    auto data=std::make_shared<CompoundDatum>();data->type=std::move(type);
    data->arrayNullBitmap=hasNull?std::optional<bool>(true):
        (sourceBitmap?sourceBitmap:(preserveUnknownBitmap?std::optional<bool>{}:std::optional<bool>(false)));
    data->arrayElements=std::move(items);data->dimensions=std::move(dims);
    // A true empty array has no bitmap, even when it came from a nonempty slice.
    if(data->dimensions.empty())data->arrayNullBitmap=false;
    const auto canonical=common_type_detail::canonical(value.typeName);
    if(canonical=="regtype[]" || canonical=="regclass[]") {
        value.elementOids.clear();value.elementOids.reserve(data->arrayElements->size());
        for(const auto& item:*data->arrayElements)value.elementOids.push_back(item.objectOid);
    }
    value.compound=std::move(data);
}
inline std::string render(const std::vector<ExprValue>& items,
                          const std::vector<DatumArrayDimension>& dims) {
    if(elementCount(dims)!=items.size())throw DbError("XX000","array render cardinality mismatch");
    if(dims.empty())return "{}";
    size_t index=0;
    const auto body=[&](const auto& self,size_t depth)->std::string {
        std::string out="{";
        for(int32_t i=0;i<dims[depth].length;++i) {
            if(i)out+=',';
            if(depth+1<dims.size())out+=self(self,depth+1);
            else {const auto& cell=items.at(index++);out+=cell.isNull?"NULL":sql_array_text::quote(cell.value);}
        }
        return out+'}';
    };
    std::vector<sql_array_text::Dimension> textDims;textDims.reserve(dims.size());
    for(const auto& d:dims)textDims.push_back({d.lower,d.length});
    return sql_array_text::prefix(textDims)+body(body,0);
}
inline std::optional<bool> mergedBitmap(const ExprValue& left,const ExprValue& right) {
    const auto a=bitmap(left),b=bitmap(right);
    if((a && *a)||(b && *b))return true;
    return a && b?std::optional<bool>(false):std::optional<bool>{};
}
inline ExprValue concatenated(const ExprValue& left,const ExprValue& right,
                              const std::string& type) {
    if(left.isNull){auto out=right;out.typeName=type;return out;}
    if(right.isNull){auto out=left;out.typeName=type;return out;}
    auto a=arrayDimensions(left),b=arrayDimensions(right);
    if(a.empty()){auto out=right;out.typeName=type;return out;}
    if(b.empty()){auto out=left;out.typeName=type;return out;}
    const auto sameTail=[](const auto& small,const auto& large,size_t offset) {
        return small.size()+offset==large.size() && std::equal(small.begin(),small.end(),large.begin()+offset,
            [](const auto& x,const auto& y){return x.length==y.length && x.lower==y.lower;});
    };
    auto dims=a;int64_t leading=0;
    if(a.size()==b.size()) {
        if(!std::equal(a.begin()+1,a.end(),b.begin()+1,
            [](const auto& x,const auto& y){return x.length==y.length && x.lower==y.lower;}))
            throw DbError("2202E","cannot concatenate incompatible arrays");
        leading=int64_t(a.front().length)+b.front().length;
    } else if(a.size()+1==b.size()) {
        if(!sameTail(a,b,1))throw DbError("2202E","cannot concatenate incompatible arrays");
        dims=b;leading=int64_t(b.front().length)+1;
    } else if(b.size()+1==a.size()) {
        if(!sameTail(b,a,1))throw DbError("2202E","cannot concatenate incompatible arrays");
        leading=int64_t(a.front().length)+1;
    } else throw DbError("2202E","cannot concatenate incompatible arrays");
    if(leading>INT32_MAX)throw DbError("54000","array size exceeds the maximum allowed");
    dims.front().length=static_cast<int32_t>(leading);
    auto items=elements(left);auto tail=elements(right);
    if(tail.size()>items.max_size()-items.size())throw DbError("54000","array size exceeds the maximum allowed");
    items.insert(items.end(),std::make_move_iterator(tail.begin()),std::make_move_iterator(tail.end()));
    ExprValue result(type,render(items,dims),false);
    attach(result,std::move(items),std::move(dims),{},mergedBitmap(left,right),true);
    return result;
}
inline ExprValue pushed(const ExprValue& array,const ExprValue& value,bool prepend,
                        const std::string& type) {
    auto dims=arrayDimensions(array);
    if(dims.size()>1)throw DbError("22000","argument must be empty or one-dimensional array");
    auto values=elements(array);
    if(prepend)values.insert(values.begin(),value);else values.push_back(value);
    if(values.size()>static_cast<size_t>(INT32_MAX))throw DbError("54000","array size exceeds the maximum allowed");
    if(dims.empty())dims.push_back({static_cast<int32_t>(values.size()),1});
    else dims.front().length=static_cast<int32_t>(values.size());
    ExprValue result(type,render(values,dims),false);
    attach(result,std::move(values),std::move(dims),{},array.isNull?std::optional<bool>(false):bitmap(array),true);return result;
}

inline ExprValue constructed(const std::string& type,std::vector<ExprValue> values) {
    if(values.size()>static_cast<size_t>(INT32_MAX))throw DbError("54000","array size exceeds the maximum allowed");
    std::vector<DatumArrayDimension> dims;
    if(!values.empty())dims.push_back({static_cast<int32_t>(values.size()),1});
    ExprValue result(type,render(values,dims),false);
    attach(result,std::move(values),std::move(dims));return result;
}

} // namespace dbms::compound_value_detail
