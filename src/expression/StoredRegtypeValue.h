#pragma once
// The call site must already own genuine stored-column provenance. Display
// strings are never accepted as a source of persistent object identity here.
#include "expression/regtype_input.h"
#include "expression/CompoundValueOps.h"
#include "common/SqlArrayText.h"
#include <charconv>
#include <functional>
#include <limits>

namespace dbms::stored_regtype_detail {
inline bool columnType(const std::string& type) {
    return type == "regtype" || type == "regtype[]";
}
inline std::string tokenValue(const std::string& token) {
    if (token.empty() || token.front() != '"') return token;
    if (token.size() < 2 || token.back() != '"')
        throw DbError("22P02", "malformed regtype array token");
    std::string value;
    for (size_t i=1;i+1<token.size();++i) {
        char ch=token[i];
        if (ch=='\\') {
            if (++i+1>=token.size()) throw DbError("22P02", "malformed regtype array escape");
            ch=token[i];
        }
        value+=ch;
    }
    return value;
}
inline Oid rawOid(const std::string& raw) {
    Oid oid=0;
    const auto result=std::from_chars(raw.data(),raw.data()+raw.size(),oid);
    if (raw.empty() || result.ec!=std::errc{} || result.ptr!=raw.data()+raw.size())
        throw DbError("XX001", "invalid persisted regtype object identity");
    return oid;
}
// These callbacks transform only genuine leaf tokens, preserving bounds and
// dimension ordering. NULL tokens never invoke input/output functions.
inline std::string mapArray(const std::string& input,
                            const std::function<std::string(const std::string&)>& leaf) {
    auto array=sql_array_text::parse(input);
    for(auto& token:array.elements) {
        if(!token.empty() && token.front()=='{') token=mapArray(token,leaf);
        else if(token!="NULL") token=sql_array_text::quote(leaf(tokenValue(token)));
    }
    return sql_array_text::compose(array.elements,array.dimensions);
}
inline std::string input(const std::string& value,bool array,
                         const CatalogManager::MetadataSnapshot& catalog,
                         const Session* session) {
    const auto convert=[&](const std::string& token) {
        return std::to_string(regtype_detail::inputOid(token,catalog,session));
    };
    return array?mapArray(value,convert):convert(value);
}
inline std::string output(const std::string& raw,bool array,
                          const CatalogManager::MetadataSnapshot& catalog,
                          const Session* session) {
    const auto render=[&](const std::string& token) {
        return regtype_detail::output(rawOid(token),catalog,session);
    };
    return array?mapArray(raw,render):render(raw);
}
inline ExprValue load(const std::string& raw,bool array,bool isNull,
                      const CatalogManager::MetadataSnapshot& catalog,
                      const Session* session,DatumTypePtr declaration={}) {
    ExprValue value(array?"regtype[]":"regtype","",isNull);
    if(isNull)return value;
    if(!array) {
        const Oid oid=rawOid(raw);value.objectOid=oid;
        value.value=regtype_detail::output(oid,catalog,session);
        return value;
    }
    const auto parsed=sql_array_text::parse(raw);
    std::vector<ExprValue> elements;
    std::function<void(const sql_array_text::Literal&)> collect=[&](const auto& part) {
        for(const auto& token:part.elements) {
            if(!token.empty() && token.front()=='{')collect(sql_array_text::parse(token));
            else if(token=="NULL") {
                elements.emplace_back("regtype","",true);value.elementOids.emplace_back();
            } else {
                const Oid oid=rawOid(tokenValue(token));
                elements.emplace_back("regtype",regtype_detail::output(oid,catalog,session));
                elements.back().objectOid=oid;value.elementOids.emplace_back(oid);
            }
        }
    };
    collect(parsed);
    std::vector<DatumArrayDimension> dimensions;
    for(const auto& dimension:parsed.dimensions)dimensions.push_back({dimension.length,dimension.lower});
    value.value=output(raw,true,catalog,session);
    compound_value_detail::attach(value,std::move(elements),std::move(dimensions),std::move(declaration),{},true);
    return value;
}
// At the existing expression-to-storage boundary retain identity from the
// actual evaluated datum. Ordinary text input continues through input() once.
inline std::string storageText(const ExprValue& value) {
    if(value.isNull)return {};
    if(value.typeName=="regtype" && value.objectOid)return std::to_string(*value.objectOid);
    if(value.typeName!="regtype[]")return value.value;
    const auto* owned=compound_value_detail::owningElements(value);
    if(!owned)
        throw DbError("0A000","REGTYPE array storage requires actual owning elements");
    const auto& dimensions=value.compound->dimensions;
    if(compound_value_detail::elementCount(dimensions)!=owned->size())
        throw DbError("XX000","regtype array owning cardinality mismatch");
    std::vector<ExprValue> raw;raw.reserve(owned->size());
    for(const auto& child:*owned) {
        if(child.isNull)raw.emplace_back("regtype","",true);
        else {
            if(child.typeName!="regtype" || !child.objectOid)
                throw DbError("XX000","regtype array element lost its actual identity");
            raw.emplace_back("regtype",std::to_string(*child.objectOid),false);
        }
    }
    return compound_value_detail::render(raw,dimensions);
}
} // namespace dbms::stored_regtype_detail
