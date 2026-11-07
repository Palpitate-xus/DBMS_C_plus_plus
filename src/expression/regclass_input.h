#pragma once
#include "expression/regtype_input.h"

namespace dbms::regclass_detail {
inline CatalogManager::QualifiedName parseName(const std::string& input) {
    std::vector<std::string> parts;
    size_t at=0;
    const auto whitespace=[&] {while(at<input.size() && std::isspace(static_cast<unsigned char>(input[at])))++at;};
    while(true) {
        whitespace();std::string part;
        if(at<input.size() && input[at]=='"') {
            ++at;bool closed=false;
            while(at<input.size()) {
                const char ch=input[at++];
                if(ch=='"') {
                    if(at<input.size() && input[at]=='"'){part+='"';++at;}
                    else {closed=true;break;}
                } else part+=ch;
            }
            if(!closed || part.empty())throw DbError("42602","invalid name syntax");
        } else {
            while(at<input.size() && input[at]!='.' && !std::isspace(static_cast<unsigned char>(input[at]))) {
                if(input[at]=='"')throw DbError("42602","invalid name syntax");
                part+=static_cast<char>(std::tolower(static_cast<unsigned char>(input[at++])));
            }
            if(part.empty())throw DbError("42602","invalid name syntax");
        }
        whitespace();parts.push_back(std::move(part));
        if(at==input.size())break;
        if(input[at++]!='.' || parts.size()==2)throw DbError("42602","invalid name syntax");
    }
    CatalogManager::QualifiedName result;result.name=parts.back();
    if(parts.size()==2)result.schema=parts.front();
    return result;
}
// A relation reference is an actual pg_class identity. Resolution uses one
// copied catalog generation and the caller's namespace, without creating any
// catalog rows or retaining catalog-owned pointers.
inline const PgClassRow* lookup(const CatalogManager::QualifiedName& name,
    const CatalogManager::MetadataSnapshot& catalog,const Session* session) {
    const auto visible=freezeDeclaredTypeNamespace(session);
    const auto schemas=name.schema.empty()?visible.schemas:std::vector<std::string>{name.schema};
    for(auto schema:schemas) {
        if(schema=="pg_temp")schema=visible.temporarySchema;
        const auto space=std::find_if(catalog.namespaces.begin(),catalog.namespaces.end(),
            [&](const auto& row){return row.nspname==schema;});
        if(space==catalog.namespaces.end())continue;
        const auto relation=std::find_if(catalog.relations.begin(),catalog.relations.end(),
            [&](const auto& row){return row.relnamespace==space->oid && row.relname==name.name;});
        if(relation!=catalog.relations.end())return &*relation;
    }
    return nullptr;
}
inline Oid inputOid(const std::string& input,const CatalogManager::MetadataSnapshot& catalog,
    const Session* session) {
    if(input=="-")return 0;
    if(!input.empty() && std::all_of(input.begin(),input.end(),
       [](unsigned char c){return c>='0' && c<='9';}))return regtype_detail::numericOid(input);
    const auto name=parseName(input);
    if(const auto* relation=lookup(name,catalog,session))return relation->oid;
    throw DbError("42P01","relation \""+input+"\" does not exist");
}
inline std::string output(Oid oid,const CatalogManager::MetadataSnapshot& catalog,
    const Session* session) {
    if(!oid)return "-";
    const auto found=std::find_if(catalog.relations.begin(),catalog.relations.end(),
        [&](const auto& row){return row.oid==oid;});
    if(found==catalog.relations.end())return std::to_string(oid);
    const auto name=SQLParser::quoteIdentifier(found->relname);
    CatalogManager::QualifiedName unqualified;unqualified.name=found->relname;
    if(const auto* visible=lookup(unqualified,catalog,session);visible && visible->oid==oid)return name;
    for(const auto& space:catalog.namespaces)if(space.oid==found->relnamespace)
        return SQLParser::quoteIdentifier(space.nspname)+"."+name;
    throw DbError("XX001","relation reference has no namespace metadata");
}
} // namespace dbms::regclass_detail
