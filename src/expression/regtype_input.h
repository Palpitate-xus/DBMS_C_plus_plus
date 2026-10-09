#pragma once
#include "catalog/CatalogService.h"
#include "catalog/declared_type.h"
#include "commands/TableManage.h"
#include "expression/common_type.h"
#include <charconv>
#include <limits>

namespace dbms::regtype_detail {
inline CatalogManager::MetadataSnapshot metadata(StorageEngine& engine,const std::string& database) {
    auto result=database.empty()?CatalogManager::MetadataSnapshot{}:engine.catalogService().metadataSnapshot(database);
    if(result.types.empty() && result.namespaces.empty()) {
        result.types=CatalogManager::builtinTypeRows();
        result.namespaces=CatalogManager::builtinNamespaceRows();
    }
    return result;
}
inline void validateCast(const std::string& input,const std::string& target) {
    if(common_type_detail::canonical(target)!="regtype")return;
    const auto source=common_type_detail::canonical(input);
    static const std::set<std::string> inputs={"unknown","text","varchar","bpchar","name","smallint","integer","bigint"};
    if(!inputs.count(source) && !common_type_detail::oidType(source))
        throw DbError("42846","cannot cast type "+input+" to regtype");
}
inline Oid numericOid(const std::string& input) {
    Oid value=0;
    const auto parsed=std::from_chars(input.data(),input.data()+input.size(),value);
    if(parsed.ec==std::errc::result_out_of_range)throw DbError("22003","OID out of range");
    if(parsed.ec!=std::errc{} || parsed.ptr!=input.data()+input.size())
        throw DbError("22P02","invalid input syntax for type oid");
    return value;
}
inline Oid inputOid(const std::string& input,const CatalogManager::MetadataSnapshot& catalog,const Session* session) {
    if(input=="-")return 0;
    if(!input.empty() && std::all_of(input.begin(),input.end(),[](unsigned char c){return c>='0' && c<='9';}))
        return numericOid(input);
    const auto tokens=SQLParser::tokenize(input);
    if(!tokens.empty() && !tokens.front().empty() && std::isdigit(static_cast<unsigned char>(tokens.front()[0])))
        throw DbError("42601","invalid type name syntax");
    const auto declaration=SQLParser::parseTypeSpecification(input);
    if(SQLParser::toLower(declaration.typeName)=="varchar" ||
       SQLParser::toLower(declaration.typeName)=="character varying")
        for(const auto& modifier:declaration.typeMods)
            if(!modifier.empty() && modifier.front()=='-')throw DbError("42601","invalid type modifier syntax");
    // The normal declaration grammar owns qualification, quoted names and
    // array decoration. Its type metadata lookup performs no execution/write.
    return resolveDeclaredTypeName(input,&catalog,session).typeOid;
}
inline std::string quoted(const std::string& name) {
    std::string result="\"";for(char c:name){result+=c;if(c=='\"')result+=c;}return result+'\"';
}
inline std::string outputImpl(Oid oid,const CatalogManager::MetadataSnapshot& catalog,const Session* session,
    std::set<Oid>& visiting) {
    if(!oid)return "-";
    if(!visiting.insert(oid).second)throw DbError("XX001","cyclic array type metadata");
    const auto found=std::find_if(catalog.types.begin(),catalog.types.end(),[&](const auto& row){return row.oid==oid;});
    if(found==catalog.types.end())return std::to_string(oid);
    if(found->typcategory=='A' && found->typelem) {
        const auto element=std::find_if(catalog.types.begin(),catalog.types.end(),[&](const auto& row){return row.oid==found->typelem;});
        if(element!=catalog.types.end() && element->typarray==oid)return outputImpl(element->oid,catalog,session,visiting)+"[]";
    }
    if(found->typnamespace==11) {
        static const std::map<std::string,std::string> aliases={
            {"bool","boolean"},{"int2","smallint"},{"int4","integer"},{"int8","bigint"},
            {"float4","real"},{"float8","double precision"},{"bpchar","character"},
            {"varchar","character varying"},{"varbit","bit varying"},
            {"timestamptz","timestamp with time zone"},{"timetz","time with time zone"},{"char","\"char\""}
        };
        if(const auto alias=aliases.find(found->typname);alias!=aliases.end())return alias->second;
        return found->typname;
    }
    const auto name=quoted(found->typname);
    try {if(resolveDeclaredTypeName(name,&catalog,session).typeOid==oid)return name;}catch(const DbError&) {}
    for(const auto& space:catalog.namespaces)if(space.oid==found->typnamespace)return quoted(space.nspname)+"."+name;
    throw DbError("XX001","type reference has no namespace metadata");
}
inline std::string output(Oid oid,const CatalogManager::MetadataSnapshot& catalog,const Session* session) {
    std::set<Oid> visiting;return outputImpl(oid,catalog,session,visiting);
}
} // namespace dbms::regtype_detail
