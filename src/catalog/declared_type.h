#pragma once
#include "catalog/catalog.h"
#include "catalog/type_registry.h"
#include "catalog/systables.h"
#include "parser/parser.h"
#include "utils/Session.h"
#include "common/DbError.h"
#include <algorithm>

namespace dbms {
struct DeclaredTypeBinding {
    std::string typeName; // real result identity, without modifiers
    std::string inputType; // same identity plus the actual input modifiers
    Oid typeOid=INVALID_OID;
};

// A copied catalog generation owns named declarations; syntax aliases select
// catalog builtins. No routine/value/row is consulted to resolve a type.
inline DeclaredTypeBinding resolveDeclaredTypeName(const std::string& spelling,
    const CatalogManager::MetadataSnapshot* catalog,const Session* session) {
    auto declaration=SQLParser::parseTypeSpecification(spelling);
    const auto low=SQLParser::toLower(declaration.typeName);
    static const std::map<std::string,std::string> syntaxAliases={
        {"integer","int4"},{"int","int4"},{"smallint","int2"},{"bigint","int8"},
        {"boolean","bool"},{"real","float4"},{"double precision","float8"},
        {"character","bpchar"},{"char","bpchar"},{"character varying","varchar"},
        {"varchar","varchar"},{"bit","bit"},{"bit varying","varbit"},
        {"numeric","numeric"},{"decimal","numeric"},{"dec","numeric"},
        {"time","time"},{"timestamp","timestamp"}
    };
    CatalogManager::QualifiedName requested;
    const auto syntax=syntaxAliases.find(low);
    if(syntax!=syntaxAliases.end()) {requested.schema="pg_catalog";requested.name=syntax->second;}
    else if(!CatalogManager::parseQualifiedName(declaration.typeName,requested,true))
        throw DbError("42601","invalid declared type name");
    const auto quote=[](const std::string& name) {
        std::string result="\"";for(char c:name){result+=c;if(c=='\"')result+=c;}return result+'\"';
    };
    DeclaredTypeBinding result;
    const PgTypeRow* modifierOwner=nullptr;
    const auto ownModifiers=[&](const PgTypeRow& type,const std::vector<PgTypeRow>& types) {
        modifierOwner=&type;
        if(type.typcategory=='A' && type.typelem) {
            const auto element=std::find_if(types.begin(),types.end(),[&](const PgTypeRow& row){return row.oid==type.typelem;});
            if(element==types.end())throw DbError("42704","array element type does not exist");
            modifierOwner=&*element;
        }
    };
    const auto builtinInput=[&](const PgTypeRow& type,const std::vector<PgTypeRow>& types) {
        const PgTypeRow* element=&type;
        if(type.typcategory=='A' && type.typelem) {
            const auto found=std::find_if(types.begin(),types.end(),[&](const PgTypeRow& row) {
                return row.oid==type.typelem && row.typnamespace==11;
            });
            if(found==types.end())throw DbError("42704","array element type does not exist");
            element=&*found;
        }
        auto input=element->oid==18?std::string("\"char\""):
            TypeRegistry::instance().normalizeTypeName(element->typname);
        if(input.empty())input=element->typname;
        if(element!=&type)input+="[]";
        return input;
    };
    if(catalog) {
        std::vector<std::string> schemas;
        if(!requested.schema.empty())schemas.push_back(requested.schema);
        else {
            std::string canonical;
            if(!session || !parseSessionSearchPath(session->searchPath,schemas,canonical))schemas={"public"};
            for(auto& schema:schemas)schema=expandSessionSearchPathEntry(schema,session?session->username:"");
            if(std::find(schemas.begin(),schemas.end(),"pg_catalog")==schemas.end())schemas.insert(schemas.begin(),"pg_catalog");
            if(session)schemas.insert(schemas.begin(),sessionTempSchemaName(*session));
        }
        for(auto schema:schemas) {
            if(schema=="pg_temp" && session)schema=sessionTempSchemaName(*session);
            Oid namespaceOid=INVALID_OID;
            for(const auto& space:catalog->namespaces)if(space.nspname==schema){namespaceOid=space.oid;break;}
            if(!namespaceOid) {
                if(!requested.schema.empty())throw DbError("3F000","schema \""+schema+"\" does not exist");
                continue;
            }
            for(const auto& type:catalog->types)if(type.typnamespace==namespaceOid && type.typname==requested.name) {
                ownModifiers(type,catalog->types);
                result.typeOid=type.oid;
                result.typeName=schema=="pg_catalog"
                    ?builtinInput(type,catalog->types)
                    :quote(schema)+"."+quote(type.typname);
                if(result.typeName.empty())result.typeName=quote(schema)+"."+quote(type.typname);
                if(declaration.isArray) {
                    Oid arrayOid=type.typarray;
                    if(!arrayOid)for(const auto& candidate:catalog->types)
                        if(candidate.typnamespace==namespaceOid && candidate.typname=="_"+type.typname && candidate.typcategory=='A') {arrayOid=candidate.oid;break;}
                    if(!arrayOid)throw DbError("42704","array type for declared type does not exist");
                    result.typeOid=arrayOid;
                }
                break;
            }
            if(result.typeOid)break;
        }
    } else {
        // Only actual bootstrap physical identities are eligible. SQL aliases
        // were handled by grammar above; quoted/qualified aliases are not rows.
        // This immutable shared producer performs no catalog or filesystem I/O.
        if(!requested.schema.empty() && requested.schema!="pg_catalog")
            throw DbError("3F000","schema \""+requested.schema+"\" does not exist");
        const auto& types=CatalogManager::builtinTypeRows();
        for(const auto& type:types)if(type.typnamespace==11 && type.typname==requested.name) {
            ownModifiers(type,types);
            result.typeName=builtinInput(type,types);
            result.typeOid=type.oid;
            if(declaration.isArray) {
                if(!type.typarray)throw DbError("42704","array type for declared type does not exist");
                result.typeOid=type.typarray;
            }
            break;
        }
    }
    if(!result.typeOid)throw DbError("42704","type \""+declaration.typeName+"\" does not exist");
    if(!declaration.typeMods.empty()) {
        auto inputName=result.typeName;
        while(inputName.size()>=2 && inputName.compare(inputName.size()-2,2,"[]")==0)
            inputName.resize(inputName.size()-2);
        const auto* input=TypeRegistry::instance().findType(inputName);
        // The actual scalar/element owns modifier input. Absence from the
        // frontend codec registry must not grant arbitrary modifiers to
        // genuine named reference types, domains or other catalog rows.
        const bool accepts=input?input->hasTypeMod:modifierOwner && modifierOwner->typmodin!=INVALID_OID;
        if(!accepts)
            throw DbError("42601","type modifier is not allowed for type "+declaration.typeName);
    }
    result.inputType=result.typeName;
    if(!declaration.typeMods.empty()) {
        result.inputType+='(';
        for(size_t i=0;i<declaration.typeMods.size();++i) {
            if(!declaration.typeMods[i].empty() && declaration.typeMods[i].front()=='+')
                throw DbError("42601","type modifiers must be simple constants or identifiers");
            result.inputType+=(i?",":"")+declaration.typeMods[i];
        }
        result.inputType+=')';
    }
    if(declaration.isArray){result.typeName+="[]";result.inputType+="[]";}
    return result;
}
} // namespace dbms
