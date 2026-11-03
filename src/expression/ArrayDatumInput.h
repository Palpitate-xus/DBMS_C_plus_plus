#pragma once

#include "expression/CompoundValueOps.h"
#include "catalog/catalog.h"
#include "commands/TableManage.h"
#include <map>

namespace dbms::compound_value_detail {

// This is a decoder for an actual declared physical storage input. It is not
// a fallback for a rendered expression result. Callers must establish that
// provenance before calling it. Actual owning values are never reparsed.
inline std::string storageElementType(const ExprValue& parent,const DatumType& element) {
    const auto parentType=common_type_detail::canonical(parent.typeName);
    if(common_type_detail::array(parentType))return parentType.substr(0,parentType.size()-2);
    // OID -> builtin spelling is a fixed identity lookup, never a typename ->
    // OID inference or catalog/search_path lookup. Domains use their base tag.
    static const auto names=[] {
        std::map<uint32_t,std::string> result;
        for(const auto& type:CatalogManager::builtinTypeRows())
            if(type.typnamespace==11)result.emplace(type.oid,type.oid==18?
                std::string("\"char\""):common_type_detail::canonical(type.typname));
        return result;
    }();
    const auto found=names.find(element.codecOid);
    return found==names.end()?(element.enumType?"enum":"unknown"):found->second;
}

inline bool decodeArrayStorageInput(ExprValue& input,const DatumTypePtr& declaration,
                                    std::optional<bool> storedBitmap={}) {
    if(input.isNull || owningElements(input))return true;
    if(!declaration || declaration->kind!=DatumKind::Array || !declaration->arrayElement ||
       !declaration->arrayElement->actualOid)return false;
    // Keep unsupported/invalid optional capture from replacing a real datum.
    ExprValue candidate=input;
    struct Job {ExprValue* value;DatumTypePtr type;std::optional<bool> bitmap;};
    std::vector<Job> pending{{&candidate,declaration,storedBitmap}};
    size_t inspected=0;
    try {
        while(!pending.empty()) {
            const auto job=std::move(pending.back());pending.pop_back();
            if(job.value->isNull || owningElements(*job.value))continue;
            if(!job.type || job.type->kind!=DatumKind::Array || !job.type->arrayElement ||
               !job.type->arrayElement->actualOid)return false;
            const auto literal=sql_array_text::parse(job.value->value);
            const auto dims=dimensions(literal);const auto count=elementCount(dims);
            const auto& element=*job.type->arrayElement;
            const auto elementName=storageElementType(*job.value,element);
            std::vector<ExprValue> values;values.reserve(count);
            bool unavailable=false;
            const auto flatten=[&](const auto& self,const sql_array_text::Literal& part,size_t depth)->void {
                for(const auto& token:part.elements) {
                    if((inspected++&127U)==0)checkForQueryInterrupt();
                    if(depth+1<dims.size()) {self(self,sql_array_text::parse(token),depth+1);if(unavailable)return;continue;}
                    const bool null=token=="NULL";
                    // A non-NULL record needs a genuine tuple producer or a
                    // real named input function. Its display is not a layout.
                    if(!null && element.kind==DatumKind::Record){unavailable=true;return;}
                    ExprValue value(elementName,null?std::string{}:unquote(token),null);
                    value.collation=job.value->collation;
                    if(!null && element.codecOid==2206) {
                        // REGTYPE display text cannot recover a stored OID.
                        // Only an already present real identity is admitted.
                        if(values.size()>=job.value->elementOids.size() ||
                           !job.value->elementOids[values.size()]){unavailable=true;return;}
                        value.objectOid=job.value->elementOids[values.size()];
                    }
                    values.push_back(std::move(value));
                }
            };
            flatten(flatten,literal,0);
            if(unavailable || values.size()!=count)return false;
            const bool nullItem=std::any_of(values.begin(),values.end(),[](const ExprValue& value){return value.isNull;});
            if(nullItem && job.bitmap && !*job.bitmap)return false;
            auto data=std::make_shared<CompoundDatum>();data->type=job.type;
            data->dimensions=dims;data->arrayElements=std::move(values);
            // Legacy storage contains only canonical array input text. NULL
            // presence proves a bitmap; NULL-free text cannot recover whether
            // an old intermediate value retained one. Keep that unknown.
            data->arrayNullBitmap=dims.empty()?std::optional<bool>(false):
                (nullItem?std::optional<bool>(true):job.bitmap);
            job.value->compound=data;
            if(element.kind==DatumKind::Array) {
                for(auto& value:*data->arrayElements)
                    if(!value.isNull)pending.push_back({&value,job.type->arrayElement,{}});
            }
        }
    } catch(const DbError& error) {
        // Optional storage capture must not add an input error for an unused
        // old text cell. Query interruption and programming/I/O failures keep
        // their original identity and propagate to the real owner cleanup.
        if(error.sqlState()=="22P02" || error.sqlState()=="2202E" || error.sqlState()=="54000")return false;
        throw;
    }
    input=std::move(candidate);return true;
}

} // namespace dbms::compound_value_detail
