#pragma once
#include "catalog/catalog.h"
#include "catalog/type_registry.h"
#include "parser/parser.h"
#include "common/BuiltinDatumRoles.h"
#include "common/DbError.h"
#include "expression/ExprEvaluator.h"
#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>

namespace dbms {
// Private analysis helper. Its snapshot must outlive the factory; all returned
// descriptors/wrappers own their declarations and never retain this object.
class DatumMetadataFactory {
    const CatalogManager::MetadataSnapshot& snapshot_;
    std::function<Oid(const std::string&)> resolveDeclaration_;
    std::map<Oid,DatumTypePtr> types_;
    std::map<std::pair<Oid,int32_t>,DatumTypePtr> modifiers_;
    std::set<Oid> constructing_;
    std::recursive_mutex cacheMutex_;
    struct FieldKeyLess {
        bool operator()(const std::vector<DatumTypePtr>& left,
                        const std::vector<DatumTypePtr>& right) const {
            return std::lexicographical_compare(left.begin(),left.end(),right.begin(),right.end(),
                [](const auto& a,const auto& b){return std::less<const DatumType*>{}(a.get(),b.get());});
        }
    };
    std::map<std::vector<DatumTypePtr>,std::shared_ptr<const CompoundDatum>,FieldKeyLess> records_;

    const PgTypeRow* find(Oid oid) const {
        const auto found=std::find_if(snapshot_.types.begin(),snapshot_.types.end(),
            [&](const auto& row){return row.oid==oid;});
        return found==snapshot_.types.end()?nullptr:&*found;
    }
    static const PgTypeRow* builtin(Oid oid) {
        const auto& rows=CatalogManager::builtinTypeRows();
        const auto found=std::find_if(rows.begin(),rows.end(),
            [&](const auto& row){return row.oid==oid && row.typnamespace==11;});
        return found==rows.end()?nullptr:&*found;
    }
    static bool genuineBuiltin(const PgTypeRow& row) {
        const auto* identity=builtin(row.oid);
        return identity && row.typnamespace==11 && row.typname==identity->typname &&
            row.typtype==identity->typtype;
    }
public:
    DatumMetadataFactory(const CatalogManager::MetadataSnapshot& snapshot,
        std::function<Oid(const std::string&)> resolver={})
        :snapshot_(snapshot),resolveDeclaration_(std::move(resolver)) {}

    DatumTypePtr type(Oid actualOid) {
        std::lock_guard<std::recursive_mutex> lock(cacheMutex_);
        if(!actualOid)return {};
        if(const auto found=types_.find(actualOid);found!=types_.end())return found->second;
        if(!constructing_.insert(actualOid).second)
            throw DbError("XX001","cyclic datum declaration metadata");
        struct Guard {std::set<Oid>& active;Oid oid;~Guard(){active.erase(oid);}} guard{constructing_,actualOid};
        auto result=std::make_shared<DatumType>();
        result->actualOid=actualOid;
        const auto* actual=find(actualOid);
        if(!actual) {
            // Preserve a genuine actual field OID even when its catalog role
            // is missing. NULL record fields can still emit their real header;
            // a non-NULL value/array element must fail codec availability.
            types_.emplace(actualOid,result);
            return result;
        }
        const auto* base=actual;
        std::set<Oid> domains;
        while(base->typtype=='d') {
            if(!domains.insert(base->oid).second || !base->typbasetype)
                throw DbError("XX001","invalid datum domain ancestry");
            if(result->effectiveTypeMod<0 && base->typtypmod>=0)
                result->effectiveTypeMod=base->typtypmod;
            base=find(base->typbasetype);
            if(!base) {
                types_.emplace(actualOid,result);
                return result;
            }
        }
        result->codecOid=base->oid;
        result->codecSize=base->typlen;
        const char* role=genuineBuiltin(*base)?builtinDatumBinaryRole(base->oid):nullptr;
        // Actual array_send identity is sufficient for genuine catalog arrays.
        // A user basename/category by itself never selects a codec role.
        const bool arrayRole=(role && (std::string(role)=="array_send" ||
            std::string(role)=="anyarray_send")) ||
            (!role && base->typsend==2401 && base->typelem && base->typlen==-1);
        if(arrayRole) {
            result->kind=DatumKind::Array;
            result->hasBinaryOutput=true;
            if(base->typelem)result->arrayElement=type(base->typelem);
        } else if(base->typtype=='c' || (genuineBuiltin(*base) && base->oid==2249)) {
            result->kind=DatumKind::Record;
            result->hasBinaryOutput=true;
            if(base->typtype=='c') {
                std::vector<const PgAttributeRow*> fields;
                for(const auto& field:snapshot_.attributes)
                    if(field.attrelid==base->typrelid && field.attnum>0 && !field.attisdropped)
                        fields.push_back(&field);
                std::sort(fields.begin(),fields.end(),[](const auto* a,const auto* b){return a->attnum<b->attnum;});
                for(const auto* field:fields) {
                    const auto declaration=attribute(*field);
                    if(!declaration) {
                        DatumType missing;missing.actualOid=field->atttypid;
                        result->recordFields.push_back(std::move(missing));
                    } else result->recordFields.push_back(*declaration);
                }
            }
        } else {
            result->kind=DatumKind::Scalar;
            result->enumType=base->typtype=='e';
            result->hasBinaryOutput=result->enumType || role;
        }
        types_.emplace(actualOid,result);
        return result;
    }
    DatumTypePtr modifier(DatumTypePtr declaration,int32_t typeMod) {
        std::lock_guard<std::recursive_mutex> lock(cacheMutex_);
        if(!declaration || typeMod<0 || declaration->effectiveTypeMod==typeMod)return declaration;
        const auto key=std::make_pair(declaration->actualOid,typeMod);
        if(const auto found=modifiers_.find(key);found!=modifiers_.end())return found->second;
        auto result=std::make_shared<DatumType>(*declaration);
        result->effectiveTypeMod=typeMod;
        modifiers_.emplace(key,result);return result;
    }
    DatumTypePtr declaration(const std::string& spelling,Oid actualOid) {
        std::lock_guard<std::recursive_mutex> lock(cacheMutex_);
        if(!actualOid && resolveDeclaration_)actualOid=resolveDeclaration_(spelling);
        auto result=type(actualOid);
        if(!result || !result->codecOid)return {};
        if(spelling.empty())return result;
        const auto syntax=SQLParser::parseTypeSpecification(spelling);
        if(syntax.typeMods.empty())return result;
        const auto modifierType=result->kind==DatumKind::Array?result->arrayElement:result;
        const auto* codec=modifierType?find(modifierType->codecOid):nullptr;
        if(codec && genuineBuiltin(*codec)) {
            const auto adjusted=TypeRegistry::instance().applyTypeMods(codec->typname,syntax.typeMods);
            if(adjusted.ok())result=modifier(std::move(result),adjusted.typmod);
        }
        return result;
    }
    DatumTypePtr attribute(const PgAttributeRow& attribute) {
        std::lock_guard<std::recursive_mutex> lock(cacheMutex_);
        const auto* actual=find(attribute.atttypid);
        auto result=type(attribute.atttypid);
        if(attribute.attndims>0 && actual && actual->typtype=='d' &&
            (actual->typndims>0 || (result && result->kind==DatumKind::Array))) {
            // Legacy domain-over-array metadata cannot distinguish its own
            // envelope from an extra array-of-domain envelope. Its catalog
            // defect is an ordinary dependency, not a license to guess D[].
            return {};
        }
        if(attribute.attndims>0 && result && result->kind!=DatumKind::Array && actual) {
            // Existing storage may use scalar atttypid+attndims. Follow only
            // the actual copied backlink and verified element identity. Never
            // synthesize a pair, repair a domain, or seed an enum array (R151).
            const auto* array=find(actual->typarray);
            if(!array || array->typelem!=actual->oid)return {};
            result=type(array->oid);
            if(!result || result->kind!=DatumKind::Array)return {};
        }
        if(!result || !result->codecOid)return {};
        return modifier(std::move(result),attribute.atttypmod);
    }
    std::shared_ptr<const CompoundDatum> record(const std::vector<DatumTypePtr>& fields) {
        std::lock_guard<std::recursive_mutex> lock(cacheMutex_);
        std::vector<DatumTypePtr> key;key.reserve(fields.size());
        for(const auto& field:fields) {if(!field)return {};key.push_back(field);}
        if(const auto found=records_.find(key);found!=records_.end())return found->second;
        const auto identity=type(2249);
        if(!identity || identity->kind!=DatumKind::Record)return {};
        auto declaration=std::make_shared<DatumType>(*identity);
        declaration->recordFields.clear();declaration->recordFields.reserve(fields.size());
        for(const auto& field:fields)declaration->recordFields.push_back(*field);
        auto result=std::make_shared<CompoundDatum>();result->type=std::move(declaration);
        records_.emplace(std::move(key),result);return result;
    }
};
} // namespace dbms
