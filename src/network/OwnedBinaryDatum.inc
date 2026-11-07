// Actual owning datums and immutable declaration metadata only. There is no
// catalog/search_path lookup, text-to-record reconstruction or execution owner.
// An iterative stack avoids consuming the C++ stack for nested records/arrays.
bool encodeOwnedBinaryDatum(const ExprValue& root,const DatumType& declaration,
                            const PgColumnDescription& presentation,
                            std::vector<uint8_t>& encoded) {
    constexpr size_t absent=std::numeric_limits<size_t>::max();
    const auto maximum=static_cast<size_t>(std::numeric_limits<int32_t>::max());
    struct Frame {
        const ExprValue* value;
        const DatumType* declaration;
        const DatumType* layout=nullptr;
        const std::vector<ExprValue>* children=nullptr;
        size_t index=0,lengthOffset=absent;
        bool started=false;
    };
    std::vector<Frame> pending{{&root,&declaration}};
    // Reuse a single scalar codec buffer; existing scalar encoders may assign
    // rather than append. Never let that overwrite a parent's wire header.
    std::vector<uint8_t> scalarBytes;
    const auto finish=[&]() {
        const auto offset=pending.back().lengthOffset;
        if(offset!=absent) {
            const size_t size=encoded.size()-offset-4;
            if(size>maximum)return false;
            const auto length=static_cast<uint32_t>(size);
            for(size_t i=0;i<4;++i)
                encoded[offset+i]=static_cast<uint8_t>(length>>(24-8*i));
        }
        pending.pop_back();return true;
    };
    while(!pending.empty()) {
        if(encoded.size()>maximum)return false;
        auto& frame=pending.back();
        if(!frame.started) {
            frame.started=true;
            if(frame.value->isNull || !frame.declaration->codecOid || !frame.declaration->hasBinaryOutput)return false;
            frame.layout=frame.declaration;
            if(frame.declaration->codecOid==2277) {
                const auto actual=frame.value->compound?frame.value->compound->type:DatumTypePtr{};
                if(frame.declaration->kind!=DatumKind::Array || !actual ||
                   actual->actualOid!=2277 || actual->codecOid!=2277 || actual->kind!=DatumKind::Array ||
                   !actual->hasBinaryOutput || !actual->arrayElement)return false;
                // New output-only arrays must not opt an enum root or
                // array-of-enum into the paused R151 wire family. Records
                // retain the existing independent field-output boundary.
                const DatumType* element=actual->arrayElement.get();size_t depth=0;
                while(element && element->kind==DatumKind::Array) {
                    if(++depth>128)return false;
                    element=element->arrayElement.get();
                }
                if(!element || (element->kind==DatumKind::Scalar && element->enumType))return false;
                frame.layout=actual.get();
            }
            if(frame.declaration->kind==DatumKind::Record) {
                const auto actual=frame.value->compound?frame.value->compound->type:DatumTypePtr{};
                if(frame.declaration->codecOid==2249) {
                    if(actual && actual->kind==DatumKind::Record)frame.layout=actual.get();
                } else {
                    // A specific named tuple/domain cannot silently inherit an
                    // unrelated ROW layout merely because both are records.
                    if(!actual || actual->kind!=DatumKind::Record ||
                       actual->codecOid!=frame.declaration->codecOid ||
                       actual->recordFields.size()!=frame.declaration->recordFields.size())return false;
                    for(size_t i=0;i<actual->recordFields.size();++i) {
                        const auto& have=actual->recordFields[i];
                        const auto& want=frame.declaration->recordFields[i];
                        if(have.actualOid!=want.actualOid || have.codecOid!=want.codecOid ||
                           have.kind!=want.kind || have.enumType!=want.enumType)return false;
                    }
                }
            }
            const auto& type=*frame.layout;
            if(type.kind==DatumKind::Scalar) {
                scalarBytes.clear();
                if(type.codecOid==2206 || type.codecOid==2205) {
                    if(!frame.value->objectOid)return false;
                    appendRawUInt32(scalarBytes,*frame.value->objectOid);
                } else if(type.codecOid==705 || type.codecOid==114) {
                    // unknownsend/json_send carry the actual datum bytes.
                    scalarBytes.assign(frame.value->value.begin(),frame.value->value.end());
                } else {
                    PgColumnDescription scalar;
                    scalar.typeOid=type.codecOid;scalar.enumType=type.enumType;
                    scalar.moneyLocale=presentation.moneyLocale;
                    if(!encodeBinaryValue(frame.value->value,scalar,scalarBytes))return false;
                }
                if(scalarBytes.size()>maximum-encoded.size())return false;
                encoded.insert(encoded.end(),scalarBytes.begin(),scalarBytes.end());
                if(!finish())return false;
                continue;
            }
            if(type.kind==DatumKind::Record) {
                if(!frame.value->recordFields ||
                   frame.value->recordFields->size()!=type.recordFields.size() ||
                   type.recordFields.size()>maximum)return false;
                frame.children=&*frame.value->recordFields;
                appendRawUInt32(encoded,static_cast<uint32_t>(frame.children->size()));
            } else {
                const auto& data=frame.value->compound;
                if(!data || !data->arrayElements || !type.arrayElement ||
                   !type.arrayElement->actualOid || !type.arrayElement->hasBinaryOutput ||
                   data->dimensions.size()>6)return false;
                // A non-NULL array resolves its element output role before
                // visiting elements, including an empty or all-NULL array.
                size_t count=data->dimensions.empty()?0:1;
                for(const auto& dimension:data->dimensions) {
                    if(dimension.length<=0 || count>maximum/static_cast<size_t>(dimension.length))return false;
                    const int64_t upper=static_cast<int64_t>(dimension.lower)+dimension.length-1;
                    if(upper<INT32_MIN || upper>INT32_MAX)return false;
                    count*=static_cast<size_t>(dimension.length);
                }
                if(count!=data->arrayElements->size())return false;
                frame.children=&*data->arrayElements;
                const bool nullItem=std::any_of(frame.children->begin(),frame.children->end(),
                    [](const ExprValue& value){return value.isNull;});
                const bool bitmap=data->arrayNullBitmap.value_or(nullItem);
                if(nullItem && !bitmap)return false;
                appendRawUInt32(encoded,static_cast<uint32_t>(data->dimensions.size()));
                appendRawUInt32(encoded,bitmap?1:0);
                appendRawUInt32(encoded,type.arrayElement->actualOid);
                for(const auto& dimension:data->dimensions) {
                    appendRawUInt32(encoded,static_cast<uint32_t>(dimension.length));
                    appendRawUInt32(encoded,static_cast<uint32_t>(dimension.lower));
                }
            }
        }
        if(frame.index==frame.children->size()) {
            if(!finish())return false;
            continue;
        }
        const size_t index=frame.index++;
        const DatumType* childType;
        if(frame.layout->kind==DatumKind::Record) {
            childType=&frame.layout->recordFields[index];
            if(!childType->actualOid)return false;
            // NULL record fields still emit their actual declaration OID,
            // but do not request or invoke that field's output codec.
            appendRawUInt32(encoded,childType->actualOid);
        } else childType=frame.layout->arrayElement.get();
        const auto* child=&frame.children->at(index);
        if(child->isNull) {appendRawUInt32(encoded,UINT32_MAX);continue;}
        const size_t lengthOffset=encoded.size();appendRawUInt32(encoded,0);
        pending.push_back({child,childType,nullptr,nullptr,0,lengthOffset,false});
    }
    return encoded.size()<=maximum;
}

bool encodeOwnedBinaryValue(const ExprValue& value,const PgColumnDescription& column,
                            std::vector<uint8_t>& encoded) {
    const DatumType* declaration=column.typeMetadata.get();
    if(!declaration && value.compound && value.compound->type) {
        const auto& type=*value.compound->type;
        // A top-level domain may have the base wire OID. Actual nested
        // declarations and array element OIDs remain in their owning payload.
        if(column.typeOid!=type.actualOid && column.typeOid!=type.codecOid)return false;
        declaration=&type;
    }
    if(declaration) {
        if(!declaration->actualOid || !declaration->codecOid ||
           (column.typeOid!=declaration->actualOid && column.typeOid!=declaration->codecOid))return false;
        // A top-level primitive needs no compound traversal stack or scalar
        // staging vector. Keep the existing scalar destination and locale.
        if(declaration->kind==DatumKind::Scalar && encoded.empty()) {
            if(value.isNull || !declaration->hasBinaryOutput)return false;
            if(declaration->codecOid==2206 || declaration->codecOid==2205) {
                if(!value.objectOid)return false;
                appendRawUInt32(encoded,*value.objectOid);return true;
            }
            if(declaration->codecOid==705 || declaration->codecOid==114) {
                encoded.assign(value.value.begin(),value.value.end());
                return encoded.size()<=static_cast<size_t>(INT32_MAX);
            }
            if(column.typeOid==declaration->codecOid && column.enumType==declaration->enumType)
                return encodeBinaryValue(value.value,column,encoded);
            PgColumnDescription scalar;
            scalar.typeOid=declaration->codecOid;scalar.enumType=declaration->enumType;
            scalar.moneyLocale=column.moneyLocale;
            return encodeBinaryValue(value.value,scalar,encoded);
        }
        return encodeOwnedBinaryDatum(value,*declaration,column,encoded);
    }
    if((column.typeOid==2206 || column.typeOid==2205) && value.objectOid) {
        appendRawUInt32(encoded,*value.objectOid);return true;
    }
    // Preserve the existing primitive path for producers with no structural
    // datum. Required compound payloads have no text reconstruction fallback.
    if(column.typeOid==2249 || column.typeOid==2287)return false;
    return encodeBinaryValue(value.value,column,encoded);
}
