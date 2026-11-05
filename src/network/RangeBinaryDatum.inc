// Actual builtin scalar ranges use their existing typed scalar representation.
// The shared subtype decoder validates finite bounds and preserves unbounded
// flags, inclusive endpoints and the actual date/timestamp infinity sentinels.
bool encodeBinaryRangeDatum(const std::string& value,
    const analyzed_statistics::range_input::BuiltinRange& type,std::vector<uint8_t>& encoded) {
    namespace input=analyzed_statistics::range_input;
    const auto range=input::decode(type,value,false,0,0,true);
    if(range.empty){encoded.push_back(1);return true;}
    const uint8_t flags=(range.lower.inclusive?2:0)|(range.upper.inclusive?4:0)|
        (range.lower.unbounded?8:0)|(range.upper.unbounded?16:0);
    encoded.push_back(flags);
    for(const auto* bound:{&range.lower,&range.upper}) {
        if(bound->unbounded)continue;
        std::vector<uint8_t> bytes;
        if(type.subtype==1700) {
            if(!encodePostgresNumeric(bound->text,bytes))return false;
        } else {
            const auto key=input::integerKey(type,*bound);
            if(type.subtype==23 || type.subtype==1082)appendRawUInt32(bytes,static_cast<uint32_t>(key));
            else appendRawUInt64(bytes,static_cast<uint64_t>(key));
        }
        if(bytes.size()>INT32_MAX)return false;
        appendRawUInt32(encoded,static_cast<uint32_t>(bytes.size()));encoded.insert(encoded.end(),bytes.begin(),bytes.end());
    }
    return true;
}
