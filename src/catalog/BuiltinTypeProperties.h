#pragma once
#include "catalog/systables.h"
#include "common/DbError.h"
#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>

namespace dbms::builtin_type_properties {
// Physical declarations for the already owned builtins, from PostgreSQL 18.6
// pg_type.dat (724edf9bde9d356724ad384a2e196edc3c9f80f7). Array alignment is
// at least int alignment, as in genbki.pl. These catalog fields do not add a
// PostgreSQL heap layout, TOAST storage, or an unimplemented type codec.
inline constexpr char version[]="DBMS_BUILTIN_TYPE_PROPERTIES1\n";
struct Property {Oid oid;const char* name;int16_t size;bool byval;char align,storage;Oid collation;};
inline constexpr std::array<Property,110> properties{{
    {16,"bool",1,true,'c','p',0},
    {17,"bytea",-1,false,'i','x',0},
    {18,"char",1,true,'c','p',0},
    {19,"name",64,false,'c','p',950},
    {20,"int8",8,sizeof(uintptr_t)>=8,'d','p',0},
    {21,"int2",2,true,'s','p',0},
    {23,"int4",4,true,'i','p',0},
    {24,"regproc",4,true,'i','p',0},
    {25,"text",-1,false,'i','x',100},
    {26,"oid",4,true,'i','p',0},
    {27,"tid",6,false,'s','p',0},
    {28,"xid",4,true,'i','p',0},
    {29,"cid",4,true,'i','p',0},
    {30,"oidvector",-1,false,'i','p',0},
    {114,"json",-1,false,'i','x',0},
    {142,"xml",-1,false,'i','x',0},
    {143,"_xml",-1,false,'i','x',0},
    {199,"_json",-1,false,'i','x',0},
    {600,"point",16,false,'d','p',0},
    {601,"lseg",32,false,'d','p',0},
    {602,"path",-1,false,'d','x',0},
    {603,"box",32,false,'d','p',0},
    {604,"polygon",-1,false,'d','x',0},
    {628,"line",24,false,'d','p',0},
    {629,"_line",-1,false,'d','x',0},
    {650,"cidr",-1,false,'i','m',0},
    {651,"_cidr",-1,false,'i','x',0},
    {700,"float4",4,true,'i','p',0},
    {701,"float8",8,sizeof(uintptr_t)>=8,'d','p',0},
    {705,"unknown",-2,false,'c','p',0},
    {718,"circle",24,false,'d','p',0},
    {719,"_circle",-1,false,'d','x',0},
    {774,"macaddr8",8,false,'i','p',0},
    {775,"_macaddr8",-1,false,'i','x',0},
    {790,"money",8,sizeof(uintptr_t)>=8,'d','p',0},
    {791,"_money",-1,false,'d','x',0},
    {829,"macaddr",6,false,'i','p',0},
    {869,"inet",-1,false,'i','m',0},
    {1000,"_bool",-1,false,'i','x',0},
    {1001,"_bytea",-1,false,'i','x',0},
    {1002,"_char",-1,false,'i','x',0},
    {1003,"_name",-1,false,'i','x',950},
    {1005,"_int2",-1,false,'i','x',0},
    {1007,"_int4",-1,false,'i','x',0},
    {1009,"_text",-1,false,'i','x',100},
    {1014,"_bpchar",-1,false,'i','x',100},
    {1015,"_varchar",-1,false,'i','x',100},
    {1016,"_int8",-1,false,'d','x',0},
    {1017,"_point",-1,false,'d','x',0},
    {1018,"_lseg",-1,false,'d','x',0},
    {1019,"_path",-1,false,'d','x',0},
    {1020,"_box",-1,false,'d','x',0},
    {1021,"_float4",-1,false,'i','x',0},
    {1022,"_float8",-1,false,'d','x',0},
    {1027,"_polygon",-1,false,'d','x',0},
    {1028,"_oid",-1,false,'i','x',0},
    {1040,"_macaddr",-1,false,'i','x',0},
    {1041,"_inet",-1,false,'i','x',0},
    {1042,"bpchar",-1,false,'i','x',100},
    {1043,"varchar",-1,false,'i','x',100},
    {1082,"date",4,true,'i','p',0},
    {1083,"time",8,sizeof(uintptr_t)>=8,'d','p',0},
    {1114,"timestamp",8,sizeof(uintptr_t)>=8,'d','p',0},
    {1115,"_timestamp",-1,false,'d','x',0},
    {1182,"_date",-1,false,'i','x',0},
    {1183,"_time",-1,false,'d','x',0},
    {1184,"timestamptz",8,sizeof(uintptr_t)>=8,'d','p',0},
    {1185,"_timestamptz",-1,false,'d','x',0},
    {1186,"interval",16,false,'d','p',0},
    {1187,"_interval",-1,false,'d','x',0},
    {1231,"_numeric",-1,false,'i','x',0},
    {1266,"timetz",12,false,'d','p',0},
    {1270,"_timetz",-1,false,'d','x',0},
    {1560,"bit",-1,false,'i','x',0},
    {1561,"_bit",-1,false,'i','x',0},
    {1562,"varbit",-1,false,'i','x',0},
    {1563,"_varbit",-1,false,'i','x',0},
    {1700,"numeric",-1,false,'i','m',0},
    {2205,"regclass",4,true,'i','p',0},
    {2206,"regtype",4,true,'i','p',0},
    {2210,"_regclass",-1,false,'i','x',0},
    {2211,"_regtype",-1,false,'i','x',0},
    {2249,"record",-1,false,'d','x',0},
    {2277,"anyarray",-1,false,'d','x',0},
    {2278,"void",4,true,'i','p',0},
    {2287,"_record",-1,false,'d','x',0},
    {2950,"uuid",16,false,'c','p',0},
    {2951,"_uuid",-1,false,'i','x',0},
    {3614,"tsvector",-1,false,'i','x',0},
    {3615,"tsquery",-1,false,'i','p',0},
    {3643,"_tsvector",-1,false,'i','x',0},
    {3645,"_tsquery",-1,false,'i','x',0},
    {3802,"jsonb",-1,false,'i','x',0},
    {3807,"_jsonb",-1,false,'i','x',0},
    {3904,"int4range",-1,false,'i','x',0},
    {3905,"_int4range",-1,false,'i','x',0},
    {3906,"numrange",-1,false,'i','x',0},
    {3907,"_numrange",-1,false,'i','x',0},
    {3908,"tsrange",-1,false,'d','x',0},
    {3909,"_tsrange",-1,false,'d','x',0},
    {3910,"tstzrange",-1,false,'d','x',0},
    {3911,"_tstzrange",-1,false,'d','x',0},
    {3912,"daterange",-1,false,'i','x',0},
    {3913,"_daterange",-1,false,'i','x',0},
    {3926,"int8range",-1,false,'d','x',0},
    {3927,"_int8range",-1,false,'d','x',0},
    {4089,"regnamespace",4,true,'i','p',0},
    {4090,"_regnamespace",-1,false,'i','x',0},
    {4096,"regrole",4,true,'i','p',0},
    {4097,"_regrole",-1,false,'i','x',0},
}};
inline bool published(const std::filesystem::path& directory) {
    const auto path=directory/".builtin_type_properties_version";std::error_code error;
    const auto status=std::filesystem::symlink_status(path,error);
    if(error==std::errc::no_such_file_or_directory || (!error && !std::filesystem::exists(status)))return false;
    if(error)throw DbError("58030","cannot inspect builtin type property version");
    if(!std::filesystem::is_regular_file(status))throw DbError("XX001","invalid builtin type property version");
    std::ifstream input(path,std::ios::binary);if(!input)throw DbError("58030","cannot read builtin type property version");
    const std::string bytes((std::istreambuf_iterator<char>(input)),{});
    if(input.bad() || bytes!=version)throw DbError("XX001","invalid builtin type property version");
    return true;
}
inline bool equal(const PgTypeRow& a,const PgTypeRow& b) {
    return a.typbyval==b.typbyval && a.typalign==b.typalign &&
        a.typstorage==b.typstorage && a.typcollation==b.typcollation;
}
inline void copy(PgTypeRow& target,const PgTypeRow& source) {
    target.typbyval=source.typbyval;target.typalign=source.typalign;
    target.typstorage=source.typstorage;target.typcollation=source.typcollation;
}
} // namespace dbms::builtin_type_properties
