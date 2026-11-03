#pragma once
#include "common/DatumType.h"
#include "parser/ast.h"
#include <cstdint>
namespace dbms::compound_value_detail {
// Immutable PG REL_18_6 pg_cast.dat binary relationships. This classifies
// an already-admitted cast only; it adds no type/cast/catalog admission.
inline bool knownBuiltinArrayCastType(uint32_t oid) {
    switch(oid) {
    case 16:
    case 17:
    case 18:
    case 19:
    case 20:
    case 21:
    case 22:
    case 23:
    case 24:
    case 25:
    case 26:
    case 27:
    case 28:
    case 29:
    case 30:
    case 32:
    case 71:
    case 75:
    case 81:
    case 83:
    case 114:
    case 142:
    case 143:
    case 194:
    case 199:
    case 210:
    case 269:
    case 270:
    case 271:
    case 272:
    case 273:
    case 325:
    case 600:
    case 601:
    case 602:
    case 603:
    case 604:
    case 628:
    case 629:
    case 650:
    case 651:
    case 700:
    case 701:
    case 705:
    case 718:
    case 719:
    case 774:
    case 775:
    case 790:
    case 791:
    case 829:
    case 869:
    case 1000:
    case 1001:
    case 1002:
    case 1003:
    case 1005:
    case 1006:
    case 1007:
    case 1008:
    case 1009:
    case 1010:
    case 1011:
    case 1012:
    case 1013:
    case 1014:
    case 1015:
    case 1016:
    case 1017:
    case 1018:
    case 1019:
    case 1020:
    case 1021:
    case 1022:
    case 1027:
    case 1028:
    case 1033:
    case 1034:
    case 1040:
    case 1041:
    case 1042:
    case 1043:
    case 1082:
    case 1083:
    case 1114:
    case 1115:
    case 1182:
    case 1183:
    case 1184:
    case 1185:
    case 1186:
    case 1187:
    case 1231:
    case 1263:
    case 1266:
    case 1270:
    case 1560:
    case 1561:
    case 1562:
    case 1563:
    case 1700:
    case 1790:
    case 2201:
    case 2202:
    case 2203:
    case 2204:
    case 2205:
    case 2206:
    case 2207:
    case 2208:
    case 2209:
    case 2210:
    case 2211:
    case 2249:
    case 2275:
    case 2276:
    case 2277:
    case 2278:
    case 2279:
    case 2280:
    case 2281:
    case 2283:
    case 2287:
    case 2776:
    case 2949:
    case 2950:
    case 2951:
    case 2970:
    case 3115:
    case 3220:
    case 3221:
    case 3310:
    case 3361:
    case 3402:
    case 3500:
    case 3614:
    case 3615:
    case 3642:
    case 3643:
    case 3644:
    case 3645:
    case 3734:
    case 3735:
    case 3769:
    case 3770:
    case 3802:
    case 3807:
    case 3831:
    case 3838:
    case 3904:
    case 3905:
    case 3906:
    case 3907:
    case 3908:
    case 3909:
    case 3910:
    case 3911:
    case 3912:
    case 3913:
    case 3926:
    case 3927:
    case 4072:
    case 4073:
    case 4089:
    case 4090:
    case 4096:
    case 4097:
    case 4191:
    case 4192:
    case 4451:
    case 4532:
    case 4533:
    case 4534:
    case 4535:
    case 4536:
    case 4537:
    case 4538:
    case 4600:
    case 4601:
    case 5017:
    case 5038:
    case 5039:
    case 5069:
    case 5077:
    case 5078:
    case 5079:
    case 5080:
    case 6150:
    case 6151:
    case 6152:
    case 6153:
    case 6155:
    case 6157:
        return true;
    default:return false;
    }
}
inline bool binaryBuiltinArrayElementCast(uint32_t source,uint32_t target) {
    switch(source) {
    case 23:return target==24 || target==26 || target==2202 || target==2203 || target==2204 || target==2205 || target==2206 || target==3734 || target==3769 || target==4089 || target==4096 || target==4191;
    case 24:return target==23 || target==26 || target==2202;
    case 25:return target==1042 || target==1043;
    case 26:return target==23 || target==24 || target==2202 || target==2203 || target==2204 || target==2205 || target==2206 || target==3734 || target==3769 || target==4089 || target==4096 || target==4191;
    case 142:return target==25 || target==1042 || target==1043;
    case 194:return target==25;
    case 650:return target==869;
    case 1043:return target==25 || target==1042;
    case 1560:return target==1562;
    case 1562:return target==1560;
    case 2202:return target==23 || target==24 || target==26;
    case 2203:return target==23 || target==26 || target==2204;
    case 2204:return target==23 || target==26 || target==2203;
    case 2205:return target==23 || target==26;
    case 2206:return target==23 || target==26;
    case 3361:return target==17;
    case 3402:return target==17;
    case 3734:return target==23 || target==26;
    case 3769:return target==23 || target==26;
    case 4089:return target==23 || target==26;
    case 4096:return target==23 || target==26;
    case 4191:return target==23 || target==26;
    case 5017:return target==17;
    default:return false;
    }
}
// Actual per-value declarations win. For a shape-only slice/COLLATE whose
// runtime payload legitimately has no type pointer, use the original bound
// source/ARRAY declaration; never resolve SQL, names, providers or frames.
inline DatumTypePtr originalArrayDeclaration(const ExprValue& value,const Expr* node) {
    if(value.compound && value.compound->type && value.compound->type->kind==DatumKind::Array)
        return value.compound->type;
    while(node) {
        if(const auto* column=dynamic_cast<const ColumnRefExpr*>(node))
            return column->binding?column->binding->typeMetadata:DatumTypePtr{};
        if(const auto* array=dynamic_cast<const ArrayExpr*>(node))return array->typeMetadata;
        if(const auto* unary=dynamic_cast<const UnaryOpExpr*>(node);
           unary && unary->op.rfind("COLLATE ",0)==0){node=unary->operand.get();continue;}
        if(const auto* binary=dynamic_cast<const BinaryOpExpr*>(node);
           binary && binary->op=="[:]"){node=binary->left.get();continue;}
        break;
    }
    return {};
}
} // namespace dbms::compound_value_detail
