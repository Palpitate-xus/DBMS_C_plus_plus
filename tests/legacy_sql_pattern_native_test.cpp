#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "catalog/collation.h"
#include "common/DbError.h"
#include "test_utils.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <optional>

int main() {
    using namespace dbms;
    using Condition = StorageEngine::Condition;
    TypeRegistry::instance().bootstrap();
    const std::string db = testDbPath("legacy_sql_pattern_native");
    StorageEngine engine;
    assert(engine.createDatabase(db,"utf8") == DBStatus::OK);
    TableSchema schema; schema.tablename = "patterns";
    schema.append(makeIntColumn("id",false,4,true));
    for (const auto& name : {"v","p","b","c"}) {
        auto column = makeTextColumn(name,true);
        if (column.dataName == "b") column.dataType = "bytea";
        if (column.dataName == "c") { column.dataType = "char"; column.dsize = 3; }
        schema.append(column);
    }
    assert(engine.createTable(db,schema) == DBStatus::OK);
    const std::vector<std::optional<std::string>> values = {"é","É","a.c","NULL","",std::nullopt,"ab","a b","a'b","a\nb"};
    for (size_t i=0;i<values.size();++i) {
        std::optional<std::string> pattern = i==5 ? std::nullopt : std::optional<std::string>(i==6 ? "a\\" : "_");
        std::optional<std::string> bytes = i==5 ? std::nullopt : std::optional<std::string>(i<2 ? "\\xc3a9" : i==6 ? "\\x6162" : "\\x");
        std::optional<std::string> padded = i==5 ? std::nullopt : std::optional<std::string>(i==4 ? "" : "a");
        assert(engine.insertRow(db,"patterns",{{"id",std::to_string(i+1)},{"v",values[i]},{"p",pattern},{"b",bytes},{"c",padded}}) == DBStatus::OK);
    }
    std::vector<std::string> failures;
    const auto check = [&](const std::string& label,const auto& actual,const auto& expected) {
        if (actual != expected) { failures.push_back(label); std::cerr << "LEGACY_PATTERN_FAILURE " << label << '\n'; }
    };
    const auto query = [&](const std::string& condition) {
        auto rows=engine.query(db,"patterns",{condition},{"id"});
        std::vector<int> ids;
        for (const auto& row : rows) ids.push_back(std::stoi(row));
        std::sort(ids.begin(),ids.end()); return ids;
    };
    for (const auto& [condition,ids] : std::vector<std::pair<std::string,std::vector<int>>>{
        {"likev _",{1,2}}, {"notlikev _",{3,4,5,7,8,9,10}},
        {"ilikev é",{1,2}}, {"notilikev é",{3,4,5,7,8,9,10}},
        {"similarv _",{1,2}}, {"notsimilarv _",{3,4,5,7,8,9,10}},
        {"similarv a.c",{3}}, {"likev NULL",{4}}, {"likev ''",{5}},
        {"likev p",{}}, {"notlikev 'a b'",{1,2,3,4,5,7,9,10}},
        {"likeb __",{1,2,7}}, {"likec a",{}}, {"likec a__",{1,2,3,4,7,8,9,10}}
    }) {
        try { check(condition,query(condition),ids); }
        catch (const std::exception& error) { failures.push_back(condition); std::cerr << "LEGACY_PATTERN_EXCEPTION " << condition << ' ' << error.what() << '\n'; }
    }
    for (const std::string condition : {"ilikeb %","similarb %","likeid %"}) {
        std::string state;
        try { (void)query(condition); }
        catch (const DbError& error) { state=error.sqlState(); }
        check(condition+" type",state,std::string("42883"));
    }
    TableSchema buffered; buffered.tablename="buffered";
    buffered.append(makeTextColumn("v",true)); buffered.append(makeTextColumn("p",true));
    const auto row = [&](const std::string& value,const std::string& pattern) {
        std::string result(8,'\0');
        uint16_t offset=8,length=value.size();
        std::memcpy(result.data(),&offset,2); std::memcpy(result.data()+2,&length,2);
        offset+=length;length=pattern.size();
        std::memcpy(result.data()+4,&offset,2);std::memcpy(result.data()+6,&length,2);
        return result+value+pattern;
    };
    for (const auto& [value,pattern,expected] : std::vector<std::tuple<std::string,std::string,bool>>{
        {"é","_",true},{"É","é",false},{"NULL","NULL",true},{"","",true}}) {
        Condition condition{"like","v",pattern,true};
        check("bitmap literal "+value,StorageEngine::evalConditionOnRow(condition,row(value,pattern),buffered,{false,false}),expected);
        check("bitmap NULL "+value,StorageEngine::evalConditionOnRow(condition,row(value,pattern),buffered,{true,false}),false);
    }
    Condition columnPattern{"like","v","p"};
    check("bitmap rhs column Unicode",StorageEngine::evalConditionOnRow(columnPattern,row("é","_"),buffered,{false,false}),true);
    check("bitmap rhs NULL",StorageEngine::evalConditionOnRow(columnPattern,row("",""),buffered,{false,true}),false);
    check("bitmap rhs empty",StorageEngine::evalConditionOnRow(columnPattern,row("",""),buffered,{false,false}),true);
    check("bitmap RHS raw control-byte datum",StorageEngine::evalConditionOnRow(columnPattern,row("\1a","\1_"),buffered,{false,false}),true);
    Condition invalid{"like","v","a\\",true};
    check("short trailing",StorageEngine::evalConditionOnRow(invalid,row("a",""),buffered,{false,false}),false);
    std::string demanded;
    try { (void)StorageEngine::evalConditionOnRow(invalid,row("ab",""),buffered,{false,false}); }
    catch (const DbError& error) { demanded=error.sqlState(); }
    check("demanded trailing",demanded,std::string("22025"));
    using Truth=StorageEngine::PredicateTruth;
    const Column text=makeTextColumn("value",true);
    check("compareValues Unicode",StorageEngine::compareValues(text,"é",false,"_",false,"LIKE"),Truth::True);
    check("compareValues ILIKE",StorageEngine::compareValues(text,"É",false,"é",false,"ILIKE"),Truth::True);
    check("compareValues SIMILAR literal",StorageEngine::compareValues(text,"abc",false,"a.c",false,"SIMILAR TO"),Truth::False);
    check("compareValues NULL",StorageEngine::compareValues(text,"",true,"%",false,"NOT LIKE"),Truth::Unknown);
    auto cText=text;cText.collation="C";
    check("compareValues actual C",StorageEngine::compareValues(cText,"É",false,"é",false,"ILIKE"),Truth::False);
    auto utfText=text;utfText.collation="C.utf8";
    check("compareValues actual C.utf8",StorageEngine::compareValues(utfText,"É",false,"é",false,"ILIKE"),Truth::True);
    check("C.utf8 byte ordering",collation::compare("Z","a","C.utf8")<0,true);
    check("C.utf8 valid",collation::isValid("C.utf8"),true);
    TableSchema cBuffered=buffered;cBuffered.cols[0].collation="C";
    Condition cTyped{"typedexpr","","v ILIKE 'é'"};
    check("typed actual C",StorageEngine::evalConditionOnRow(cTyped,row("É",""),cBuffered,{false,false}),false);
    check("typed actual C NULL",StorageEngine::evalConditionOnRow(cTyped,row("É",""),cBuffered,{true,false}),false);
    cBuffered.cols[0].collation="C.utf8";
    check("typed actual C.utf8",StorageEngine::evalConditionOnRow(cTyped,row("É",""),cBuffered,{false,false}),true);
    TableSchema bytesBuffered=buffered;bytesBuffered.cols[0].dataType="bytea";
    Condition bytePattern{"like","v","__",true};
    check("BYTEA actual two bytes",StorageEngine::evalConditionOnRow(bytePattern,row("\\xc3a9",""),bytesBuffered,{false,false}),true);
    bytePattern.value="_";
    check("BYTEA underscore one byte",StorageEngine::evalConditionOnRow(bytePattern,row("\\xc3a9",""),bytesBuffered,{false,false}),false);
    bytePattern.value="";
    check("BYTEA empty",StorageEngine::evalConditionOnRow(bytePattern,row("\\x",""),bytesBuffered,{false,false}),true);
    check("BYTEA empty bitmap NULL",StorageEngine::evalConditionOnRow(bytePattern,row("\\x",""),bytesBuffered,{true,false}),false);
    auto arrayColumn=text;arrayColumn.isArray=true;
    std::string arrayState;
    try { (void)StorageEngine::compareValues(arrayColumn,"{é}",false,"%",false,"LIKE"); }
    catch (const DbError& error) { arrayState=error.sqlState(); }
    check("actual array type rejected",arrayState,std::string("42883"));
    for (const auto& [op,value,pattern,escape,expected] :
         std::vector<std::tuple<std::string,std::string,std::string,std::optional<std::string>,bool>>{
        {"similar","é","_",std::nullopt,true},{"notsimilar","中","_",std::nullopt,false},
        {"similar","éé","_{2}",std::nullopt,true},{"similar","abc","a.c",std::nullopt,false},
        {"similar","^a$","^a$",std::nullopt,true},{"similar","a_","aé_","é",true},
        {"similar","a%","aé%","é",true},{"similar","a\nb","a_b",std::nullopt,true},
        {"ilike","É","é",std::nullopt,true},{"ilike","É_","é#_","#",true},
        {"similar","éé","[é]{2}",std::nullopt,true},{"similar","é","(é|中)",std::nullopt,true},
        {"similar","a","[[:alpha:]]",std::nullopt,true},{"similar","é","[[:alpha:]]",std::nullopt,true},
        {"like","a\\","a\\","",true},{"like","a\1","a\1","",true}}) {
        Condition condition{op,"v",pattern,true}; condition.patternEscape=escape;
        check("direct stored "+op+" "+value,StorageEngine::evalConditionOnRow(condition,row(value,""),buffered,{false,false}),expected);
        check("direct stored NULL "+op+" "+value,StorageEngine::evalConditionOnRow(condition,row(value,""),buffered,{true,false}),false);
    }
    Condition escaped{"like","v","aé_",true}; escaped.patternEscape="é";
    check("explicit escape underscore",StorageEngine::evalConditionOnRow(escaped,row("a_",""),buffered,{false,false}),true);
    escaped.patternEscape.reset();escaped.patternEscapeColumn="p";
    check("column escape Unicode",StorageEngine::evalConditionOnRow(escaped,row("a_","é"),buffered,{false,false}),true);
    check("column escape NULL",StorageEngine::evalConditionOnRow(escaped,row("a_","é"),buffered,{false,true}),false);
    escaped.patternEscapeColumn.clear();escaped.patternEscape="xx";
    escaped.patternIsNull=true;
    check("NULL pattern unused invalid escape",StorageEngine::evalConditionOnRow(escaped,row("a_",""),buffered,{false,false}),false);
    for (const auto& [condition,state] : std::vector<std::pair<std::string,std::string>>{
        {"likev '_' ESCAPE 'xx'","22025"},{"similarv '['","2201B"},
        {"likev p ESCAPE id","42883"},{"likeb '_'::text","42883"}}) {
        std::string actual;
        try {
            std::vector<std::vector<std::string>> cells;std::vector<std::vector<bool>> nulls;
            (void)engine.query(db,"patterns",{condition},{"id"},{},false,false,false,0,{},&cells,&nulls);
        } catch (const DbError& error) { actual=error.sqlState(); }
        check("structured type/escape "+condition,actual,state);
    }
    const auto structured=[&](const std::string& condition) {
        std::vector<std::vector<std::string>> cells;std::vector<std::vector<bool>> nulls;
        (void)engine.query(db,"patterns",{condition},{"id"},{},false,false,false,0,{},&cells,&nulls);
        std::vector<int> ids;
        for (size_t i=0;i<cells.size();++i) { assert(!nulls[i][0]);ids.push_back(std::stoi(cells[i][0])); }
        std::sort(ids.begin(),ids.end());return ids;
    };
    for (const auto& [condition,ids] : std::vector<std::pair<std::string,std::vector<int>>>{
        {"likev NULL",{}},{"likev 'NULL'",{4}},{"likev ''",{5}},
        {"likev '_'",{1,2}},{"ilikev 'é'",{1,2}},
        {"similarv 'a.c'",{3}},{"similarv 'a_b'",{8,9,10}},
        {"similarv '(é|É)'",{1,2}},{"similarv '[éÉ]'",{1,2}},
        {"likev 'a# b' ESCAPE '#'",{8}},{"likev '_' ESCAPE NULL",{}},
        {"likev NULL ESCAPE 'xx'",{}},{"likeb '__'",{1,2,7}},
        {"likec 'a'::char(3)",{}},{"likec 'a__'",{1,2,3,4,7,8,9,10}}}) {
        try { check("structured "+condition,structured(condition),ids); }
        catch (const std::exception& error) { failures.push_back(condition);std::cerr<<"LEGACY_PATTERN_EXCEPTION "<<condition<<' '<<error.what()<<'\n'; }
    }
    StorageEngine::SelectExpr idExpression;idExpression.displayName="id";idExpression.colName="id";
    auto expressions=engine.queryExpr(db,"patterns",{"similarv '_'"},{idExpression});
    std::vector<int> expressionIds;
    for (const auto& value:expressions) expressionIds.push_back(std::stoi(value));
    std::sort(expressionIds.begin(),expressionIds.end());
    check("queryExpr actual compact",expressionIds,std::vector<int>({1,2}));
    assert(engine.createIndex(db,"patterns","v")==DBStatus::OK);
    check("indexed Unicode still heap pattern",query("likev _"),std::vector<int>({1,2}));
    check("index equality companion",structured("=id 1"),std::vector<int>({1}));

    TableSchema rhs;rhs.tablename="rhs";rhs.append(makeIntColumn("id",false,4,true));
    rhs.append(makeTextColumn("p",true));rhs.append(makeTextColumn("escape",true));
    assert(engine.createTable(db,rhs)==DBStatus::OK);
    const std::vector<std::optional<std::string>> patterns={"_","é","a.c","NULL","",std::nullopt,"ab","a_b","a'b","a_b"};
    for (size_t i=0;i<patterns.size();++i)
        assert(engine.insertRow(db,"rhs",{{"id",std::to_string(i+1)},{"p",patterns[i]},{"escape",std::optional<std::string>("é")}})==DBStatus::OK);
    const std::vector<int> joinedExpected={1,3,4,5,7,8,9,10};
    for (int kind=0;kind<5;++kind) {
        std::vector<std::vector<std::string>> cells;std::vector<std::vector<bool>> nulls;
        const std::vector<std::string> conditions={"likepatterns.v rhs.p"};
        const std::set<std::string> columns={"patterns.id","rhs.id"};
        if (kind==0) (void)engine.join(db,"patterns","rhs","id","id",conditions,columns,&cells,&nulls);
        if (kind==1) (void)engine.leftJoin(db,"patterns","rhs","id","id",conditions,columns,&cells,&nulls);
        if (kind==2) (void)engine.rightJoin(db,"patterns","rhs","id","id",conditions,columns,&cells,&nulls);
        if (kind==3) (void)engine.fullOuterJoin(db,"patterns","rhs","id","id",conditions,columns,&cells,&nulls);
        if (kind==4) (void)engine.crossJoin(db,"patterns","rhs",{"=patterns.id rhs.id","likepatterns.v rhs.p"},columns,&cells,&nulls);
        std::vector<int> ids;
        for (size_t i=0;i<cells.size();++i) {
            check("JOIN bitmap",nulls[i],std::vector<bool>({false,false}));
            ids.push_back(std::stoi(cells[i][0]));
        }
        std::sort(ids.begin(),ids.end());check("JOIN kind "+std::to_string(kind),ids,joinedExpected);
    }
    std::vector<std::vector<std::string>> onCells;std::vector<std::vector<bool>> onNulls;
    (void)engine.leftJoin(db,"patterns","rhs","id","id",{}, {"patterns.id","rhs.id"},&onCells,&onNulls,{"ilikepatterns.v rhs.p"});
    check("left ON ILIKE count",onCells.size(),size_t(10));
    size_t unmatched=0;
    for (size_t i=0;i<onCells.size();++i) if (onNulls[i][1]) ++unmatched;
    check("left ON exact NULL extension",unmatched,size_t(1));
    TableSchema collated;collated.tablename="collated";collated.append(makeIntColumn("id",false,4,true));
    auto collatedText=makeTextColumn("v",true);collatedText.collation="C";
    collated.append(collatedText);
    assert(engine.createTable(db,collated)==DBStatus::OK);
    assert(engine.insertRow(db,"collated",{{"id",std::optional<std::string>("1")},{"v",std::optional<std::string>("É")}})==DBStatus::OK);
    check("stored C native",engine.query(db,"collated",{"ilikev é"},{"id"}).empty(),true);
    std::vector<std::vector<std::string>> cCells;std::vector<std::vector<bool>> cNulls;
    (void)engine.query(db,"collated",{"typedexpr v ILIKE 'é'"},{"id"},{},false,false,false,0,{},&cCells,&cNulls);
    check("stored C typed",cCells.empty(),true);
    (void)engine.join(db,"collated","rhs","id","id",{"typedexpr collated.v ILIKE 'é'"},{"collated.id"},&cCells,&cNulls);
    check("stored C typed JOIN",cCells.empty(),true);
    (void)engine.join(db,"patterns","rhs","id","id",{"likepatterns.v 'aé b' ESCAPE rhs.escape"},{"patterns.id"},&cCells,&cNulls);
    check("stored JOIN column Unicode escape",cCells,std::vector<std::vector<std::string>>({{"8"}}));
    TableSchema empty;empty.tablename="empty_patterns";empty.append(makeIntColumn("id",true,4));empty.append(makeTextColumn("v",true));
    assert(engine.createTable(db,empty)==DBStatus::OK);
    for (const auto& [condition,state] : std::vector<std::pair<std::string,std::string>>{
        {"likeid '%'","42883"},{"likev '_' ESCAPE 'xx'","22025"},
        {"typedexpr id LIKE '%'","42883"}}) {
        std::string actual;
        try {(void)engine.query(db,"empty_patterns",{condition},{"id"},{},false,false,false,0,{},&cCells,&cNulls);}
        catch (const DbError& error) {actual=error.sqlState();}
        check("empty static "+condition,actual,state);
    }
    for (const auto& [condition,state] : std::vector<std::pair<std::string,std::string>>{
        {"typedexpr id LIKE '%'","42883"},{"typedexpr b ILIKE '%'","42883"}}) {
        std::string actual;
        try {(void)structured(condition);}
        catch (const DbError& error) {actual=error.sqlState();}
        check("typed actual type "+condition,actual,state);
    }
    check("typed CHAR pattern cast",structured("typedexpr c LIKE 'a'::char(3)"),std::vector<int>{});
    check("CHAR escape cast",structured("likev 'a# b' ESCAPE '#'::char(3)"),std::vector<int>{8});
    check("typed CHAR escape cast",structured("typedexpr v LIKE 'a# b' ESCAPE '#'::char(3)"),std::vector<int>{8});
    std::vector<StorageEngine::SqlRow> updates;
    assert(engine.updateRows(db,"patterns",{{"p",std::optional<std::string>("changed")}}, {"ilikev 'é'"},&updates)==DBStatus::OK);
    check("native DML exact rows",updates.size(),size_t(2));
    std::vector<StorageEngine::SqlRow> deletes;
    assert(engine.removeRows(db,"patterns",{"similarv 'a.c'"},&deletes)==DBStatus::OK);
    check("native DELETE SQL literal metachar",deletes.size(),size_t(1));
    assert(engine.dropDatabase(db) == DBStatus::OK);
    std::cout << "LEGACY_PATTERN_FAILURE_COUNT=" << failures.size() << std::endl;
    assert(failures.empty());
}
