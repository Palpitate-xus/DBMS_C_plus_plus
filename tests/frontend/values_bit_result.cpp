// Exercise the real Main VALUES dispatcher, never test_stubs.cpp.
#define main dbms_values_bit_result_program_main_not_invoked
#include "../../src/main.cpp"
#undef main

int main() {
    dbms::TypeRegistry::instance().bootstrap();
    const std::string database="__t_values_bit_result";
    if(g_engine.createDatabase(database,"utf8")!=dbms::DBStatus::OK)return 2;
    Session session;session.username="postgres";session.permission=1;session.currentDB=database;
    dbms::setCurrentSession(&session);
    size_t checked=0,failed=0;
    const auto check=[&](bool pass,const std::string& label) {
        ++checked;if(!pass){++failed;std::cerr<<"FAIL "<<label<<'\n';}
    };
    struct Case {std::string sql,state;std::vector<uint32_t> oids;std::vector<int32_t> modifiers;
        std::vector<std::vector<std::string>> rows;std::vector<std::vector<bool>> nulls;};
    // Actual PostgreSQL18.6 expected results, frozen before producer changes.
    const Case cases[]={
        {"VALUES (B'101'),(B'1'),(B''),(NULL)","",{1560},{-1},{{"101"},{"1"},{""},{""}},{{false},{false},{false},{true}}},
        {"VALUES (B'101'),(B'101')","",{1560},{-1},{{"101"},{"101"}},{{false},{false}}},
        {"VALUES (B''),(B'')","",{1560},{-1},{{""},{""}},{{false},{false}}},
        {"VALUES (NULL::bit(8)),(NULL::bit(8))","",{1560},{8},{{""},{""}},{{true},{true}}},
        {"VALUES (NULL::bit),(B'101'::varbit)","",{1560},{-1},{{""},{"101"}},{{true},{false}}},
        {"VALUES (NULL::varbit),(B'101')","",{1562},{-1},{{""},{"101"}},{{true},{false}}},
        {"VALUES (B'101'::varbit),(B'01')","",{1562},{-1},{{"101"},{"01"}},{{false},{false}}},
        {"VALUES (B'01'),(B'101'::varbit)","",{1560},{-1},{{"01"},{"101"}},{{false},{false}}},
        {"VALUES (B'101'::varbit(8)),(B'1'::varbit(8))","",{1562},{8},{{"101"},{"1"}},{{false},{false}}},
        {"VALUES ('101'),(B'101')","",{1560},{-1},{{"101"},{"101"}},{{false},{false}}},
        {"VALUES ('10100'),(B'1')","",{1560},{-1},{{"10100"},{"1"}},{{false},{false}}},
        {"VALUES (B'1'),('10100')","",{1560},{-1},{{"1"},{"10100"}},{{false},{false}}},
        {"VALUES ('10100'),(B'1'::varbit)","",{1562},{-1},{{"10100"},{"1"}},{{false},{false}}},
        {"VALUES ('101x'),(B'1')","22P02",{},{},{},{}},
        {"VALUES (NULL),(NULL::bit)","",{1560},{-1},{{""},{""}},{{true},{true}}},
        {"VALUES (NULL),(NULL::varbit)","",{1562},{-1},{{""},{""}},{{true},{true}}},
        {"VALUES (NULL::pg_catalog.bit),(B'101'::pg_catalog.varbit)","",{1560},{-1},{{""},{"101"}},{{true},{false}}},
        {"VALUES (B'101'::\"pg_catalog\".\"bit\"),(B'01'::\"pg_catalog\".\"varbit\")","",{1560},{-1},{{"101"},{"01"}},{{false},{false}}},
        {"VALUES (B'101',NULL),(NULL,B'1'::varbit)","",{1560,1562},{-1,-1},{{"101",""},{"","1"}},{{false,true},{true,false}}},
        {"VALUES (B'101',1,'x'),(B'1',2,NULL)","",{1560,23,25},{-1,-1,-1},{{"101","1","x"},{"1","2",""}},{{false,false,false},{false,false,true}}},
        {"VALUES (CAST(1 AS bit(8))),(CAST(-1 AS bit(8)))","",{1560},{8},{{"00000001"},{"11111111"}},{{false},{false}}},
        {"VALUES (B'101'::bit(4)),(B'1'::bit(4))","",{1560},{4},{{"1010"},{"1000"}},{{false},{false}}},
        {"VALUES (B'101'::bit(3)),(B'1'::bit(4))","",{1560},{-1},{{"101"},{"1000"}},{{false},{false}}},
        {"VALUES (B'101'),(1)","42804",{},{},{},{}},
        {"VALUES (TRUE),(B'101')","42804",{},{},{},{}},
        {"VALUES (1),(2),(NULL)","",{23},{-1},{{"1"},{"2"},{""}},{{false},{false},{true}}},
        {"VALUES ('abc'::character(4)),('xy'::character(4))","",{1042},{8},{{"abc "},{"xy  "}},{{false},{false}}},
        {"VALUES ('x'::varchar(8)),('longer'::varchar(8))","",{1043},{12},{{"x"},{"longer"}},{{false},{false}}},
        {"VALUES (NULL),(NULL)","",{25},{-1},{{""},{""}},{{true},{true}}},
        {"VALUES (nextval('r338_values_once')::bit(8)),(B'101'::varbit)","",{1560},{-1},{{"00000001"},{"101"}},{{false},{false}}},
        {"VALUES (nextval('r338_values_once')::bit(8)),(B'101'::varbit)","",{1560},{-1},{{"00000010"},{"101"}},{{false},{false}}},
        {"VALUES (nextval('r338_values_once')::bit(8)),(B'101'::varbit)","",{1560},{-1},{{"00000011"},{"101"}},{{false},{false}}},
    };
    bool sequenceCreated=false;
    for(const auto& item:cases) {
        if(item.sql.find("nextval(")!=std::string::npos && !sequenceCreated) {
            std::ostringstream output;dbms::ScopedOutputCapture capture(output);
            check(!execute("CREATE SEQUENCE r338_values_once START 1",session),"sequence setup");
            sequenceCreated=true;
        }
        if(item.state.empty()) {
            try {
                const auto prepared=g_engine.prepareBoundQuery(database,item.sql);
                bool descriptor=prepared.output.size()==item.oids.size();
                for(size_t i=0;descriptor && i<item.oids.size();++i)
                    descriptor=prepared.output[i].typeOid==item.oids[i] && prepared.output[i].typeMod==item.modifiers[i];
                check(descriptor,item.sql+" actual bound declaration");
            } catch(const dbms::DbError& error){check(false,item.sql+" bound SQLSTATE="+error.sqlState());}
        }
        dbms::clearLastDmlResult();
        std::ostringstream output;std::string state;bool error=false;
        try {dbms::ScopedOutputCapture capture(output);error=execute(item.sql,session);}
        catch(const dbms::DbError& exception){error=true;state=exception.sqlState();}
        const auto result=dbms::takeLastDmlResult();
        if(!item.state.empty()) {
            const bool actualState=state==item.state ||
                output.str().find("(SQLSTATE "+item.state+")")!=std::string::npos ||
                (item.state=="22P02" && output.str().find("invalid input syntax")!=std::string::npos);
            check(error && actualState,item.sql+" expected SQLSTATE="+item.state+" actual="+state+" output="+output.str());
            check(!result.available || result.rows.empty(),item.sql+" no success rows after error");
            continue;
        }
        check(!error && state.empty(),item.sql+" execution output="+output.str());
        check(result.available && !result.metadataOnly,item.sql+" actual structured VALUES result");
        if(!result.available || result.metadataOnly)continue;
        check(result.rows==item.rows,item.sql+" complete values");
        check(result.nulls==item.nulls,item.sql+" SQL NULL distinct from empty bit string");
        check(result.commandTag=="SELECT "+std::to_string(item.rows.size()),item.sql+" command tag");
        bool types=result.columnTypes.size()==item.oids.size();
        for(size_t i=0;types && i<item.oids.size();++i)
            types=dbms::mapBuiltinTypeNameToOid(result.columnTypes[i])==item.oids[i];
        check(types,item.sql+" actual result type identities");
    }
    const auto value=dbms::ExprHelper::evalString("currval('r338_values_once')",{}, {},database,session.username);
    check(value.ok && !value.isNull && value.value=="3","no binding effects; once evaluation per VALUES expression");
    dbms::setCurrentSession(nullptr);
    std::cout<<"VALUES_BIT_RESULT_COMPLETE CHECKED="<<checked<<" FAILURES="<<failed<<'\n';
    return failed?1:0;
}
