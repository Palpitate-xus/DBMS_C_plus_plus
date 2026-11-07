#include "commands/TableManage.h"
#include "catalog/type_registry.h"
#include "test_utils.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

extern dbms::StorageEngine g_engine;
using namespace dbms;
namespace {
unsigned checks=0;
void need(bool value,const std::string& reason){++checks;if(!value)throw std::runtime_error(reason);}
std::string bytes(const std::filesystem::path& path) {
    std::ifstream input(path,std::ios::binary);return {std::istreambuf_iterator<char>(input),{}};
}
void replace(const std::filesystem::path& path,const std::string& content) {
    std::ofstream output(path,std::ios::binary|std::ios::trunc);output<<content;
    output.close();need(bool(output),"owned declaration fixture write");
}
}
int main() {
    try {
        TypeRegistry::instance().bootstrap();
        g_engine.setBackgroundIntervals(60000,60000);
        const auto database=testDbPath("sequence_type_storage");cleanupTestDb("sequence_type_storage");
        need(g_engine.createDatabase(database,"utf8")==DBStatus::OK,"actual database");
        SequenceInfo request;request.dataTypeOid=21;request.dataTypeSpecified=true;
        request.start=3;request.startSpecified=true;request.increment=2;request.incrementSpecified=true;
        need(g_engine.createSequence(database,"typed",request)==DBStatus::OK,"actual int2 generator");
        SequenceInfo info;
        need(g_engine.getSequenceInfo(database,"typed",info)==DBStatus::OK && info.dataTypeOid==21 &&
            info.hasStoredType && info.minValue==1 && info.maxValue==32767,"all actual typed limits reload from file");
        const auto path=g_engine.dbPath(database)/"typed.seq";const auto original=bytes(path);
        need(original.rfind("DBMSSEQ6 ",0)==0 && original.find(" SEQTYPE1 21\n")!=std::string::npos,"required V6 type trailer");
        const auto trailer=original.find(" SEQTYPE1 ");need(trailer!=std::string::npos,"actual trailer offset");
        for(const auto& content:std::vector<std::string>{original.substr(0,trailer)+"\n",
            original.substr(0,trailer)+" SEQTYPE1\n",original.substr(0,trailer)+" SEQTYPE1 0\n",
            original.substr(0,trailer)+" SEQTYPE1 25\n",original.substr(0,trailer)+" SEQTYPE1 2\n",
            original.substr(0,trailer)+" SEQTYPE2 21\n",original+"unexpected\n"}) {
            replace(path,content);
            need(g_engine.getSequenceInfo(database,"typed",info)==DBStatus::CORRUPTED_DATA,"whole/truncated/invalid type declaration rejected");
            replace(path,original);
            need(g_engine.getSequenceInfo(database,"typed",info)==DBStatus::OK && info.dataTypeOid==21,
                "exact valid control restored after each corruption");
        }
        std::istringstream tokens(original);std::vector<std::string> words;std::string word;
        while(tokens>>word)words.push_back(word);need(words.size()==17,"complete V6 fixture fields");
        const auto legacy=[&](int version) {
            std::vector<std::string> fields;
            if(version>=2)fields.push_back("DBMSSEQ"+std::to_string(version));
            const size_t last=version>=3?11:version==2?10:8;
            for(size_t i=1;i<=last;++i)fields.push_back(words[i]);
            if(version>=5)fields.push_back(words[12]);
            if(version>=4){fields.push_back(words[13]);fields.push_back(words[14]);}
            std::string text;for(const auto& value:fields){if(!text.empty())text+=' ';text+=value;}return text+'\n';
        };
        for(const int version:{0,2,3,4,5}) {
            replace(path,legacy(version));
            need(g_engine.getSequenceInfo(database,"typed",info)==DBStatus::OK && info.dataTypeOid==20 &&
                !info.hasStoredType && info.start==3 && info.increment==2 && info.minValue==1 && info.maxValue==32767,
                "legacy actual bytes keep int8 semantics and explicit recorded bounds");
        }
        replace(path,original);
        need(g_engine.nextval(database,"typed")==3,"metadata/corruption controls allocate no values");
        SequenceInfo change;change.dataTypeOid=23;change.dataTypeSpecified=true;
        need(g_engine.alterSequence(database,"typed",change)==DBStatus::OK,"actual AS int4");
        need(g_engine.getSequenceInfo(database,"typed",info)==DBStatus::OK && info.dataTypeOid==23 &&
            info.maxValue==2147483647 && info.start==3,"persisted widening resets only old typed extreme");
        need(g_engine.nextval(database,"typed")==5,"ALTER does not consume a generated value");
        change=SequenceInfo{};change.maxValue=40000;change.hasMaxValue=true;
        need(g_engine.alterSequence(database,"typed",change)==DBStatus::OK,"actual custom bound");
        const auto beforeNarrow=bytes(path);change=SequenceInfo{};change.dataTypeOid=21;change.dataTypeSpecified=true;
        need(g_engine.alterSequence(database,"typed",change)==DBStatus::INVALID_VALUE && bytes(path)==beforeNarrow,
            "failed narrowing preserves whole declaration bytes");
        change.dataTypeOid=20;
        need(g_engine.alterSequence(database,"typed",change)==DBStatus::OK,"actual AS int8");
        need(g_engine.getSequenceInfo(database,"typed",info)==DBStatus::OK && info.dataTypeOid==20 &&
            info.hasStoredType && info.maxValue==40000,"explicit int8 and custom bound persist");
        need(g_engine.renameSequence(database,"typed","renamed")==DBStatus::OK,"native rename");
        StorageEngine cold;cold.setBackgroundIntervals(60000,60000);
        need(cold.getSequenceInfo(database,"renamed",info)==DBStatus::OK && info.dataTypeOid==20 &&
            info.hasStoredType && info.maxValue==40000,"independent owner loads renamed typed file");
        need(cold.nextval(database,"renamed")==7,"failed narrowing and rename do not reallocate values");
        need(g_engine.createSequence(database,"legacy_native",1,1)==DBStatus::OK,"historical untyped native API");
        need(bytes(g_engine.dbPath(database)/"legacy_native.seq").rfind("DBMSSEQ5 ",0)==0,
            "historical int8 native writer retains its V5 contract");
        need(cold.checkpoint(database),"persist actual allocation fork");
        std::cout<<"[SEQUENCE TYPE STORAGE] checks="<<checks<<" legacy_formats_and_required_type_verified=true\n";return 0;
    } catch(const std::exception& error){std::cerr<<"SEQUENCE_TYPE_STORAGE_FAIL "<<error.what()<<'\n';return 1;}
}
