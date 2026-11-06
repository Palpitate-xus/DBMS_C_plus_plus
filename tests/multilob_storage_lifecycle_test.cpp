// P29 permanent-entry DRAFT: default noargs owns ten current-generation exec workers.
// The following original v4 helpers/SQL/values/assertions and worker dispatch are
// byteexact except the mechanical main -> p29OriginalWorkerMain identifier.
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "common/sha256.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fcntl.h>
#include <regex>
#include <spawn.h>
#include <sstream>
#include <thread>
#include <sys/wait.h>
#include <unistd.h>
extern char** environ;
// BEGIN EXACT V4 WORKER (main identifier adapted only)
// P29 DRAFT V4, NOT COMPILED/RUN. V1/V2/V3 preserved; actual fresh-page bound is 8160. Actual public engine + physical heap evidence.
// Root compiles identical source against each generation's headers/core61/stub.
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "commands/DdlExecutor.h"
#include "commands/HeapScanCursor.h"
#include "commands/StoredRowNull.h"
#include "storage/DataFileHeader.h"
#include "storage/HeapTupleHeader.h"
#include "storage/PageAllocator.h"
#include "storage/PageWrapper.h"
#include "storage/PgPage.h"
#include "types/bytea.h"
#include "utils/Session.h"
#include <array>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>
extern dbms::StorageEngine g_engine;
namespace {
using namespace dbms; namespace fs=std::filesystem;
using Row=StorageEngine::SqlRow; using Model=std::map<int,Row>;
const std::string db="multilob_storage";Session session;size_t checks=0,groups=0;
void need(bool good,const std::string& message){++checks;if(!good)throw std::runtime_error(message);}
std::string hex(const std::string& s){static const char* d="0123456789abcdef";std::string r;for(unsigned char c:s){r+=d[c>>4];r+=d[c&15];}return r;}
std::string rawPayload(size_t n,uint32_t x){std::string s(n,'\0');for(char& c:s){x^=x<<13;x^=x>>17;x^=x<<5;c=char(x&255);}if(n>1){s[0]='\0';s[1]=char(255);}return s;}
std::string blob(size_t n,uint32_t seed){return ByteaValue::fromBytes(rawPayload(n,seed)).toString();}
void beginGroup(const std::string& name){++groups;std::cout<<"MULTILOB_GROUP "<<groups<<' '<<std::quoted(name)<<'\n';}
void ddl(const std::string& sql){DdlExecutor command;need(!command.executeSql(sql,session),"original actual DDL failed: "+sql);std::cout<<"MULTILOB_DDL "<<std::quoted(sql)<<'\n';}
void flush(const std::string& table){auto* pa=g_engine.getPageAllocator(db,table);need(pa&&pa->flushAllocationStateForCommit()&&pa->flush(),"real allocator commit/flush "+table);need(g_engine.checkpoint(db),"public normal checkpoint");need(!fs::exists(fs::path(db)/(table+".dt.extent_pending")),"normal extent marker removed");}
uint32_t fnv(const char* p,size_t n){uint32_t h=2166136261u;for(size_t i=0;i<n;++i){h^=static_cast<unsigned char>(p[i]);h*=16777619u;}return h;}
DataFileHeader header(const fs::path& path,uint64_t logical,uint32_t wanted){
 std::ifstream in(path,std::ios::binary);need(bool(in),"actual heap header opens "+path.string());
 std::array<char,24> bytes{};in.read(bytes.data(),bytes.size());need(in.gcount()==24,"complete original24B header");
 DataFileHeader h{};static_assert(sizeof h==24,"unchanged file header ABI");std::memcpy(&h,bytes.data(),24);
 need(h.magic==0x44415441&&h.formatVersion==2&&h.numPages>0&&h.freeListHead<h.numPages,"header magic/version/extent metadata");
 need(h.rowSize==wanted&&h.headerChecksum==fnv(bytes.data(),20),"independent header width/checksum");
 need(fs::file_size(path)==uint64_t(h.numPages)*8192,"durable exact heap extent");
 std::cout<<"MULTILOB_HEADER "<<std::quoted(path.string())<<" logical="<<logical<<" hint="<<h.rowSize<<" pages="<<h.numPages<<" raw24="<<hex(std::string(bytes.data(),bytes.size()))<<'\n';return h;
}
uint32_t wantedWidth(const TableSchema& schema){
 const auto nominal=schema.rowSize();if(nominal<=UINT32_MAX)return static_cast<uint32_t>(nominal);
 need(schema.formatVersion==2&&schema.hasVariableLength(),"only current true var schema overflow role");
 const size_t minimum=computeHeapHeaderSize(static_cast<int>(schema.len))+schema.fixedDataSize()+4*schema.varColCount();
 need(minimum<=PgPage::PAGE_SIZE-sizeof(PgPage::PageHeaderData)-sizeof(PgPage::ItemIdData)-sizeof(uint32_t),"real custom tuple minimum fits page");return 0;
}
struct Actual{int64_t rid;Row row;};using ActualRows=std::map<int,Actual>;
ActualRows verify(const std::string& table,const Model& wanted,bool requireLarge=false){
 const auto schema=g_engine.getTableSchema(db,table);need(schema.len>0,"actual schema exists");
 auto* allocator=g_engine.getPageAllocator(db,table);need(allocator&&allocator->rowSize()==wantedWidth(schema),"actual cached allocator agrees with immutable width role");
 flush(table);header(fs::path(db)/(table+".dt"),schema.rowSize(),wantedWidth(schema));
 auto scan=g_engine.beginHeapScan(db,table,schema);need(bool(scan),"actual public visible heap cursor");HeapScanRow item;ActualRows actual;size_t largePointers=0;
 while(scan->next(item)){
  need(item.rid>0&&item.nulls.size()==schema.len,"actual RID and NULL owner width");
  std::vector<std::string> values;std::vector<bool> nulls;detail::decodeStoredRowValues(g_engine,db,item.data,schema,item.nulls,values,nulls);
  need(values.size()==schema.len&&nulls==item.nulls,"full actual value/NULL decode");Row row;
  for(size_t i=0;i<schema.len;++i)row[schema.cols[i].dataName]=nulls[i]?std::nullopt:std::optional<std::string>{values[i]};
  need(row.count("id")&&row.at("id"),"actual ID datum");int id=std::stoi(*row.at("id"));need(actual.emplace(id,Actual{item.rid,row}).second,"unique actual fixture ID");
  auto expected=wanted.find(id);need(expected!=wanted.end()&&expected->second==row,"all original fields/NULL/empty exact, id="+std::to_string(id));
  uint32_t pageId;uint16_t slot;StorageEngine::decodeRid(item.rid,pageId,slot);need(allocator->pageExistsOnDisk(pageId)==std::optional<bool>{true},"actual durable header agrees with requested width and RID extent");char* buffer=allocator->fetchPage(pageId);need(buffer,"actual RID page is fetched");
  struct Pin{PageAllocator* p;uint32_t id;~Pin(){p->unpinPage(id);}}pin{allocator,pageId};
  PageWrapper page(buffer,allocator->pageSize(),schema.formatVersion);const char* tuple=nullptr;size_t length=0;
  need(page.isValid(pageId)&&page.read(slot,tuple,length),"actual page identity/checksum and slot");
  need(length<=PgPage::PAGE_SIZE-sizeof(PgPage::PageHeaderData)-sizeof(PgPage::ItemIdData)-sizeof(uint32_t)&&length>=sizeof(HeapTupleHeaderData),"actual tuple length fits physical page");
  const auto* ht=reinterpret_cast<const HeapTupleHeaderData*>(tuple);need((ht->t_infomask2&HEAP_NATTS_MASK)==schema.len&&ht->t_hoff<=length,"actual tuple header attributes/hoff");
  need(std::string(tuple+ht->t_hoff,length-ht->t_hoff)==item.data,"cursor bytes are exact owned original tuple payload");
  std::set<uint64_t> rowPointers;
  for(size_t i=0;i<schema.len;++i){
   need(nulls[i]==(schema.cols[i].isNull&&dbms::isNull(ht,static_cast<int>(i))),"actual physical bitmap agrees with visible row");
   std::cout<<"MULTILOB_CELL "<<std::quoted(table)<<' '<<id<<' '<<item.rid<<' '<<std::quoted(schema.cols[i].dataName)<<' '<<nulls[i]<<' '<<hex(values[i])<<'\n';
   if(nulls[i]||!schema.cols[i].isVariableLength)continue;
   const size_t pos=schema.fixedDataSize()+4*schema.getVarColIndex(i);need(pos+4<=item.data.size(),"actual var directory bounded");uint16_t offset=0,size=0;std::memcpy(&offset,item.data.data()+pos,2);std::memcpy(&size,item.data.data()+pos+2,2);need(offset<=item.data.size()&&size<=item.data.size()-offset,"actual var span bounded");
   const std::string stored=item.data.substr(offset,size);uint64_t pointer=0;
   if(StorageEngine::parseToastMarker(stored,pointer)){
    std::string bytes;need(rowPointers.insert(pointer).second&&g_engine.readToast(db,table,pointer,schema.cols[i].dsize,bytes)&&bytes==values[i],"real distinct TOAST IDs and complete original bytes");
    std::cout<<"MULTILOB_TOAST "<<std::quoted(table)<<' '<<id<<' '<<std::quoted(schema.cols[i].dataName)<<" id="<<pointer<<" stored="<<hex(stored)<<" bytes="<<bytes.size()<<'\n';if(bytes.size()>65535)++largePointers;
   }
   if(schema.cols[i].dataType=="blob"){ByteaValue decoded;need(ByteaValue::parse(values[i],decoded),"actual bytea input remains canonical parsable");need(decoded.toString()==values[i],"complete binary bytes roundtrip without truncation");}
  }
  std::cout<<"MULTILOB_TUPLE "<<std::quoted(table)<<' '<<id<<' '<<item.rid<<" bytes="<<length<<" payload="<<hex(item.data)<<'\n';
 }
 need(actual.size()==wanted.size(),"complete visible population");if(requireLarge)need(largePointers>=2,"two genuinely external >64KiB bytea values");std::cout<<"MULTILOB_VISIBLE_RIDS "<<std::quoted(table)<<' '<<actual.size();for(const auto& row:actual)std::cout<<' '<<row.first<<' '<<row.second.rid;std::cout<<'\n';return actual;
}
void sameRID(const ActualRows&a,const ActualRows&b){need(a.size()==b.size(),"RID population size");for(const auto& x:a)need(b.count(x.first)&&b.at(x.first).rid==x.second.rid,"unchanged row actual RID, no assumed updated-RID stability");}
void insert(const std::string& table,const Row& row){need(g_engine.insertRow(db,table,row)==DBStatus::OK,"public real INSERT "+table);}
Model original(){
 return {{1,{{"id","1"},{"n","1"},{"tag",""},{"other_tag","NULL"},{"bytes",blob(0,1)},{"other_bytes",std::nullopt},{"select","8"}}},
 {2,{{"id","2"},{"n",std::nullopt},{"tag",std::nullopt},{"other_tag",""},{"bytes",std::nullopt},{"other_bytes",blob(0,2)},{"select","9"}}},
 {3,{{"id","3"},{"n","3"},{"tag",std::string(7000,'T')+"Ω"},{"other_tag","__TOAST__1"},{"bytes",blob(65553,0x13579bdf)},{"other_bytes",blob(71693,0x2468ace0)},{"select","10"}}},
 {4,{{"id","4"},{"n","4"},{"tag","text-λ"},{"other_tag",std::nullopt},{"bytes",blob(19,17)},{"other_bytes",blob(21,19)},{"select","11"}}}};
}
void legacy(bool updated){
 Model fixed={{1,{{"id","1"},{"n",updated?"8":"7"}}},{2,{{"id","2"},{"n",std::nullopt}}}};
 Model one={{1,{{"id","1"},{"payload",blob(updated?90013:70003,0x55aa77bb)}}},{2,{{"id","2"},{"payload",std::nullopt}}}};
 if(updated){fixed[3]={{"id","3"},{"n","9"}};one[3]={{"id","3"},{"payload",blob(0,5)}};}
 verify("legacy_fixed",fixed);verify("legacy_one_lob",one);
 std::ifstream pins("legacy-header-widths.txt");uint64_t a=0,b=0;pins>>a>>b;need(bool(pins),"actual before seed header identity pins");
 need(header(fs::path(db)/"legacy_fixed.dt",g_engine.getTableSchema(db,"legacy_fixed").rowSize(),static_cast<uint32_t>(a)).rowSize==a,"old fixed width never migrated");
 need(header(fs::path(db)/"legacy_one_lob.dt",g_engine.getTableSchema(db,"legacy_one_lob").rowSize(),static_cast<uint32_t>(b)).rowSize==b,"old singleLOB width never migrated");
}
void seedLegacy(){
 beginGroup("legacy-before-seed-fixed-and-singleLOB");need(g_engine.createDatabase(db)==DBStatus::OK,"real legacy database");
 TableSchema fixed;fixed.tablename="legacy_fixed";fixed.append(makeIntColumn("id",false,4,true));fixed.append(makeIntColumn("n",true,4));need(g_engine.createTable(db,fixed)==DBStatus::OK,"old fixed table");
 TableSchema one;one.tablename="legacy_one_lob";one.append(makeIntColumn("id",false,4,true));one.append(makeBlobColumn("payload",true));need(one.rowSize()<=UINT32_MAX,"genuine old representable singleLOB nominal");need(g_engine.createTable(db,one)==DBStatus::OK,"old singleLOB table");
 insert("legacy_fixed",{{"id","1"},{"n","7"}});insert("legacy_fixed",{{"id","2"},{"n",std::nullopt}});
 insert("legacy_one_lob",{{"id","1"},{"payload",blob(70003,0x55aa77bb)}});insert("legacy_one_lob",{{"id","2"},{"payload",std::nullopt}});
 need(!fs::exists("legacy-header-widths.txt"),"fresh legacy pin file");std::ofstream pin("legacy-header-widths.txt");pin<<fixed.rowSize()<<' '<<one.rowSize()<<'\n';pin.close();need(!pin.fail(),"legacy nominal pin complete");legacy(false);
}
void updateLegacy(){
 beginGroup("legacy-after-open-write-marker-flush");legacy(false);
 need(g_engine.updateRows(db,"legacy_fixed",{{"n","8"}},{"=id 1"})==DBStatus::OK,"update actual old fixed");
 need(g_engine.updateRows(db,"legacy_one_lob",{{"payload",blob(90013,0x55aa77bb)}},{"=id 1"})==DBStatus::OK,"update actual old singleLOB");
 insert("legacy_fixed",{{"id","3"},{"n","9"}});insert("legacy_one_lob",{{"id","3"},{"payload",blob(0,5)}});legacy(true);
}
void pageBoundary(){
 beginGroup("real-PgPage-reserved-nextPage-boundary");std::array<char,PgPage::PAGE_SIZE> bytes{};PgPage page(bytes.data());page.init(17);
 const size_t maximum=PgPage::PAGE_SIZE-sizeof(PgPage::PageHeaderData)-sizeof(PgPage::ItemIdData)-sizeof(uint32_t);
 need(maximum==8160&&page.canFit(maximum)&&!page.canFit(maximum+1),"real page special4 plus lineptr produces 8160 bound");
 std::string payload(maximum,'P');OffsetNumber pointer=0;need(page.insert(payload.data(),payload.size(),pointer),"real full8160 insertion");const char* got=nullptr;size_t n=0;
 need(page.isValid(17)&&page.get(pointer,got,n)&&n==maximum&&std::string(got,n)==payload,"real full8160 page checksum and bytes");
}
void partitionVerify(bool final){
 beginGroup("real-partition-fork-owner-not-root-RID");const auto schema=g_engine.getTableSchema(db,"multi_parts");need(schema.partitionType==TableSchema::PartitionType::List&&schema.rowSize()>UINT32_MAX,"actual overflowing list partition schema");
 Model expected=final?Model{{201,{{"id","201"},{"region","NY"},{"a",blob(65553,71)},{"b",blob(71693,73)},{"tag","after truncate"}}}}:
 Model{{101,{{"id","101"},{"region","NY"},{"a",blob(65553,71)},{"b",blob(71693,73)},{"tag","east"}}},
 {102,{{"id","102"},{"region","CA"},{"a",std::nullopt},{"b",blob(0,2)},{"tag",""}}},
 {103,{{"id","103"},{"region","TX"},{"a",blob(0,3)},{"b",std::nullopt},{"tag",std::nullopt}}}};
 need(g_engine.checkpoint(db),"normal partition checkpoint");std::set<int> seen;std::map<int,std::pair<std::string,int64_t>> physical;
 for(const std::string leaf:{"east","west","other"}){
  const auto path=fs::path(db)/("multi_parts#"+leaf+".dt");header(path,schema.rowSize(),0);std::ifstream input(path,std::ios::binary);need(bool(input),"actual partition fork exists");
  need(g_engine.forEachRow(db,"multi_parts",[&](uint32_t pageId,uint16_t slot,const char* raw,size_t rawlen){
   std::array<char,PgPage::PAGE_SIZE> bytes{};input.clear();input.seekg(uint64_t(pageId)*bytes.size());input.read(bytes.data(),bytes.size());need(input.gcount()==static_cast<std::streamsize>(bytes.size()),"actual leaf RID fullpage read");
   PageWrapper page(bytes.data(),bytes.size(),schema.formatVersion);const char* tuple=nullptr;size_t n=0;need(page.isValid(pageId)&&page.read(slot,tuple,n)&&n>=sizeof(HeapTupleHeaderData),"actual leaf page checksum/RID");
   const auto* ht=castHeapHeader(tuple);need(ht->t_hoff<=n&&std::string(tuple+ht->t_hoff,n-ht->t_hoff)==std::string(raw,rawlen),"logical visible row matches its actual physical fork tuple");
   std::vector<bool> bitmap(schema.len);for(size_t i=0;i<schema.len;++i)bitmap[i]=schema.cols[i].isNull&&dbms::isNull(ht,static_cast<int>(i));std::vector<std::string> values;std::vector<bool> nulls;
   detail::decodeStoredRowValues(g_engine,db,std::string(raw,rawlen),schema,bitmap,values,nulls);need(values.size()==schema.len&&nulls==bitmap,"actual partition NULL owner captured from actual fork");Row row;for(size_t i=0;i<schema.len;++i)row[schema.cols[i].dataName]=nulls[i]?std::nullopt:std::optional<std::string>{values[i]};
   const int id=std::stoi(*row.at("id"));need(seen.insert(id).second&&expected.count(id)&&expected.at(id)==row,"complete visible leaf values/NULL without RID alias across forks");
   const std::string actualLeaf=*row.at("region")=="NY"?"east":(*row.at("region")=="CA"?"west":"other");need(actualLeaf==leaf,"true partition routing");physical.emplace(id,std::make_pair(path.string(),StorageEngine::encodeRid(pageId,slot)));
   std::cout<<"MULTILOB_PARTITION "<<std::quoted(path.string())<<' '<<id<<' '<<StorageEngine::encodeRid(pageId,slot)<<" payload="<<hex(std::string(raw,rawlen))<<'\n';
  },nullptr,{leaf}),"actual visible partition scan succeeds");
 }
 need(seen.size()==expected.size(),"all and only original partition rows");std::cout<<"MULTILOB_PARTITION_RIDS "<<physical.size();for(const auto& row:physical)std::cout<<' '<<row.first<<' '<<std::quoted(row.second.first)<<' '<<row.second.second;std::cout<<'\n';
}
void partitions(){
 beginGroup("normal-partition-create-flush-rewrite-truncate");TableSchema schema;schema.tablename="multi_parts";schema.append(makeIntColumn("id",false,4));schema.append(makeVarCharColumn("region",false,16));schema.append(makeBlobColumn("a",true));schema.append(makeBlobColumn("b",true));schema.append(makeTextColumn("tag",true));
 schema.partitionType=TableSchema::PartitionType::List;schema.partitionKey="region";schema.listPartitions={{"east",{"NY"}},{"west",{"CA"}}};schema.defaultPartitionName="other";need(g_engine.createTable(db,schema)==DBStatus::OK,"real supported overflowing partition table");
 insert("multi_parts",{{"id","101"},{"region","NY"},{"a",blob(65553,71)},{"b",blob(71693,73)},{"tag","east"}});insert("multi_parts",{{"id","102"},{"region","CA"},{"a",std::nullopt},{"b",blob(0,2)},{"tag",""}});insert("multi_parts",{{"id","103"},{"region","TX"},{"a",blob(0,3)},{"b",std::nullopt},{"tag",std::nullopt}});partitionVerify(false);
 bool ok=false;need(g_engine.vacuumFull(db,"multi_parts",&ok)==3&&ok,"normal partition VACUUM FULL");partitionVerify(false);
 need(g_engine.truncateTable(db,"multi_parts")==DBStatus::OK,"normal partition TRUNCATE");insert("multi_parts",{{"id","201"},{"region","NY"},{"a",blob(65553,71)},{"b",blob(71693,73)},{"tag","after truncate"}});partitionVerify(true);
}
void unloggedHeads(bool overflow,bool requireCurrentInit=true){
 const auto schema=g_engine.getTableSchema(db,"u_width");need(schema.isUnlogged,"actual UNLOGGED identity");const uint32_t width=wantedWidth(schema);need((width==0)==overflow,"actual logical width role");
 auto main=header(fs::path(db)/"u_width.dt",schema.rowSize(),width);
 const auto initPath=fs::path(db)/"u_width.dt.init";uint32_t initWidth=width;
 if(!requireCurrentInit){DataFileHeader actual{};std::ifstream input(initPath,std::ios::binary);need(bool(input.read(reinterpret_cast<char*>(&actual),sizeof actual)),"actual pre-reset init header observation");initWidth=actual.rowSize;std::cout<<"MULTILOB_INIT_BEFORE_RESET observed="<<initWidth<<" current_main="<<width<<" refresh_deferred_until_reset=1\n";}
 auto init=header(initPath,schema.rowSize(),initWidth);need(init.numPages==1,"actual init fork remains empty single header page");
 header(fs::path(db)/"u_width.toast.dt",0,0);auto toastInit=header(fs::path(db)/"u_width.toast.dt.init",0,0);need(toastInit.numPages==1&&main.numPages>0,"real TOAST init0 distinct from relation logical width");
}
void unloggedOverflow(){
 beginGroup("UNLOGGED-nonzero-to-zero-init-and-TRUNCATE");need(g_engine.createDatabase(db)==DBStatus::OK,"unlogged independent database");ddl("CREATE UNLOGGED TABLE u_width(id INT PRIMARY KEY,a BYTEA)");
 insert("u_width",{{"id","1"},{"a",blob(70003,0x66778899)}});verify("u_width",{{1,{{"id","1"},{"a",blob(70003,0x66778899)}}}});unloggedHeads(false);
 const auto old=g_engine.getTableSchema(db,"u_width").rowSize();std::ofstream pin("unlogged-header-width.txt");pin<<old<<'\n';pin.close();need(!pin.fail(),"before schema-width pin");
 ddl("ALTER TABLE u_width ADD COLUMN b BYTEA");need(g_engine.updateRows(db,"u_width",{{"b",blob(71693,0x778899aa)}},{"=id 1"})==DBStatus::OK,"actual new overflow field write");verify("u_width",{{1,{{"id","1"},{"a",blob(70003,0x66778899)},{"b",blob(71693,0x778899aa)}}}},true);unloggedHeads(true,false);
 need(g_engine.truncateTable(db,"u_width")==DBStatus::OK,"actual reset from current .init");verify("u_width",{});unloggedHeads(true);
 insert("u_width",{{"id","2"},{"a",blob(70003,0x66778899)},{"b",blob(71693,0x778899aa)}});verify("u_width",{{2,{{"id","2"},{"a",blob(70003,0x66778899)},{"b",blob(71693,0x778899aa)}}}},true);unloggedHeads(true);
}
void unloggedCold(bool drop){
 beginGroup(drop?"UNLOGGED-zero-back-to-original-nonzero":"UNLOGGED-overflow-normal-cold");verify("u_width",{{2,{{"id","2"},{"a",blob(70003,0x66778899)},{"b",blob(71693,0x778899aa)}}}},true);unloggedHeads(true);
 if(drop){ddl("ALTER TABLE u_width DROP COLUMN b");verify("u_width",{{2,{{"id","2"},{"a",blob(70003,0x66778899)}}}});unloggedHeads(false,false);need(g_engine.truncateTable(db,"u_width")==DBStatus::OK,"actual representable reset");insert("u_width",{{"id","3"},{"a",blob(70003,0x66778899)}});verify("u_width",{{3,{{"id","3"},{"a",blob(70003,0x66778899)}}}});unloggedHeads(false);}
}
void unloggedFinal(){beginGroup("UNLOGGED-representable-normal-cold-no-migration");verify("u_width",{{3,{{"id","3"},{"a",blob(70003,0x66778899)}}}});unloggedHeads(false);uint64_t old=0;std::ifstream pin("unlogged-header-width.txt");pin>>old;need(bool(pin)&&g_engine.getTableSchema(db,"u_width").rowSize()==old,"inverse DROP restores actual old singleLOB nominal width");}
void multi(){
 pageBoundary();beginGroup("original-seven-column-DDL-and-physical-policy");need(g_engine.createDatabase(db)==DBStatus::OK,"real multiLOB database");
 ddl("CREATE TABLE cv_sig(id INT PRIMARY KEY,n INT,tag TEXT,other_tag TEXT,bytes BYTEA,other_bytes BYTEA,\"select\" INT)");
 auto schema=g_engine.getTableSchema(db,"cv_sig");need(schema.len==7&&schema.cols[4].dsize==INT32_MAX&&schema.cols[5].dsize==INT32_MAX&&schema.rowSize()>UINT32_MAX,"original logical maxima remain unchanged");
 Model model=original();for(const auto& row:model)insert("cv_sig",row.second);
 beginGroup("actual-NULL-empty-two-large-TOAST-binary");auto initial=verify("cv_sig",model,true);
 beginGroup("real-mutation-and-normal-transaction-rollback");
 need(g_engine.beginTransaction(db)==DBStatus::OK,"ordinary transaction starts");
 need(g_engine.updateRows(db,"cv_sig",{{"bytes",blob(90017,31)},{"other_bytes",blob(0,9)}},{"=id 3"})==DBStatus::OK,"large to empty update");
 need(g_engine.savepoint("p29_real") == DBStatus::OK,"ordinary savepoint");need(g_engine.updateRows(db,"cv_sig",{{"tag","savepoint transient"}},{"=id 1"})==DBStatus::OK,"real savepoint mutation");need(g_engine.rollbackToSavepoint("p29_real")==DBStatus::OK,"ordinary savepoint rollback");need(g_engine.releaseSavepoint("p29_real")==DBStatus::OK,"ordinary savepoint release");need(g_engine.rollbackTransaction()==DBStatus::OK,"ordinary real rollback");sameRID(initial,verify("cv_sig",model,true));
 need(g_engine.updateRows(db,"cv_sig",{{"bytes",blob(81937,29)},{"other_bytes",std::nullopt}},{"=id 1"})==DBStatus::OK,"real small to large and NULL transition");model[1]["bytes"]=blob(81937,29);model[1]["other_bytes"]=std::nullopt;
 need(g_engine.remove(db,"cv_sig",{"=id 4"})==DBStatus::OK,"real delete");model.erase(4);verify("cv_sig",model,true);
 beginGroup("ordinary-VACUUM-positive");g_engine.vacuum(db,"cv_sig");verify("cv_sig",model,true);beginGroup("ordinary-VACUUM-FULL-rewrite");bool success=false;need(g_engine.vacuumFull(db,"cv_sig",&success)==model.size()&&success,"public VACUUM FULL");verify("cv_sig",model,true);
 beginGroup("ADD-DROP-ALTER-supported-schema-rewrite");ddl("ALTER TABLE cv_sig ADD COLUMN extra INT");for(auto& row:model)row.second["extra"]=std::nullopt;verify("cv_sig",model,true);
 ddl("ALTER TABLE cv_sig DROP COLUMN extra");for(auto& row:model)row.second.erase("extra");ddl("ALTER TABLE cv_sig ALTER COLUMN n TYPE BIGINT");verify("cv_sig",model,true);
 beginGroup("public-backup-restore-positive");auto before=verify("cv_sig",model,true);need(g_engine.physicalBackup(db,"multilob-backup-image"),"actual public backup");
 Row later=model.begin()->second;later["id"]="55";insert("cv_sig",later);need(g_engine.physicalRestore(db,"multilob-backup-image"),"actual public restore");sameRID(before,verify("cv_sig",model,true));
 beginGroup("TRUNCATE-reset-reinsert");need(g_engine.truncateTable(db,"cv_sig")==DBStatus::OK,"normal TRUNCATE");verify("cv_sig",{});
 Row final=original().at(3);final["id"]="99";final["n"]="7";insert("cv_sig",final);verify("cv_sig",{{99,final}},true);
 beginGroup("actual-MAX_COLUMNS-metadata-no-huge-allocation");TableSchema many;many.tablename="max_lobs";many.append(makeIntColumn("id",false,4,true));Row nullrow={{"id","777"}};
 for(int i=1;i<MAX_COLUMNS;++i){const auto name="b"+std::to_string(i);many.append(makeBlobColumn(name,true));nullrow[name]=std::nullopt;}
 need(many.len==MAX_COLUMNS&&many.rowSize()>UINT32_MAX,"actual supported maximum column metadata");need(g_engine.createTable(db,many)==DBStatus::OK,"many logical LOBs actual empty heap");insert("max_lobs",nullrow);verify("max_lobs",{{777,nullrow}});partitions();
}
void multiCold(){beginGroup("true-fresh-process-cold");Row row=original().at(3);row["id"]="99";row["n"]="7";verify("cv_sig",{{99,row}},true);const auto schema=g_engine.getTableSchema(db,"cv_sig");need(schema.cols[4].dsize==INT32_MAX&&schema.cols[5].dsize==INT32_MAX&&schema.cols[1].dataType=="bigint","cold full logical schema survives rewrites");Row many={{"id","777"}};for(int i=1;i<MAX_COLUMNS;++i)many["b"+std::to_string(i)]=std::nullopt;verify("max_lobs",{{777,many}});partitionVerify(true);}
}
int p29OriginalWorkerMain(int argc,char**argv){
 if(argc!=2){std::cerr<<"one explicit mode required\n";return 2;}
 try{dbms::TypeRegistry::instance().bootstrap();g_engine.setBackgroundIntervals(60000,60000);session.username="admin";session.permission=1;session.currentDB=db;dbms::setCurrentSession(&session);
  const std::string mode=argv[1];if(mode=="legacy-seed")seedLegacy();else if(mode=="legacy-update")updateLegacy();else if(mode=="legacy-seed-check")legacy(false);else if(mode=="legacy-updated-check")legacy(true);else if(mode=="multi")multi();else if(mode=="multi-cold")multiCold();else if(mode=="unlogged-overflow")unloggedOverflow();else if(mode=="unlogged-overflow-cold")unloggedCold(false);else if(mode=="unlogged-drop")unloggedCold(true);else if(mode=="unlogged-final-cold")unloggedFinal();else throw std::runtime_error("unknown fixed mode");
  std::cout<<"MULTILOB_COMPLETE mode="<<std::quoted(mode)<<" checks="<<checks<<" groups="<<groups<<"\n";return 0;
 }catch(const std::exception& error){std::cerr<<"MULTILOB_FIRST_FAILURE checks="<<checks<<" groups="<<groups<<" what="<<std::quoted(error.what())<<'\n';return 1;}
}

// END EXACT V4 WORKER
namespace {
std::string p29Read(const fs::path& path) {
 std::ifstream in(path,std::ios::binary);need(bool(in),"real closed stream read");std::string bytes{std::istreambuf_iterator<char>(in),std::istreambuf_iterator<char>()};need(!in.bad(),"complete real closed stream");return bytes;
}
struct P29Step {std::string mode,context;};
struct P29Observed {std::map<std::string,std::map<int,int64_t>> rows;std::map<int,std::pair<std::string,int64_t>> partition;size_t checks=0,groups=0;};
P29Observed p29Parse(const fs::path& output,const fs::path& error,const std::string& mode) {
 const auto text=p29Read(output),errors=p29Read(error);need(!std::regex_search(errors,std::regex("terminate called|bad_alloc|double.free|corrupted|shutdown.*(fail|error|exception)",std::regex::icase)),"no captured secondary shutdown error");
 need(text.find("MULTILOB_FIRST_FAILURE")==std::string::npos&&errors.find("MULTILOB_FIRST_FAILURE")==std::string::npos,"no worker semantic failure");
 P29Observed actual;size_t footers=0;std::istringstream lines(text);std::string line;
 while(std::getline(lines,line)) {
  std::istringstream in(line);std::string token;in>>token;
  if(token=="MULTILOB_VISIBLE_RIDS") {std::string table;size_t n=0;in>>std::quoted(table)>>n;std::map<int,int64_t> rows;
   for(size_t i=0;i<n;++i){int id=0;int64_t rid=0;in>>id>>rid;need(bool(in)&&rid>0&&rows.emplace(id,rid).second,"complete real visible RID record");}actual.rows[table]=std::move(rows);in>>std::ws;need(in.eof(),"RID record fully consumed");}
  else if(token=="MULTILOB_PARTITION_RIDS") {size_t n=0;in>>n;actual.partition.clear();for(size_t i=0;i<n;++i){int id=0;std::string path;int64_t rid=0;in>>id>>std::quoted(path)>>rid;need(bool(in)&&rid>0&&actual.partition.emplace(id,std::make_pair(path,rid)).second,"complete real fork RID record");}in>>std::ws;need(in.eof(),"fork RID record fully consumed");}
  else if(token=="MULTILOB_COMPLETE") {std::string got;in>>token;need(token.rfind("mode=",0)==0,"actual footer mode field");std::istringstream field(token.substr(5));field>>std::quoted(got);need(got==mode,"actual mode footer");in>>token;need(token.rfind("checks=",0)==0,"actual checks field");actual.checks=std::stoull(token.substr(7));in>>token;need(token.rfind("groups=",0)==0,"actual groups field");actual.groups=std::stoull(token.substr(7));++footers;}
 }
 need(footers==1&&actual.checks>0,"one complete actual worker footer");return actual;
}
P29Observed p29Spawn(const fs::path& exe,const fs::path& root,size_t index,const P29Step& step) {
 const auto cluster=root/step.context;fs::create_directory(cluster);need(fs::is_directory(cluster)&&!fs::is_symlink(cluster),"actual private worker cluster");
 const auto label=std::to_string(index)+"-"+step.mode;const auto out=root/(label+".stdout"),err=root/(label+".stderr");
 std::vector<std::string> environment;for(char** p=environ;*p;++p){const std::string e=*p;if(e.rfind("DBMS_",0)!=0&&e.rfind("PG",0)!=0)environment.push_back(e);}environment.push_back("DBMS_DATA_DIR="+cluster.string());
 std::vector<char*> env;for(auto& s:environment)env.push_back(s.data());env.push_back(nullptr);
 std::vector<std::string> arguments{exe.string(),step.mode};std::vector<char*> args;for(auto& s:arguments)args.push_back(s.data());args.push_back(nullptr);
 posix_spawn_file_actions_t actions;need(posix_spawn_file_actions_init(&actions)==0,"new owned spawn actions");need(posix_spawn_file_actions_addchdir_np(&actions,cluster.c_str())==0,"actual cwd before global engine construction");
 need(posix_spawn_file_actions_addopen(&actions,STDOUT_FILENO,out.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600)==0&&posix_spawn_file_actions_addopen(&actions,STDERR_FILENO,err.c_str(),O_WRONLY|O_CREAT|O_EXCL,0600)==0,"new complete child stdout/stderr");
 posix_spawnattr_t attributes;need(posix_spawnattr_init(&attributes)==0&&posix_spawnattr_setflags(&attributes,POSIX_SPAWN_SETPGROUP)==0&&posix_spawnattr_setpgroup(&attributes,0)==0,"owned child process group");pid_t pid=0;
 const int error=posix_spawn(&pid,exe.c_str(),&actions,&attributes,args.data(),env.data());posix_spawn_file_actions_destroy(&actions);posix_spawnattr_destroy(&attributes);need(error==0&&pid>0,"real self exec");
 struct Child {pid_t pid;bool reaped=false;~Child(){if(reaped)return;(void)kill(-pid,SIGKILL);int s=0;while(waitpid(pid,&s,0)<0&&errno==EINTR){}}} child{pid};
 std::string ticks;{std::ifstream stat("/proc/"+std::to_string(pid)+"/stat");std::string line;std::getline(stat,line);const auto end=line.rfind(')');need(end!=std::string::npos,"actual child start identity");std::istringstream fields(line.substr(end+2));for(int i=0;i<20;++i)fields>>ticks;need(bool(fields),"complete start_ticks field");}
 std::cout<<"MULTILOB_PERMANENT_CHILD index="<<index<<" mode="<<std::quoted(step.mode)<<" pid="<<pid<<" start_ticks="<<ticks<<" cwd="<<std::quoted(cluster.string())<<" stdout="<<std::quoted(out.string())<<" stderr="<<std::quoted(err.string())<<'\n'<<std::flush;
 int status=0;const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(180);
 for(;;){const auto w=waitpid(pid,&status,WNOHANG);if(w==pid){child.reaped=true;break;}need(w==0||(w<0&&errno==EINTR),"wait exact owned worker");need(std::chrono::steady_clock::now()<deadline,"worker timeout is first failure, not normal lifecycle");std::this_thread::sleep_for(std::chrono::milliseconds(5));}
 std::cout<<"MULTILOB_PERMANENT_CHILD_TERMINAL index="<<index<<" actual_status="<<status<<'\n'<<std::flush;
 need(WIFEXITED(status)&&WEXITSTATUS(status)==0,"actual normal child exit0 including global destructor");need(kill(pid,0)==-1&&errno==ESRCH&&kill(-pid,0)==-1&&errno==ESRCH,"actual worker PID/process group absent");
 return p29Parse(out,err,step.mode);
}
void p29Capture(const fs::path& root,size_t index,const P29Step& step) {
 const auto source=root/step.context;const auto phase=std::to_string(index)+"-"+step.mode;const auto destination=root/"images"/phase;
 need(!fs::exists(destination)&&fs::create_directories(destination),"new immutable closed worker phase");
 std::vector<fs::path> dirs,files;for(const auto& entry:fs::recursive_directory_iterator(source)) {
  const auto status=entry.symlink_status();need(!fs::is_symlink(status),"closed phase never follows symlink");const auto relative=entry.path().lexically_relative(source);need(!relative.empty()&&!relative.is_absolute(),"true owned relative phase path");for(const auto& c:relative)need(c!="..","closed phase stays owned");
  if(fs::is_directory(status))dirs.push_back(relative);else{need(fs::is_regular_file(status),"closed phase only real directories/files");files.push_back(relative);}
 }
 std::sort(dirs.begin(),dirs.end());std::sort(files.begin(),files.end());const auto manifest=root/(phase+"-retained-inventory-v1.txt");need(!fs::exists(manifest),"never overwrite closed inventory");std::ofstream inventory(manifest,std::ios::binary);need(bool(inventory),"actual closed inventory opens");
 inventory<<"MULTILOB_CLOSED_PHASE_V1 "<<index<<' '<<std::quoted(step.mode)<<' '<<std::quoted(step.context)<<' '<<files.size()<<' '<<dirs.size()<<'\n';
 for(const auto& relative:dirs){need(fs::create_directories(destination/relative)||fs::is_directory(destination/relative),"retain actual empty/nonempty directory");inventory<<"D "<<std::quoted(relative.generic_string())<<'\n';}
 uintmax_t total=0;size_t zero=0;std::array<char,65536> buffer{};
 for(const auto& relative:files){const auto inputPath=source/relative,outputPath=destination/relative;const auto n=fs::file_size(inputPath);need(!fs::exists(outputPath),"phase file is new");std::ifstream in(inputPath,std::ios::binary);std::ofstream out(outputPath,std::ios::binary);need(bool(in)&&bool(out),"actual source and new captured file");
  SHA256 digest;uintmax_t count=0;while(in){in.read(buffer.data(),buffer.size());const auto got=in.gcount();if(got>0){digest.append(buffer.data(),static_cast<size_t>(got));out.write(buffer.data(),got);count+=static_cast<uintmax_t>(got);}}
  need(in.eof()&&!in.bad()&&count==n&&fs::file_size(inputPath)==n,"complete stable source bytes including zeroB");out.close();need(!out.fail(),"captured output complete");const auto hash=digest.finishHex();
  SHA256 check;uintmax_t copied=0;std::ifstream reread(outputPath,std::ios::binary);need(bool(reread),"actual captured file readback");while(reread){reread.read(buffer.data(),buffer.size());const auto got=reread.gcount();if(got>0){check.append(buffer.data(),static_cast<size_t>(got));copied+=static_cast<uintmax_t>(got);}}
  need(reread.eof()&&!reread.bad()&&copied==n&&check.finishHex()==hash,"full captured bytes/SHA match source");total+=n;if(n==0)++zero;
  const auto image=outputPath.lexically_relative(root).generic_string();inventory<<"F "<<std::quoted(relative.generic_string())<<' '<<n<<' '<<std::quoted(hash)<<' '<<std::quoted(image)<<'\n';
  std::cout<<"MULTILOB_PERMANENT_PHASE_FILE phase="<<std::quoted(phase)<<" source_relative="<<std::quoted(relative.generic_string())<<" image_relative="<<std::quoted(image)<<" bytes="<<n<<" sha256="<<hash<<'\n';
 }
 inventory.close();need(!inventory.fail(),"closed actual inventory complete");std::cout<<"MULTILOB_PERMANENT_PHASE index="<<index<<" files="<<files.size()<<" dirs="<<dirs.size()<<" zeroB="<<zero<<" bytes="<<total<<" manifest="<<std::quoted(manifest.string())<<'\n'<<std::flush;
}
int p29Parent(const fs::path& exe) {
 std::string pattern=(fs::temp_directory_path()/"dbms-multilob-native-XXXXXX").string();std::vector<char> name(pattern.begin(),pattern.end());name.push_back(0);const char* made=mkdtemp(name.data());need(made,"new owned root");const auto root=fs::canonical(made);
 std::cout<<"MULTILOB_PERMANENT_OWNED_ROOT "<<std::quoted(root.string())<<'\n'<<std::flush;
 const std::vector<P29Step> steps={{"legacy-seed","legacy"},{"legacy-seed-check","legacy"},{"legacy-update","legacy"},{"legacy-updated-check","legacy"},{"multi","overflow"},{"multi-cold","overflow"},{"unlogged-overflow","unlogged"},{"unlogged-overflow-cold","unlogged"},{"unlogged-drop","unlogged"},{"unlogged-final-cold","unlogged"}};
 std::vector<P29Observed> actual;size_t workerChecks=0,workerGroups=0;
 for(size_t i=0;i<steps.size();++i){auto record=p29Spawn(exe,root,i+1,steps[i]);workerChecks+=record.checks;workerGroups+=record.groups;actual.push_back(std::move(record));p29Capture(root,i+1,steps[i]);}
 const std::vector<std::pair<size_t,size_t>> pairs={{0,1},{2,3},{4,5},{6,7}};
 for(const auto& pair:pairs){need(actual[pair.first].rows==actual[pair.second].rows&&actual[pair.first].partition==actual[pair.second].partition,"real complete closed/cold visible RIDs including physical fork owner");std::cout<<"MULTILOB_PERMANENT_COLD_RID_PAIR "<<pair.first+1<<' '<<pair.second+1<<" exact=1\n";}
 std::cout<<"MULTILOB_PERMANENT_COMPLETE workers="<<steps.size()<<" worker_checks="<<workerChecks<<" worker_groups="<<workerGroups<<" parent_checks="<<checks<<" all_actual_normal0=1 owned_root="<<std::quoted(root.string())<<'\n';return 0;
}
} // namespace
int main(int argc,char**argv) {
 if(argc!=1)return p29OriginalWorkerMain(argc,argv);
 try{return p29Parent(fs::canonical(argv[0]));}
 catch(const std::exception& error){std::cerr<<"MULTILOB_PERMANENT_FIRST_FAILURE parent_checks="<<checks<<" what="<<std::quoted(error.what())<<'\n';return 1;}
}
