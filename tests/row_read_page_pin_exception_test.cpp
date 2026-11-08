// Run only in an isolated CWD: test_stubs.cpp constructs a global engine
// before main, and that constructor may inspect or recover databases there.
#include "catalog/type_registry.h"
#include "commands/TableManage.h"
#include "commands/HeapScanCursor.h"
#include "storage/PageAllocator.h"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <new>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

static thread_local bool failNext = false;
void* operator new(std::size_t n) { if(failNext){failNext=false;throw std::bad_alloc();} if(void* p=std::malloc(n?n:1))return p;throw std::bad_alloc(); }
void* operator new[](std::size_t n) {return ::operator new(n);}
void operator delete(void* p) noexcept {std::free(p);}
void operator delete[](void* p) noexcept {std::free(p);}
void operator delete(void* p,std::size_t) noexcept {std::free(p);}
void operator delete[](void* p,std::size_t) noexcept {std::free(p);}

using namespace dbms;
static void need(bool ok,const char* message) {if(!ok)throw std::runtime_error(message);}
static int pins(PageAllocator* pa,uint32_t page) {
  for(const auto& info:pa->bufferPool()->getFrameInfo()) if(info.valid && info.pageId==page)return info.pinCount;
  return -1;
}
// Only an actual successful CREATE grants this object cleanup ownership.
// Verify the physical directory remains that same creation before DROP;
// never remove a collision or another directory substituted at the name.
class OwnedTestDatabase {
public:
 explicit OwnedTestDatabase(StorageEngine& engine):engine_(engine) {
  const auto tick=std::chrono::steady_clock::now().time_since_epoch().count();
  for(unsigned i=0;i<32;++i) {
   name_="__t_row_pin_"+std::to_string(static_cast<unsigned long>(::getpid()))+"_"+
         std::to_string(tick)+"_"+std::to_string(i);
   const auto status=engine_.createDatabase(name_);
   if(status==DBStatus::TABLE_ALREADY_EXISTS)continue;
   need(status==DBStatus::OK,"create private database");
   struct stat info{};
   need(::lstat(name_.c_str(),&info)==0 && S_ISDIR(info.st_mode),"created directory identity");
   device_=info.st_dev;inode_=info.st_ino;owned_=true;return;
  }
  throw std::runtime_error("cannot obtain unique private database name");
 }
 OwnedTestDatabase(const OwnedTestDatabase&)=delete;
 OwnedTestDatabase& operator=(const OwnedTestDatabase&)=delete;
 const std::string& name() const {return name_;}
 bool cleanup() noexcept {
  if(!owned_)return true;
  failNext=false;
  try {
   struct stat info{};
   if(::lstat(name_.c_str(),&info)!=0 || !S_ISDIR(info.st_mode) ||
      info.st_dev!=device_ || info.st_ino!=inode_)return false;
   if(engine_.inTransaction() && engine_.transactionDatabase()==name_ &&
      engine_.rollbackTransaction()!=DBStatus::OK)return false;
   if(engine_.dropDatabase(name_)!=DBStatus::OK)return false;
   owned_=false;return true;
  } catch(...) {return false;}
 }
 ~OwnedTestDatabase() {if(!cleanup())std::cerr<<"FAIL fixture cleanup\n";}
private:
 StorageEngine& engine_;std::string name_;dev_t device_{};ino_t inode_{};bool owned_=false;
};
int main(int argc,char** argv) {
 try {
  need(::setenv("DBMS_BUFFER_FRAMES","16",1)==0,"set buffer frames");
  TypeRegistry::instance().bootstrap(); StorageEngine engine; engine.setBackgroundIntervals(60000,60000);
  OwnedTestDatabase owner(engine); const std::string& db=owner.name();
  TableSchema table; table.tablename="rows"; table.formatVersion=DATA_FILE_FORMAT_VERSION;
  table.append(makeIntColumn("id",false,2)); table.append(makeTextColumn("payload",true));
  need(engine.createTable(db,table)==DBStatus::OK,"create table");
  need(engine.beginTransaction(db)==DBStatus::OK,"begin");
  for(int i=0;i<400;++i) {
   const std::optional<std::string> value=(i%5==0)?std::nullopt:std::optional<std::string>(std::string(400,char('A'+i%26))+std::to_string(i));
   need(engine.insertRow(db,"rows",{{"id",std::to_string(i)},{"payload",value}})==DBStatus::OK,"insert");
  }
  need(engine.commitTransaction()==DBStatus::OK,"commit");
  table=engine.getTableSchema(db,"rows"); auto* pa=engine.getPageAllocator(db,"rows");need(pa,"allocator");
  need(pa->bufferPool()->getFrameInfo().size()==16,"configured buffer frames");
  auto scan=engine.beginHeapScan(db,"rows",table);HeapScanRow item;std::vector<HeapScanRow> expected;
  while(scan->next(item))expected.push_back(std::move(item));
  scan.reset();
  need(expected.size()==400,"scan count");std::set<uint32_t> pages;
  for(const auto& row:expected){uint32_t p;uint16_t s;StorageEngine::decodeRid(row.rid,p,s);pages.insert(p);}
  need(pages.size()>16,"fixture must exceed configured 16-frame cache");
  for(size_t j=0;j<expected.size();++j) {
   const size_t pos=(j%2)?(expected.size()-1-j/2):(j/2);const auto& reference=expected[pos];
   std::string actual;std::vector<bool> nulls;
   need(engine.readRowByRid(pa,reference.rid,actual,table,&nulls),"read valid rid");
   need(actual==reference.data && nulls==reference.nulls,"payload/bitmap identity");
  }
  uint32_t page;uint16_t slot;StorageEngine::decodeRid(expected.front().rid,page,slot);
  std::string sentinel="unchanged";std::vector<bool> nullSentinel{true};
  need(!engine.readRowByRid(pa,StorageEngine::encodeRid(page,65534),sentinel,table,&nullSentinel),"missing slot");
  need(sentinel=="unchanged" && nullSentinel==std::vector<bool>{true},"missing outputs");
  std::cout<<"normal_rows="<<expected.size()<<" distinct_pages="<<pages.size()<<" frames="<<pa->bufferPool()->getFrameInfo().size()<<std::endl;
  if(argc==1 || (argc==2 && std::string(argv[1])=="visible")) {
   const auto visibleRow=std::find_if(expected.begin(),expected.end(),[](const auto& value){return !value.nulls[1] && value.data.size()>32;});
   need(visibleRow!=expected.end(),"no non-NULL visible payload");const auto& row=*visibleRow;
   uint32_t visiblePage;uint16_t visibleSlot;StorageEngine::decodeRid(row.rid,visiblePage,visibleSlot);
   StorageEngine::ReadView view;view.lowLimitId=std::numeric_limits<uint64_t>::max();
   std::string warm;need(engine.readVisibleRowByRid(db,pa,row.rid,warm,table,&view),"warm visible row");need(warm==row.data,"visible row content");
   const int before=pins(pa,visiblePage);need(before==0,"visible warm pin balance");
   std::string target;bool threw=false;failNext=true;
   try {engine.readVisibleRowByRid(db,pa,row.rid,target,table,&view);}catch(const std::bad_alloc&){threw=true;}
   failNext=false;const int after=pins(pa,visiblePage);
   std::cout<<"visible_failure bad_alloc="<<threw<<" pin_before="<<before<<" pin_after="<<after<<std::endl;
   if(after>before)for(int i=before;i<after;++i)pa->unpinPage(visiblePage);
   need(threw,"visible payload allocation not reached");need(before==after,"visible exception leaked page pin");
   need(engine.readVisibleRowByRid(db,pa,row.rid,target,table,&view)&&target==row.data,"visible retry");
   std::cout<<"VISIBLE_PIN_TEST_PASS\n";if(argc==2){need(owner.cleanup(),"private fixture cleanup");return 0;}
  }
  std::string warm;std::vector<bool> warmNulls;
  need(engine.readRowByRid(pa,expected.front().rid,warm,table,&warmNulls),"warm target");
  const int before=pins(pa,page);need(before==0,"warm pin balance");
  std::string unchanged="unchanged";std::vector<bool> empty;
  bool threw=false; failNext=true;
  try {engine.readRowByRid(pa,expected.front().rid,unchanged,table,&empty);}catch(const std::bad_alloc&){threw=true;}
  failNext=false;const int after=pins(pa,page);
  std::cout<<"bitmap_failure bad_alloc="<<threw<<" pin_before="<<before<<" pin_after="<<after<<std::endl;
  if(after>before)for(int i=before;i<after;++i)pa->unpinPage(page);
  need(threw,"bitmap allocation not reached");need(before==after,"bitmap exception leaked page pin");
  const auto longRowIt=std::find_if(expected.begin(),expected.end(),[](const auto& row){return !row.nulls[1] && row.data.size()>32;});
  need(longRowIt!=expected.end(),"no non-NULL long payload");const auto& longRow=*longRowIt;
  StorageEngine::decodeRid(longRow.rid,page,slot);std::string cached;need(engine.readRowByRid(pa,longRow.rid,cached,table),"warm long row");
  const int oldPins=pins(pa,page);std::string target;bool payloadThrew=false;failNext=true;
  try {engine.readRowByRid(pa,longRow.rid,target,table);}catch(const std::bad_alloc&){payloadThrew=true;}
  failNext=false;const int newPins=pins(pa,page);
  std::cout<<"payload_failure bad_alloc="<<payloadThrew<<" pin_before="<<oldPins<<" pin_after="<<newPins<<std::endl;
  if(newPins>oldPins)for(int i=oldPins;i<newPins;++i)pa->unpinPage(page);
  need(payloadThrew,"payload allocation not reached");need(oldPins==newPins,"payload exception leaked page pin");
  std::string retry;std::vector<bool> retryNull;
  need(engine.readRowByRid(pa,longRow.rid,retry,table,&retryNull)&&retry==longRow.data&&retryNull==longRow.nulls,"retry");
  need(owner.cleanup(),"private fixture cleanup");
  std::cout<<"PIN_TEST_PASS rows="<<expected.size()<<" pages="<<pages.size()<<std::endl;return 0;
 } catch(const std::exception& e){failNext=false;std::cerr<<"FAIL "<<e.what()<<std::endl;return 1;}
}
