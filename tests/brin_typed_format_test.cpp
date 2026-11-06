#include "access/BrinIndexFormat.h"
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <sys/mman.h>
#include <unistd.h>

namespace {
size_t checks=0;
void need(bool value,const char* label){++checks;if(!value)throw std::runtime_error(label);}
void integer(std::string& out,uint64_t value,unsigned bytes){for(unsigned i=0;i<bytes;++i)out.push_back(char(value>>(i*8)));}
void text(std::string& out,const std::string& value){integer(out,value.size(),8);out+=value;}
uint32_t scalarCRC(const std::string& bytes){uint32_t state=0xffffffffu;for(unsigned char byte:bytes){state^=byte;for(unsigned j=0;j<8;++j)state=(state>>1)^((state&1)?0x82f63b78u:0u);}return state^0xffffffffu;}
void sign(std::string& bytes){bytes.resize(bytes.size()-4);integer(bytes,scalarCRC(bytes),4);}
std::string golden(){
 std::string b;integer(b,0x5342524e,4);integer(b,3,4);integer(b,2,8);
 integer(b,4,4);integer(b,16,4);integer(b,0,4);integer(b,71,8);text(b,std::string("schema\0identity",15));
 integer(b,1,4);integer(b,4,4);integer(b,3,4);text(b,"2");text(b,"10");
 integer(b,5,4);integer(b,8,4);integer(b,2,4);text(b,"");text(b,"");
 integer(b,scalarCRC(b),4);return b;
}
}
int main()try{
 using namespace dbms::brin_index_format;
 TypedIndex model;model.pagesPerRange=4;model.heapPages=16;model.physicalRelation=71;
 model.orderingIdentity=std::string("schema\0identity",15);model.ranges={{1,4,3,"2","10"},{5,8,2,"",""}};
 const auto bytes=golden();need(encodeTyped(model)==bytes,"independent little-endian complete golden frame and bitwise CRC");
 TypedIndex parsed;need(decodeTyped(bytes,parsed),"golden decode");need(parsed.pagesPerRange==4&&parsed.heapPages==16&&parsed.physicalRelation==71&&parsed.orderingIdentity==model.orderingIdentity&&parsed.ranges.size()==2,"complete typed header");
 need(parsed.ranges[0].min=="2"&&parsed.ranges[0].max=="10"&&parsed.ranges[0].flags==3&&parsed.ranges[1].flags==2,"numeric strings and NULL are separate state");
 for(size_t n=0;n<bytes.size();++n)need(!decodeTyped(std::string_view(bytes.data(),n),parsed),"every truncation rejected");
 for(size_t i=0;i<bytes.size();++i){auto bad=bytes;bad[i]^=1;need(!decodeTyped(bad,parsed),"every one-byte checksum damage rejected");}
 need(!decodeTyped(bytes+std::string(1,'\0'),parsed),"trailing byte rejected");
 for(const auto offset:{size_t(0),size_t(4),size_t(16),size_t(20),size_t(24),size_t(36)}){
  auto bad=bytes;for(unsigned i=0;i<4;++i)bad[offset+i]=char(offset==16||offset==20?0:0xff);sign(bad);need(!decodeTyped(bad,parsed),"valid-CRC invalid header rejected");
 }
 auto bad=bytes;for(unsigned i=0;i<8;++i)bad[8+i]=char(0xff);sign(bad);need(!decodeTyped(bad,parsed),"overflowing range count rejected");
 model.ranges[0].flags=8;need(!decodeTyped(encodeTyped(model),parsed),"unknown range flag rejected");
 model.ranges[0].flags=4;need(!decodeTyped(encodeTyped(model),parsed),"unbounded requires non-NULL state");
 model.ranges[0].flags=2;need(!decodeTyped(encodeTyped(model),parsed),"NULL-only range cannot carry extrema");
 model.ranges[0].flags=3;model.ranges[1].first=4;need(!decodeTyped(encodeTyped(model),parsed),"overlapping physical ranges rejected");
 model.flags=kPartitioned;need(decodeTyped(encodeTyped(model),parsed),"partition diagnostic format explicitly marked");
 model.flags=0;model.ranges.clear();need(decodeTyped(encodeTyped(model),parsed)&&parsed.ranges.empty(),"genuine empty heap index");
 model.orderingIdentity.clear();need(!decodeTyped(encodeTyped(model),parsed),"missing ordering identity rejected");
 uint64_t random=0x6272696e32303236ull;
 for(size_t attempt=0;attempt<4096;++attempt){
  random^=random<<13;random^=random>>7;random^=random<<17;
  auto altered=bytes;altered[random%(altered.size()-4)]^=char(1u<<((random>>16)%8));sign(altered);
  if(decodeTyped(altered,parsed)){
   need(encodeTyped(parsed)==altered,"valid mutated frame has exact canonical roundtrip");
   need(parsed.heapPages>0&&parsed.pagesPerRange>0&&!parsed.orderingIdentity.empty(),"accepted frame header invariants");
  }else need(true,"invalid valid-CRC mutation rejected");
 }
 const auto page=static_cast<size_t>(sysconf(_SC_PAGESIZE));
 void* memory=mmap(nullptr,page*2,PROT_READ|PROT_WRITE,MAP_PRIVATE|MAP_ANONYMOUS,-1,0);need(memory!=MAP_FAILED,"owned guard pages");
 need(mprotect(static_cast<char*>(memory)+page,page,PROT_NONE)==0,"protected successor page");
 char* tail=static_cast<char*>(memory)+page-bytes.size();std::memcpy(tail,bytes.data(),bytes.size());
 need(decodeTyped(std::string_view(tail,bytes.size()),parsed),"valid frame at guard-page boundary");
 need(!decodeTyped(std::string_view(static_cast<char*>(memory)+page,0),parsed),"zero length at inaccessible page");
 need(munmap(memory,page*2)==0,"guard page ownership released");
 std::cout<<"BRIN_TYPED_FORMAT checks="<<checks<<" failures=0\n";return 0;
}catch(const std::exception& error){std::cerr<<"BRIN_TYPED_FORMAT_FAIL checks="<<checks<<' '<<error.what()<<'\n';return 1;}
