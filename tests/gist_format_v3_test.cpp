// Standalone C++17 codec acceptance; no engine, heap, query or service.
#include "access/GiSTIndexFormat.h"
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <vector>
namespace f = dbms::gist_index_format;
namespace {
size_t checks=0,failures=0;
void need(bool ok,const std::string& label) {
    ++checks;
    if(!ok) { ++failures;std::cerr<<"FAIL "<<label<<'\n'; }
}
bool same(const std::vector<f::Entry>& a,const std::vector<f::Entry>& b) {
    if(a.size()!=b.size())return false;
    for(size_t i=0;i<a.size();++i)
        if(a[i].rid!=b[i].rid||a[i].low!=b[i].low||a[i].high!=b[i].high)return false;
    return true;
}
// Independent reflected Castagnoli polynomial, not the production helper.
uint32_t crc(std::string_view s) {
    uint32_t value=0xffffffffu;
    for(unsigned char byte:s) {
        value^=byte;
        for(int bit=0;bit<8;++bit)
            value=(value>>1)^((value&1)?0x82f63b78u:0u);
    }
    return ~value;
}
void integer(std::string& s,uint64_t value,size_t width) {
    for(size_t i=0;i<width;++i) s.push_back(static_cast<char>((value>>(8*i))&255));
}
void store(std::string& s,size_t position,uint64_t value,size_t width) {
    for(size_t i=0;i<width;++i)s.at(position+i)=static_cast<char>((value>>(8*i))&255);
}
std::string checksum(std::string s) {
    s.resize(s.size()-4);integer(s,crc(s),4);return s;
}
std::string independent(const std::vector<f::Entry>& entries,unsigned version) {
    std::string out("\0DBMSS\0",7);out.push_back(static_cast<char>('0'+version));
    integer(out,version,4);integer(out,entries.size(),8);
    for(const auto& entry:entries) {
        integer(out,entry.rid,8);integer(out,entry.low.size(),8);integer(out,entry.high.size(),8);
        out.append(entry.low);out.append(entry.high);
    }
    integer(out,crc(out),4);return out;
}
const std::vector<f::Entry> sentinel{{777,"unchanged-low","unchanged-high"}};
void bad(const std::string& bytes,const std::string& label) {
    auto output=sentinel;
    uint32_t version=0x89abcdefu;
    need(!f::decode(bytes,output,&version),label+" generic rejects");
    need(same(output,sentinel),label+" generic entries atomic");
    need(version==0x89abcdefu,label+" version atomic");
    output=sentinel;
    need(!f::decodeV2(bytes,output),label+" V2 rejects");
    need(same(output,sentinel),label+" V2 entries atomic");
    output=sentinel;
    need(!f::decodeV3(bytes,output),label+" V3 rejects");
    need(same(output,sentinel),label+" V3 entries atomic");
}
}
int main() {
    const std::vector<std::vector<f::Entry>> samples{
        {},
        {{1,"",""}},
        {{1,"",""},{2,"NULL","NULL"},{3,"a","z"},{4,u8"界é",u8"界😀"}},
        {{7,std::string("\0\x7f\n\"\\|",6),std::string("a\0b\xff",4)},
         {8,std::string(1024*1024,'x'),""}},
        // Codec preserves framing data; RID/order/null validity is caller-owned.
        {{0,"z","a"},{std::numeric_limits<uint64_t>::max(),"",""}}
    };
    need(f::kHeaderBytes==20&&f::kMinimumEntryBytes==24&&f::kChecksumBytes==4,"framing dimensions unchanged");
    for(size_t index=0;index<samples.size();++index) {
        const auto& sample=samples[index];
        const std::string name="sample "+std::to_string(index);
        const auto v2=f::encodeV2(sample),v3=f::encodeV3(sample);
        need(v2==independent(sample,2),name+" V2 independent bytes exact");
        need(v3==independent(sample,3),name+" V3 independent bytes exact");
        need(v2.size()==v3.size(),name+" same frame width");
        need(v2.substr(f::kHeaderBytes,v2.size()-f::kHeaderBytes-4)==v3.substr(f::kHeaderBytes,v3.size()-f::kHeaderBytes-4),name+" payload bytes unchanged");
        std::string payload;
        for(const auto& e:sample)f::appendEntryPayload(payload,e.rid,e.low,e.high);
        need(f::encodeV2(sample.size(),payload)==v2,name+" V2 overload agreement");
        need(f::encodeV3(sample.size(),payload)==v3,name+" V3 overload agreement");
        need(f::hasMagic(v2)&&!f::hasMagicV3(v2)&&f::hasMagicV3(v3)&&!f::hasMagic(v3),name+" strict magic separation");
        auto output=sentinel;uint32_t version=55;
        need(f::decodeV2(v2,output)&&same(output,sample),name+" strict V2 succeeds");
        output=sentinel;
        need(f::decodeV3(v3,output)&&same(output,sample),name+" strict V3 succeeds");
        output=sentinel;
        need(!f::decodeV2(v3,output)&&same(output,sentinel),name+" V2 rejects V3 atomically");
        need(!f::decodeV3(v2,output)&&same(output,sentinel),name+" V3 rejects V2 atomically");
        for(unsigned v=2;v<=3;++v) {
            const auto& bytes=v==2?v2:v3;
            output=sentinel;version=55;
            need(f::decode(bytes,output,&version)&&same(output,sample)&&version==v,name+" generic version "+std::to_string(v));
            output=sentinel;
            need(f::decode(bytes,output)&&same(output,sample),name+" generic optional version "+std::to_string(v));
        }
        // All cuts/byte mutations for small frames, bounded representatives for 1MiB.
        for(unsigned v=2;v<=3;++v) {
            const auto& bytes=v==2?v2:v3;
            if(bytes.size()<1024) {
                for(size_t cut=0;cut<bytes.size();++cut)bad(bytes.substr(0,cut),name+" truncation "+std::to_string(v)+"/"+std::to_string(cut));
                for(size_t at=0;at<bytes.size();++at) {
                    auto damaged=bytes;damaged[at]^=1;
                    bad(damaged,name+" CRC byte "+std::to_string(v)+"/"+std::to_string(at));
                }
            } else {
                for(size_t cut:{size_t(0),size_t(7),size_t(20),size_t(44),bytes.size()/2,bytes.size()-1})bad(bytes.substr(0,cut),name+" large truncation");
                for(size_t at:{size_t(0),size_t(8),size_t(12),size_t(44),bytes.size()/2,bytes.size()-1}) {
                    auto damaged=bytes;damaged[at]^=1;bad(damaged,name+" large CRC corruption");
                }
            }
        }
    }
    for(unsigned v=2;v<=3;++v) {
        const auto base=independent(samples[2],v);
        auto wrong=base;store(wrong,8,v==2?3:2,4);bad(checksum(wrong),"magic/version disagreement");
        wrong=base;store(wrong,8,4,4);bad(checksum(wrong),"unknown version");
        wrong=base;wrong[7]='4';store(wrong,8,4,4);bad(checksum(wrong),"unknown matching magic/version");
        wrong=base;store(wrong,12,std::numeric_limits<uint64_t>::max(),8);bad(checksum(wrong),"entry count overflow/extent");
        wrong=base;store(wrong,12,samples[2].size()-1,8);bad(checksum(wrong),"unconsumed entry tail");
        wrong=base;store(wrong,28,std::numeric_limits<uint64_t>::max(),8);bad(checksum(wrong),"low length overflow/extent");
        wrong=base;store(wrong,36,std::numeric_limits<uint64_t>::max(),8);bad(checksum(wrong),"high length overflow/extent");
        wrong=base;wrong.insert(wrong.size()-4,1,'x');bad(checksum(wrong),"checksum-valid trailing payload");
        wrong=base;wrong.append(1,'x');bad(wrong,"post-checksum trailing byte");
    }
    std::cout<<"GIST_FORMAT_V3_COMPONENT checks="<<checks<<" failures="<<failures<<" samples="<<samples.size()<<'\n';
    return failures?1:0;
}
