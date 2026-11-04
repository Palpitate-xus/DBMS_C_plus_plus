/*-------------------------------------------------------------------------
 *
 * sha2.c
 *	   SHA functions for SHA-224, SHA-256, SHA-384 and SHA-512.
 *
 * This includes the fallback implementation for SHA2 cryptographic
 * hashes.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/common/sha2.c
 *
 *-------------------------------------------------------------------------
 */

/*	$OpenBSD: sha2.c,v 1.6 2004/05/03 02:57:36 millert Exp $	*/
/*
 * FILE:	sha2.c
 * AUTHOR:	Aaron D. Gifford <me@aarongifford.com>
 *
 * Copyright (c) 2000-2001, Aaron D. Gifford
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *	  notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *	  notice, this list of conditions and the following disclaimer in the
 *	  documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of contributors
 *	  may be used to endorse or promote products derived from this software
 *	  without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTOR(S) ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTOR(S) BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * $From: sha2.c,v 1.1 2001/11/08 00:01:51 adg Exp adg $
 */


// Ordinary backup-manifest CREATION only. Not a security/auth/SQL hash replacement.
// Portable C++17 adaptation of PG REL_18_6 SHA-256 unrolled compression and
// full-block streaming. Preserve this source's copyright/license notices.
// Reference: https://github.com/postgres/postgres/blob/REL_18_6/src/common/sha2.c
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
namespace dbms::backup_manifest_detail {
class ManifestCreationSha256 {
    inline static constexpr std::uint32_t constants_[64]={
	0x428a2f98UL, 0x71374491UL, 0xb5c0fbcfUL, 0xe9b5dba5UL,
	0x3956c25bUL, 0x59f111f1UL, 0x923f82a4UL, 0xab1c5ed5UL,
	0xd807aa98UL, 0x12835b01UL, 0x243185beUL, 0x550c7dc3UL,
	0x72be5d74UL, 0x80deb1feUL, 0x9bdc06a7UL, 0xc19bf174UL,
	0xe49b69c1UL, 0xefbe4786UL, 0x0fc19dc6UL, 0x240ca1ccUL,
	0x2de92c6fUL, 0x4a7484aaUL, 0x5cb0a9dcUL, 0x76f988daUL,
	0x983e5152UL, 0xa831c66dUL, 0xb00327c8UL, 0xbf597fc7UL,
	0xc6e00bf3UL, 0xd5a79147UL, 0x06ca6351UL, 0x14292967UL,
	0x27b70a85UL, 0x2e1b2138UL, 0x4d2c6dfcUL, 0x53380d13UL,
	0x650a7354UL, 0x766a0abbUL, 0x81c2c92eUL, 0x92722c85UL,
	0xa2bfe8a1UL, 0xa81a664bUL, 0xc24b8b70UL, 0xc76c51a3UL,
	0xd192e819UL, 0xd6990624UL, 0xf40e3585UL, 0x106aa070UL,
	0x19a4c116UL, 0x1e376c08UL, 0x2748774cUL, 0x34b0bcb5UL,
	0x391c0cb3UL, 0x4ed8aa4aUL, 0x5b9cca4fUL, 0x682e6ff3UL,
	0x748f82eeUL, 0x78a5636fUL, 0x84c87814UL, 0x8cc70208UL,
	0x90befffaUL, 0xa4506cebUL, 0xbef9a3f7UL, 0xc67178f2UL
};
    std::array<std::uint32_t,8> state_={
	0x6a09e667UL,
	0xbb67ae85UL,
	0x3c6ef372UL,
	0xa54ff53aUL,
	0x510e527fUL,
	0x9b05688cUL,
	0x1f83d9abUL,
	0x5be0cd19UL
};
    std::array<std::uint8_t,64> tail_{};
    std::uint64_t bitCount_=0;
    std::size_t used_=0;
    static constexpr std::uint32_t rotate(std::uint32_t x,unsigned n) noexcept {return (x>>n)|(x<<(32-n));}
    static constexpr std::uint32_t big0(std::uint32_t x) noexcept{return rotate(x,2)^rotate(x,13)^rotate(x,22);}
    static constexpr std::uint32_t big1(std::uint32_t x) noexcept{return rotate(x,6)^rotate(x,11)^rotate(x,25);}
    static constexpr std::uint32_t small0(std::uint32_t x) noexcept{return rotate(x,7)^rotate(x,18)^(x>>3);}
    static constexpr std::uint32_t small1(std::uint32_t x) noexcept{return rotate(x,17)^rotate(x,19)^(x>>10);}
    static constexpr std::uint32_t choose(std::uint32_t x,std::uint32_t y,std::uint32_t z) noexcept{return (x&y)^(~x&z);}
    static constexpr std::uint32_t majority(std::uint32_t x,std::uint32_t y,std::uint32_t z) noexcept{return (x&y)^(x&z)^(y&z);}
    void transform(const std::uint8_t* data) noexcept {
        // Real uint32_t schedule storage avoids typed aliasing into a byte array.
        std::uint32_t words[16];
        std::uint32_t a=state_[0],b=state_[1],c=state_[2],d=state_[3];
        std::uint32_t e=state_[4],f=state_[5],g=state_[6],h=state_[7];
        std::uint32_t temp,s0,s1;unsigned j=0;
#define DBMS_BACKUP_MANIFEST_FIRST(A,B,C,D,E,F,G,H) do { \
        words[j]=std::uint32_t(data[3])|(std::uint32_t(data[2])<<8)|(std::uint32_t(data[1])<<16)|(std::uint32_t(data[0])<<24); \
        data+=4; temp=(H)+big1(E)+choose((E),(F),(G))+constants_[j]+words[j]; \
        (D)+=temp;(H)=temp+big0(A)+majority((A),(B),(C));++j; \
    }while(false)
#define DBMS_BACKUP_MANIFEST_LATER(A,B,C,D,E,F,G,H) do { \
        s0=small0(words[(j+1)&15]);s1=small1(words[(j+14)&15]); \
        temp=(H)+big1(E)+choose((E),(F),(G))+constants_[j]+(words[j&15]+=s1+words[(j+9)&15]+s0); \
        (D)+=temp;(H)=temp+big0(A)+majority((A),(B),(C));++j; \
    }while(false)
        do {
            DBMS_BACKUP_MANIFEST_FIRST(a,b,c,d,e,f,g,h);
            DBMS_BACKUP_MANIFEST_FIRST(h,a,b,c,d,e,f,g);
            DBMS_BACKUP_MANIFEST_FIRST(g,h,a,b,c,d,e,f);
            DBMS_BACKUP_MANIFEST_FIRST(f,g,h,a,b,c,d,e);
            DBMS_BACKUP_MANIFEST_FIRST(e,f,g,h,a,b,c,d);
            DBMS_BACKUP_MANIFEST_FIRST(d,e,f,g,h,a,b,c);
            DBMS_BACKUP_MANIFEST_FIRST(c,d,e,f,g,h,a,b);
            DBMS_BACKUP_MANIFEST_FIRST(b,c,d,e,f,g,h,a);
        } while(j<16);
        do {
            DBMS_BACKUP_MANIFEST_LATER(a,b,c,d,e,f,g,h);
            DBMS_BACKUP_MANIFEST_LATER(h,a,b,c,d,e,f,g);
            DBMS_BACKUP_MANIFEST_LATER(g,h,a,b,c,d,e,f);
            DBMS_BACKUP_MANIFEST_LATER(f,g,h,a,b,c,d,e);
            DBMS_BACKUP_MANIFEST_LATER(e,f,g,h,a,b,c,d);
            DBMS_BACKUP_MANIFEST_LATER(d,e,f,g,h,a,b,c);
            DBMS_BACKUP_MANIFEST_LATER(c,d,e,f,g,h,a,b);
            DBMS_BACKUP_MANIFEST_LATER(b,c,d,e,f,g,h,a);
        } while(j<64);
#undef DBMS_BACKUP_MANIFEST_FIRST
#undef DBMS_BACKUP_MANIFEST_LATER
        state_[0]+=a;state_[1]+=b;state_[2]+=c;state_[3]+=d;
        state_[4]+=e;state_[5]+=f;state_[6]+=g;state_[7]+=h;
    }
public:
    // Single-use per file, like the original SHA256 append()/finishHex() caller.
    void append(const char* input,std::size_t length) noexcept {
        if(length==0)return;
        const auto* data=reinterpret_cast<const std::uint8_t*>(input);
        bitCount_+=static_cast<std::uint64_t>(length)*8;
        if(used_!=0){
            const std::size_t free=64-used_;
            const std::size_t copied=length<free?length:free;
            std::memcpy(tail_.data()+used_,data,copied);
            used_+=copied;data+=copied;length-=copied;
            if(used_!=64)return;
            transform(tail_.data());used_=0;
        }
        while(length>=64){transform(data);data+=64;length-=64;}
        if(length!=0){std::memcpy(tail_.data(),data,length);used_=length;}
    }
    std::string finishHex(){
        const std::uint64_t originalBits=bitCount_;
        tail_[used_++]=0x80;
        if(used_>56){std::memset(tail_.data()+used_,0,64-used_);transform(tail_.data());used_=0;}
        std::memset(tail_.data()+used_,0,56-used_);
        for(unsigned i=0;i<8;++i)tail_[56+i]=static_cast<std::uint8_t>(originalBits>>(56-8*i));
        transform(tail_.data());used_=0;
        static constexpr char hex[]="0123456789abcdef";
        std::string output(64,'0');
        for(unsigned i=0;i<8;++i)for(unsigned n=0;n<8;++n)output[i*8+n]=hex[(state_[i]>>(28-4*n))&15];
        return output;
    }
};
} // namespace dbms::backup_manifest_detail
