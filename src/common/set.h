#include<iostream>
typedef int ll;
const ll maxn=1000010;
class Chris{
    private:
    ll s[maxn][2],f[maxn],k[maxn],act[maxn],mct;bool rb[maxn];
    inline bool id(ll x){return s[f[x]][1]==x;}
    inline void update(ll x){act[x]=act[s[x][0]]+act[s[x][1]]+1;}
    inline void rotate(ll x){
        ll y=f[x],z=f[y],j=id(x),w=s[x][j^1];
        f[x]=z,s[z][id(y)]=x;
        f[w]=y,s[y][j]=w;
        f[y]=x,s[x][j^1]=y;
        update(y),update(x);
    }
    void INSERT(ll v){
        if(!s[0][1]){s[0][1]=++mct;k[mct]=v,act[mct]=1;return;}
        ll x=s[0][1],y;
        while(1){
            act[x]++;
            if(k[x]==v){while(x)act[x]--,x=f[x];return;}
            bool j=v>k[x];
            if(!s[x][j]){
                s[x][j]=++mct,f[mct]=x;
                k[mct]=v,rb[mct]=1;
                act[mct]=1;
                break;
            }
            x=s[x][j];
        }
        for(x=mct,y=f[x];y&&rb[x]&&rb[y];){
            ll z=f[y],j=id(y),w=s[z][j^1];
            if(!rb[w]){
                if(j!=id(x))rotate(x),y=x;
                rb[y]=0,rb[z]=1;
                rotate(y);return;
            }
            rb[y]=rb[w]=0,rb[z]=1;
            x=z,y=f[z];
        }
        rb[s[0][1]]=0;
    }
    void DELET(ll v){
        ll x=s[0][1];if(!x)return;
        while(k[x]!=v){
            act[x]--;
            ll t=s[x][v>k[x]];
            if(!t){while(x)act[x]++,x=f[x];return;}
            x=t;
        }
        act[x]--;
        if(s[x][0]&&s[x][1]){
            ll y=x;x=s[x][1];
            while(s[x][0])x=s[x][0];
            k[y]=k[x];
            for(ll t=f[x];t!=y;t=f[t])act[t]--;
        }
        ll j=id(x),y=f[x],t=s[x][0]+s[x][1];
        s[y][j]=t,f[t]=y,rb[t]=0;
        if(rb[x]||t)return;
        while(x!=s[0][1]){
            ll w=s[y][j^1],l=s[w][j],r=s[w][j^1];
            if(rb[w]){rb[y]=1,rb[w]=0,rotate(w),w=l,l=s[w][j],r=s[w][j^1];}
            if(rb[l]&&!rb[r])rb[w]=1,rb[l]=0,rotate(l),r=w,w=l,l=s[w][j];
            if(rb[r]){rb[w]=rb[y],rb[y]=rb[r]=0,rotate(w);return;}
            rb[w]=1;
            if(rb[y]){rb[y]=0;return;}
            x=y,y=f[y],j=id(x);
        }
    }
    void DFS(ll x){
        if(!x)return;
        DFS(s[x][0]);
        std::cout<<k[x]<<' '<<std::ends;
        DFS(s[x][1]);
    }
    ll RANK(ll v){
        ll x=s[0][1],otc=0;
        while(x){
            if(v<k[x]){x=s[x][0];continue;}
            otc+=act[s[x][0]];
            if(v==k[x])return otc+1;
            otc++,x=s[x][1];
        }
        return 0;
    }
    ll KTH(ll v){
        ll x=s[0][1];
        while(x){
            if(v<=act[s[x][0]]){x=s[x][0];continue;}
            v-=act[s[x][0]];
            if(v<=1)return k[x];
            v--,x=s[x][1];
        }
        return 0;
    }
    ll PRE(ll v){
        ll otc=0x80000000,x=s[0][1];
        while(x){
            if(k[x]<v){otc=otc<k[x]?k[x]:otc;x=s[x][1];}
            else x=s[x][0];
        }
        return otc;
    }
    ll NXT(ll v){
        ll otc=0x7FFFFFFF,x=s[0][1];
        while(x){
            if(k[x]>v){otc=otc>k[x]?k[x]:otc;x=s[x][0];}
            else x=s[x][1];
        }
        return otc;
    }
    bool HAS(ll v){
        ll x=s[0][1];
        while(k[x]!=v&&x)x=s[x][v>k[x]];
        return x;
    }
    public:
    void insert(ll v){INSERT(v);}
    void delet(ll v){DELET(v);}
    void dfs(){DFS(s[0][1]);putchar(10);}
    ll last(ll v){return PRE(v);}
    ll next(ll v){return NXT(v);}
    ll rank(ll v){return RANK(v);}
    ll kth(ll v){return KTH(v);}
    ll size(){return act[s[0][1]];}
    bool has(ll v){return HAS(v);}
};