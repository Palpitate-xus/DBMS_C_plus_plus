# R182：共享UTF-8拆分的重复前缀扫描

状态：Open，未修复、未验收。性能候选在私树，Root没有本项生产修改或注册回归。

最新：R183已独立完成回调身份修复及两端21入口验收；本项性能候选仍未接受。
必须从新accepted版本创建R182 V2，透明修正错误ARRAY期望并保留严格opaque断言；
原V1及-6失败日志不得改写，新的完整native76/协议329及两端门禁仍需真实运行。

## 已定位问题与修改方法

| 路径 | 原因 | 方法 |
| --- | --- | --- |
| TRIM/LTRIM/RTRIM输入和字符集合 | helper逐字符两次utf8ByteAt重新扫描前缀 | 一次正向扫描完整UTF-8码点 |
| LPAD/RPAD填充串 | 重复实现同一平方级拆分 | 使用同一helper，保留原长度/NULL/截断/大小限制 |
| TRANSLATE输入、from、to | 私有lambda分别重复前缀扫描 | 统一helper，保留重复键首次优先与缺失目标删除 |
| STRING_TO_ARRAY(NULL delimiter) | 同样逐字符查找 | 使用同一helper，保持空数组/null_string/转义规则 |

只针对拆分O(n²)，未宣称字符集合/翻译查找也线性；公共头/AST/API及BYTEA分支不变。
PG18和冻结旧accepted R181各完整329实际0，值与元数据本来正确，不称兼容性错值。
329包含三协议、八种大参数角色各三种大小、精确TEXT帧、真实表/参数/NULL和执行副作用。
耗时为观测，无通过阈值。旧版Simple最大规模的各参数角色多为秒级。

## 未通过的原生检查：不得抹去

V1原生夹具把STRING_TO_ARRAY内部callback原始ARRAY标记误当公开输出，
但现有公开eval会规范化为text[]，九个可见失败标签来自这个错误期望。
最后一个opaque替换返回TEXT opaque，却被错误按builtin数组返回声明强转，抛出22P02；
因为未捕获异常，真实exit -6，无完整76 footer。不能称完整76检查通过或把九个标签当九个产品bug。
这后一项独立实际问题记为[R183](issue-review-r183-string-to-array-callback-identity.md)。

原fixture、首轮编译器receipt、全部旧日志保留，不在原版本上改预期、删检查或声称候选通过。
后续需新fixture版本纠正错误的ARRAY期望，并继续严格要求opaque callback不继承数组声明；
先完成R183，再重建独立R182候选，完整PG/old/clean/Root及相邻入口验收后才能接受。

## 固定证据与边界

私树：/tmp/dbms-r182-utf8-split-candidate-v1.bsZaIV58；已构建，未运行新产品验收。
R182 Root未安装，也未commit生产修复或新增接受测试。R180等用户暂缓不恢复，R164仍开放。
不push，Actions禁用；原273不因本项变更状态。

`/tmp/dbms-review-r46.ifu6SWLp/r182-reference329.log`：`af6fd58ed0b0ecf6acb7df5b678125cc7d90994cd8bfa6665e2f52227fc9a23b`。

`/tmp/dbms-review-r46.ifu6SWLp/r182-old329.log`：`bc3c0c6669ebdf6bb20a2c59d329284a65690f7ca78c46aeb8307d5e8a0b7683`。

`/tmp/dbms-review-r46.ifu6SWLp/r182-old-native76.log`：`234019d424431a5f0898f4d47249f8868f0c6a71cff2e3e33060fd0b62491a76`。

`/tmp/dbms-review-r46.ifu6SWLp/r182-old-native-driver.log`：`8c85dfbec43ce19696632b74e19c1995979c890e3c2b582d142ad6c3e8dfccbc`。

候选编译/链接实际0（非运行通过）：`/tmp/dbms-r182-clean-compiler.33hvksib/completed-compiler-receipt.json`，`d01d34690c2e74adf7a1b50acee7ef5d1bf73b972b033e1d635fb217e71a896b`。
