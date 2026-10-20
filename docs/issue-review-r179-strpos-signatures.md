# R179：STRPOS声明签名、错误阶段与同名覆盖

状态：有限修复，已完成双侧定向验收；不关闭整个FUNC-01/PROTO-04或原273总目标。
基线为R178源码提交`1430b578`及发现文档`9bc172b9`；[R180](issue-review-r180-position-call-identity.md)另列未修复。

## 实际问题与修改方法

| 问题 | 旧产品/PG实际差异 | 已实现的修改方法 |
| --- | --- | --- |
| INTEGER、UUID NULL、BYTEA等被错误接受 | 旧返回1/NULL/0，PG42883 | 只给真实STRPOS callback绑定TEXT/TEXT→INTEGER声明，拒绝无隐式转换的输入 |
| 缺失/多余实参及named arguments | 旧返回NULL或忽略多余实参，PG42883 | 准备阶段精确验证arity/声明，不因NULL或零行绕过 |
| 空表/WHERE false/LIMIT0及未选分支 | 多处未报分析错误 | Main与Parse前置接入同一纯查询绑定，不执行输入猜签名 |
| 同名INTEGER/INTEGER用户重载 | 旧部分走builtin返回0，描述也可误为TEXT；PG103/int4 | 从实际namespace和typed声明选择callee，metadata与执行一致 |
| quoted大写STRPOS不存在 | 旧extended/binary在Execute才报42883，PG Parse失败 | 名称族只负责进入分析；实际quoted/schema身份不折叠、不改写 |
| UNKNOWN wire参数 | 不能由执行值或NULL SQL猜类型 | 从绑定声明及真实源参数位置推断两个OID25，再验证最终共享声明 |
| native registerFunction替换 | 不能继承同名builtin的类型/arity/metadata或volatility | 私有真实callback marker识别声明所有者，opaque replacement仍完全拥有自身规则 |

仅改ExprEvaluator/Main/NetworkServer三个CPP及注册；无共享头变化。
保留原UTF8搜索、空needle/haystack、合法NULL及NAME/VARCHAR/CHAR转TEXT的语义。
STRPOS没有BYTEA重载，POSITION有TEXT/BYTEA/BIT三种，不能混同；POSITION与原生API换参规则未在本项修改。

## 完整验证结果

| 输入/版本 | 实际结果 | 证据边界 |
| --- | --- | --- |
| PG18.6严格协议294 | 外层0，294全通过 | 值/字段/原始binary/错误阶段/参数/副作用完整参考 |
| 冻结旧accepted R178协议294 | 外层1，154匹配/140断言失败，294 raw均到ReadyI，UUID清理确认 | 不是140个独立缺陷或154项候选通过 |
| 旧native76 | 外层1、完整76、49失败 | 同一新增夹具，未在首错停止或改旧断言 |
| 新accepted-only clean完整16 | 9native/4wire/3真实Main均实际0 | 新native76/完整294及旧80/190/654、原聚合61均保留 |
| 新Root完整16 | 同一16入口均实际0 | 工作区其他改动保留；结果不与clean拼接 |

native76覆盖20个描述/值、36个非法参数/空输入、3个未选/缺失列、8个opaque覆盖、
2个lazy输入及7个legacy typed-cell控制。协议294覆盖三模式、Parse错误、空输入/未选分支、
严格输出名称/来源/OID/宽度/typmod/格式/NULL/原始datum、typed与UNKNOWN参数、
重复Bind/Execute、真实同名重载、quoted身份、nextval及metadata不执行。

## 源码、构建与版本边界

- 双侧各71对象/13当前程序、127头独立核对；fresh3生产CPP、fresh1原生及fresh3真实Main。
  其余55生产/8原生/1stub为输入/全头/flags/对象已核对的donor，未冒称fresh。
- 三个真实Main采用accepted git测试体，仅投影include路径，本轮fresh编译；
  新链接当前57个库对象，绝不链接test_stubs。未使用Root的未接受session测试体。
- Root正常实际编3CPP并链接，repeat零编译/零链接；clean显式3CPP+55donor及链接，未声称normal/repeat。
- 全部2201已接受源码投影与实际测试clean逐blob对应。Root2320仅安装12个拥有hunk及两个新夹具，
  原有其他修改不提交；安装前2318源码/118cache/server文件的独立恢复快照已核对。
- 当前已跟踪600MD、918native+566已注册Python+3Main=1487；Root603MD、924+572+3=1499，
  原12个未接受入口和三份文档继续分开。不是1487全量、完整72、TLS或整库sanitizer声明。

V1曾完成四次syntax-only0，没有对象或候选DB运行；V2沿用完整同字节夹具，只补纯名称族分析入口。
最初source-only把四setup漏计为290，在任何DB启动前已被库存断言拒绝；第一次PG运行前校正为294，
也校正了既有Wire.command参数形式。没有改已运行断言以取得通过，旧失败与V1输入均保留。

域/自定义隐式转换及更多query owner/类型边界未在此宣称完成。R164两次完整矩阵超时仍未确定原因，
不因本项变为通过或用户暂缓。R177及R173/R156等跳过模块不恢复；R180仍开放。
原273仍22complete/166partial/70unverified/15deferred，总目标未完成。不push，Actions禁用。

## 固定证据

本地证据目录`/tmp/dbms-review-r46.ifu6SWLp/`，不是可移植release依赖。

独立核对：`/tmp/dbms-r179-independent-proof.nf8t4zak/completed-proof.json`，SHA256 `80ba1b21ee81e3cebcd5ba7aebfd0ec145cbb4301a17816691f7655c79787765`，实际0；这是证据一致性通过，不是整库通过。

- `/tmp/dbms-r179-clean-main-matrix.pqigwv5g/completed-matrix-compiler-receipt.json`：`3f5b9feb956565676d44b01edf9f7b37e97f9dabaf01573b0eb662a1c9edc0d3`。
- `/tmp/dbms-r179-root-main-matrix.vuy4yy56/completed-matrix-compiler-receipt.json`：`f2581d84d9b662d0a4147cecffc25c055cf33f0a5a2af8d65af85f5de7bc47be`。
- `/tmp/dbms-r179-focused-runtime.m2eibyls/completed-runtime-receipt.json`：`584f607ccc9f03349abe47b525ccf23cdae3c8d5c24525b5f1e9ec8230adc644`。
- `/tmp/dbms-r179-focused-runtime.0rmzsp8n/completed-runtime-receipt.json`：`5097126d1ae1d82acd700770f98931ccfa728313a5efa08357c9a0bac1ac8755`。
- `/tmp/dbms-r179-old-native76.0y49djba/completed-counterproof-receipt.json`：`7fa48845370e8a5e6e5f497e85e22332a05ea22ebd68ddf417be63ad6b7981f8`。

PG294日志`r179-reference294.log`的SHA256为`9871aee8ebfb4c02b7f6f648cd03d5ddc22e4aff80f1e10740085151529099a4`；
旧294日志`r179-old-accepted294.log`为`e1c5b2fb9ffa592f42822372e8bf50499b714c1b8140a7343c1004a62a9787f3`。
