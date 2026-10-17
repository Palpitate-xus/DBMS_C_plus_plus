# R135：TIMESTAMPTZ的RETURNING结果未按会话时区呈现

状态：本项有限修复完成。核心134及既有微秒266全过，45个不同完整相邻入口均0，
独立终态核对0。标量微秒持久化的另一个普通缺口见本文末尾，不计为本项已修复。

## 期望、实际与影响

| 内容 | 已确认事实 |
| --- | --- |
| 期望 | TIMESTAMPTZ的INSERT/UPDATE/DELETE RETURNING按语句执行时的TimeZone输出正确本地时间与偏移；会话时区不改变binary UTC时刻，TIMESTAMP/TEXT/NULL及infinity对照不受影响 |
| 旧实际 | UTC下存储列RETURNING为2000-01-01 00:00:00，缺少+00；非UTC下还未转换墙钟时间。普通SELECT同列已能输出+00，不能将整个类型或输入接收器误判为失效 |
| 复现 | PG18.6原133控制全过、冻结Root旧133/72失败；增加同一Simple Query批次中SET切换时区的保护后，PG134全过，冻结旧Root和干净ecf程序各134完整/73失败、实际1，保留初版日志 |
| 原因 | SELECT的Main路径已有时区呈现，结构化DmlResult保存内部UTC行像；协议直接发送该中间文本，没有统一处理RETURNING的OID1184结果。不能改内部行像来伪装修复网络显示 |

## 修改方法

只修改私有NetworkServer.cpp的协议结果呈现。按实际OID1184及SQL NULL位处理
结果中间文本；无偏移的内部TIMESTAMPTZ行像按UTC解释，显式偏移仍表示原UTC时刻。
在整数整秒与保留的微秒小数间分拆，按每个值对应的日期查IANA偏移，再把小数
放回显示偏移之前。规范化不更改DmlResult/heap行像、RowDescription或NULL位。

转换发生在每条语句执行完成时，不等到整个Simple Query批次发送时才使用最终
TimeZone，因此同批SET不能追溯改变先前结果。不在Parse/Describe执行callback。
对超出既有civil formatter可表示范围的呈现先验证整秒往返，不把仍可编码的
binary datum改成无效日期。未修改binary encoder；134中的36个DML binary控制
及既有266个接收器控制均保持原精确8字节。

公共头、Main、parser、物理codec及DDL赋值精度没有修改。本项按基础scalar
OID1184及所测现代冬夏日期验收，不冒全部BC/大年份、历史秒级offset、domain/
数组输出、DateStyle或跨Execute分批切时区均已完成。

## 永久回归与证据

新增注册入口[timestamptz_returning_timezone_protocol_e2e_test](../tests/timestamptz_returning_timezone_protocol_e2e_test.py)：
六会话时区（UTC/Shanghai/NewYork/Kathmandu/Kiritimati/Berlin）、冬夏两时点、
Simple/Extended-text/Extended-binary及三种DML，检查精确值、OID/宽度/typmod/
format、tag和Ready。每行保留TIMESTAMP、TEXT、typed NULL、正负infinity及
不落heap的计算微秒对照，另有18个既有SELECT路径控制和一个同批SET保护。

| 验证 | 实际结果 |
| --- | --- |
| PG18.6/180006最终矩阵 | 134全过，reference仅用TEMP表，连接关闭清理；初133全过日志保留 |
| 冻结Root旧ca37程序 | 最终134完整、73失败、实际1；原133/72失败保留，不把新增控制写回旧数量 |
| 干净ecf旧72ee程序 | 同一最终134完整、73失败、实际1，不由Root的R98/R99/R109候选推断旧错误 |
| Root正常O2/repeat | 仅NetworkServer.cpp重编、实际0；repeat0CPP、实际0 |
| Root核心与接收器邻居 | 新134全过，原R133微秒266全过，包括精确负epoch/基础civil边界，不用新formatter破坏binary |
| 干净开发组合 | R135 Net与已接受R134 Expr两个CPP fresh O2，56份已核对原生产donor含真实Main、无stubs，链接隔离真实server；134和266两个完整入口均0。不是正常整库构建，也未改干净full工作树 |
| 最终相邻门禁 | 18native+24Python（23协议/1CLI）+3核对R132未变TU后新链接当前库并执行的真实Main，共45个有序不同完整入口，全部实际0；未冒Main TU fresh编译 |
| 独立终态核对 | 实际45个名字/顺序/次数/日志、旧新134矩阵、正常1/repeat0、源/程序/58 receipt/双cache/57库字节及真实Main对象/新链接产物均0；原1436完整日志与两真实失败仍保持 |

日志在`/tmp/dbms-review-r46.ifu6SWLp/`的`r135-reference133.log`、
`r135-reference134.log`、`r135-old-root133.log`、`r135-old-root134.log`、
`r135-clean-ecf134.log`、`r135-normal-build.log`、`r135-repeat-build.log`、
`r135-focused134.log`、`r135-microsecond-neighbor266.log`、`r135-clean-candidate.log`、
`r135-final-gate.log`、`r135-final-native-results/`、`r135-final-protocols.log`、三个
frontend日志及`r135-independent-gate.log`。
干净开发源码/真实程序及两个入口日志在`/tmp/dbms-r135-clean-source.EQ0tp4Y7/`。

Root程序SHA256为
`15501fa37b57ff2df009dd148027c9c89ca44d03cf999c6d89e54e3d968d2eca`，
cache为`a17133049dbe9a56fd999e04d9a3ec620540967b9edd71697d0e25829316646f`，
src/tests/scripts封印为
`a5b8b7be5169aca04eafbb779d789cb7f7c386c6024268a520e71188361f3a0d`，
Root头/flags/compiler签名仍739bf42b，不与干净8c绝对路径签名混用。

## 另一个未修复的普通缺口：标量微秒不能持久化

这不是用户暂缓项，也不因本项结果显示修复而关闭。隔离诊断在PG完成40个观察，
在干净ecf和R134 Root各完成37个观察；进程退出0只表示采集完整，不是兼容通过。
三者均能独立CAST合法微秒、存取数组微秒与整秒标量；但两个本项目程序在新建
普通表上对TIMESTAMP/TIMESTAMPTZ/TIME的微秒INSERT和UPDATE均报22023，共六个
已观测拒绝。PG均成功。不能把随后缺行/索引查询差异再计为六个新的独立根因。

证据为`temporal-heap-reference-discovery.log`、`temporal-heap-clean-ecf-discovery.log`
及`temporal-heap-root-r134-discovery.log`，采集脚本保留。只观察了暖路径，不冒
冷重启、索引微秒精度或typmod舍入验证；这些必须在存储支持后重新实际测试。

修改方法：先明确旧整秒codec与新微秒codec的版本/列元数据，保持既有秒数据可读，
不能静默把同一8字节旧值改解释为微秒或只绕过验证后继续丢精度。统一INSERT/
UPDATE/扫描/RETURNING/比较/索引/约束及冷热读写，加入负值/NULL/infinity、同秒
不同微秒、显式精度舍入及兼容旧schema的永久回归，再按独立事项提交。原
TIME四字节及timestamp八字节整秒路径和单独辅助微秒解析函数不能充当已经升级的证据。

本项接受558MD、900native+535Python+3Main=1438；工作区另含原三个MD/六个
运行入口候选，为561MD、903native+538Python+3Main=1444；559AST/13Bash另检。
本项独立本地commit仅纳入自有hunk/测试/相关文档，原56933字节候选差异与三个
暂缓draft逐字节保护。原273条目/checkbox及22complete/166partial/70unverified/15deferred保持，不冒新
1438全量、真实TLS或整库sanitizer；不关闭原1436两真实失败，原候选字节保护，
不push，GitHub Actions保持禁用。
