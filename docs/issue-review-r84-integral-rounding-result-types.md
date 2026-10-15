# R84：CEIL/CEILING/FLOOR 输出声明不能继承整数或 REAL 输入类型

状态：本范围已修复；新310原生和266协议通过，完整邻居批次仍有未定因的清理/
CREATE超时。这里修复结果声明，不扩张为完整参数
签名、arity、所有用户重载或全部 legacy 接收器覆盖；R62 广义范围仍 partial。

## 问题与修改方法

实际 PostgreSQL 18.6/180006 中，这三个函数各有 numeric→numeric 和
float8→float8 两个结果域。整数和 REAL 输入转换到 float8，输出 OID 701；
显式 NUMERIC/DECIMAL 保持 numeric，OID 1700。旧 helper 直接返回第一个
参数的类型，导致 SMALLINT/INTEGER/BIGINT/REAL 分别被描述成21/23/20/700，
空结果和 typed NULL 同样错误，嵌套 ABS 还会选择错误的外层结果声明。

修改方法是在实际声明推断中保留 numeric 域，其余 builtin 输入结果为 float8，
不改已正确保留两域的实际取整 callback。真实已选中 stored callee 的声明
优先，不能把同名用户函数一并归类为 builtin。

单纯更改按名字推断的规则还会影响 registerFunction 的 opaque replacement：
旧 `abs(custom_ceil(1::integer))` 返回 INTEGER 7，简单候选却返回 float8 7。
因此真实 builtin 用私有 functor 标记；这三个名字的 opaque native replacement
使用独立内部 identity，helper 按实际 AST 节点保留其旧输入类型推断行为，
不为未知 callback 伪造返回声明、强制转换或执行参数。没有公共头文件变更。

## 复现证据

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：

- 当前 Root 旧匹配对象运行 `integral_rounding_result_type_test`：274 控制，
  144 失败，见 `r84-old-rounding-native.log`。
- 扩展回调保护后的最终 310 控制，用 R83 冻结 Root 的57个历史缓存对象
  运行仍是144失败，额外36个旧回调行为全通过；先逐一校验缓存字节与
  `r83-full-cache-preparation.log` 的原producer SHA256相同，再编译驱动和当前
  同头文件 stubs、隔离执行，见 `r84-old-expanded-native.log`。这是完整旧对象
  差分，不是把旧缓存伪称当前 Root 产品构建。
- 冻结 `49cbc029` 的实际二进制 SHA256
  `681ce8f6d015a0469e07c02397a5e82622deceac704909bfbd6d37603c79a9af`
  运行完整新协议：266 控制，138 失败，见 `r84-old-protocol.log`。
  当时 helper 候选源已编辑但未构建，故这次明确是冻结旧二进制差分，
  不是声称当前变更源码的匹配门禁。
- 同一协议在真实 PostgreSQL 18.6 上 266 控制全部通过，见
  `integral-rounding-reference266.TBxzCUFP.log`；只创建并清理独占 schema。
- 原生回调独立隔离探针：旧缓存的三个整数嵌套调用全部通过，简单候选三个
  全失败，见 `r84-native-callback-probe-old.log` 与 `-current.log`。
  旧缓存来自 R83 当前 Root 的57个逐字节复制对象，其原始 producer 摘要保留；
  它只用于历史差分，不替代最终当前 Root 门禁。首次探针误在非隔离目录启动，
  在测试前被 WAL 恢复保护中止；不计为上述 callback 故障，之后才隔离重跑。

## 验收范围

原 274 控制覆盖七个输入域、字面量/NULL/真实列、普通/WHERE false/LIMIT 0、
嵌套及显式 pg_catalog 和用户 schema 返回声明。新增 native replacement
36 控制后最终共310，进一步验证准备不执行 callback、原输入不变以及旧
直接/嵌套行为；覆盖空/真实数据库、INTEGER/NUMERIC/TEXT、两个注册重载和
显式 immutable，不能以 volatility 猜实现身份。原生 metadata 仍不伪造声明。
完整协议 266 控制还核对结果值/NULL、OID、行数 tag、真实 RowDescription、
DataRow 数量、无 Error 和 ReadyForQuery I。

真实标准构建及重复构建退出0，见 `r84-owned-build.log`：实际重编两个变化的
CPP，其他56个对象匹配同一公共头 epoch，全部58个receipt首尾核对。
二进制SHA256为
`e4d49ecef2907d1a94c8fe94339c14c19cb0bea42b4704fa315ac3b1890f5688`。

`r84-final-native.log` 中前13个完整驱动通过，包括新310、R83的906/237、
R62/R79/R80及数值、ABS、常量规划、窗口routine/frame；第14个存储函数排序
metadata驱动在所有前置断言之后的line82 DROP DATABASE清理断言失败，批次
退出134，剩余七个未运行，不把该批写成全部通过。随后
`r84-remainder-native.log` 完整运行余下七个以及原样存储函数排序驱动，共八个
全部退出0、全部58个receipt及输入签名首尾一致。21个不同入口因此均有完整
通过记录，但先前的清理失败不被覆盖，不猜为相同根因、不放宽原断言，也不
自动重开用户暂缓的DROP/cache专项。

`r84-final-protocols.log` 前16个完整协议驱动通过，包含新266、R83的951、
原R62/R68/legacy窗口及普通ORDER/WHERE、stored order、prepared read root、
窗口routine、numeric sqrt、SQL-value keyword、routine array、generate_series、
真实取消和CLI超时。第17个COPY error/lock release驱动在自己的line53
`CREATE TABLE ... (id INT PRIMARY KEY,v INT NOT NULL)` 准备阶段socket超时，
未进入COPY主体，见对应driver日志；批次退出1，不写成17全通过。
没有改SQL/断言/期限或证明与完整PG/JOIN/ROLLBACK TO超时共享根因。

本次文档/机械检查覆盖506份Markdown、845个本地inline目标、521份Python
AST、13份shell语法、501 unique注册/483独立E2E；文档状态、兼容性及六个总账
单测通过。工作区另有下一项sign回归驱动，其失败已单独保存，未纳入本项提交。

历史完整全量仍见[冻结 d974c80d](issue-full-registered-d974c80d.md)，不得用本次
定向矩阵抹掉三个失败；[R83](issue-review-r83-float8-scalar-signatures.md)中的完整
PG CREATE 超时也仍未解决。用户暂缓范围和禁用的 Actions 保持不变。
