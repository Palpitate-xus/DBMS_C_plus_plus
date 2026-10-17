# R110：数组二进制结果输出缺失

状态：本范围已实现并验收，按本项独立本地 commit。独立输入缺陷 R112 先行提交；
不恢复用户已要求跳过的 R109 候选或其他暂缓专项。

## 问题、影响与证据

非 NULL INTEGER[]、NUMERIC[] 等数组请求二进制结果时，旧程序发送
RowDescription 后报 0A000，无 DataRow。SQL NULL 数组结果正常，文本格式
及数组参数调用返回标量的 array_position 正常，不能诊断为普遍参数类型丢失。
依赖数组二进制结果的客户端无法正常读取结果。

最初实际 PostgreSQL 18.6（180006）与固定旧程序各 10 条完整采集有 2 条
语义差异。扩展为 45 个类型/形状与 NULL 输入 × 两种结果格式，共 90 条
观察，两个观察器实际退出 0，发现 49 条差异；观察器 0 仅表示采集完成。
其中还包括输入/文本格式及缺失标量 codec，不全部归于二进制数组容器。

## 修改方法

1. 从不可变内置类型目录的真实数组 OID/typelem 关系选择数组编码器；不靠
   列名、类型拼写或任意用户 OID 猜测。查找结果缓存，不为每个标量扫描目录。
2. 解析已有数组值的真实维度和下界，按行主序输出 PostgreSQL 的数组头、
   NULL 标记、元素 OID、每维长度/下界及每元素长度/字节；空数组是 ndim=0，
   与字段长度 -1 的 SQL NULL 不同。
3. 非 NULL 元素调用已有真实标量发送编码器，包括 NUMERIC scale、UTF8、
   BYTEA、浮点、网络、money locale 和几何；字面值 NULL、空字符串和 SQL NULL
   各自区分。缺少元素 codec 时不把文本字节冒充二进制。
4. 只新增 PostgresProtocol.cpp 内部实现，无公共头或二进制输入协议修改。
   具名 BPCHAR/BIT 输入截断在编码前发生，另列[R112](issue-review-r112-unconstrained-named-array-input.md)、单独提交。

## 永久验证

`tests/array_binary_result_protocol_e2e_test.py`：110 条完整控制。30 种已有
元素编码类型各测非空、空数组和 SQL NULL，另测多维/负下界/整数上下界附近、
NUMERIC 精度、混合列格式、空结果 metadata、真实表 SELECT/RETURNING/UPDATE
及标量回归。期望字节独立构造，实际 180006 全部通过；固定旧程序 75 失败，
实际退出 1。协议入口注册一次，所有原断言保留。

`tests/array_binary_result_test.cpp`：完整 182 条低层发送控制，含 49 个真实
内置数组身份的空/全 NULL/SQL NULL，以及值字节、既有标量、错误形状/类型和
发送失败不输出部分 DataRow。Engine 链接运行通过隔离器先切目录再进入全局
初始化；不直接在项目目录运行。最终完整入口实际退出0。

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：`r110-array-parameter-*.log`、
`r110-binary-matrix-*.log`、`r110-reference110-*.log`、`r110-old110-initial.log`。
旧程序 `r110-pre-fix-dbms-main` 的 SHA256：
`c1f5ce345ef26132e466aac2a2928f95183a1f230f94d44a1338bc45dd97704c`。
它含保留的 R109 未接受候选，不冒充已提交版本。

正常/repeat 构建两版各重编 1/0 CPP、实际退出均 0；第二版只缓存目录查找。
第二版新增 C++ 原始字符串分隔符夹具编译失败，修正后仍保留日志；第一轮
低层 BOX 夹具未使用实际行值的规范坐标顺序，退出 134。协议 110 的 BPCHAR
输入失败另归 R112。后一次夹具误预测50个普通数组身份，在实际49个全部值
检查后仍退出134：目录的_record为P类，不属A类普通数组。本项保留所有实际
A类身份遍历，修正计数并增加已有标量/原生raw BOX与SQL规范行值的三项边界
校准；最终182全部通过，原协议110断言不变。上述中间结果不删除、不记作通过。

## 最终有限验收与提交边界

最终冻结输入下，15个完整native与22个完整协议共37个不重复入口实际退出
均0，包括本项182/110、R112的57以及数组库、modifier/descriptor/NULL/
constructor、原354/208/607/654、真实表读写和默认协议等完整入口。
日志 `r110-r112-final3-{driver,native,protocol}.log`、逐协议日志目录保留。
生产编译一直为O2；native使用fresh stubs/隔离器；最终正常/repeat实际重编
0/0个生产CPP、程序字节不变，不借用旧全量或声称新编58份。

生产 binary SHA256：
`595869a944bc409cb04e37597a0aa74c9734a07c11a8c813b0aa81473001a6e5`；cache：
`f119652e76df96b502610bb149f3efbd85704c05821ef66a271465cc31f11086`；最终
src/tests/scripts seal：
`372b318071ba82b2ca588176faec5f2f355c131a1b732d4af923e285144408ba`。
cache双stamp、全部58个生产receipt、57份test库字节、冻结输入和未改公共头
的终态/最终独立核对均0。未接受的R98/R99/R109候选仍存在工作区；这不是
clean HEAD全量、真实TLS或整库sanitizer证明。

补充90条采集再次完整退出0，参考对照由49差异降至10；剩余BOX文本输入2、
JSONB/TSVECTOR文本规范形式2、缺失标量binary codec 6均如下表保留。
该采集并非90项全部行为通过，不能藏掉剩余失败。证据为
`r110-binary-matrix-current-final.log` 和 `r110-r112-final-independent-fence.log`。
首次临时参考连接设置/认证helper不适用的失败日志也保留，未改认证代码；
用现有本地参考入口核对真实180006后完成90条采集。

R112先独立提交，本项只收PostgresProtocol.cpp、两个新测试、一个协议注册
入口和文档，共13路径。投影接受索引533份Markdown/888native/515Python，
注册组合888+3+515=1406；含保留候选的工作区891+3+518=1412。
两者均没有新的完整注册回归终态。原273项和勾选不变，静态全工作区检查与
提交前/后范围核对另有日志，不把候选源码/草稿混入本项版本。

## 后续独立能力及状态

下表保留R110验收时的发现与方法，不改写原90条观察的失败。后续
[R113](issue-review-r113-interval-binary-result.md)已单独实现INTERVAL标量/数组的
真实二进制结果；其余缺失标量codec、binary输入和文本问题仍开放。

| 范围 | 已观察问题 | 后续修改方法 |
| --- | --- | --- |
| 具名 BPCHAR[]/BIT[] 输入 | 未带 typmod 却截为长度 1 | R112 保留具名声明的无长度约束语义 |
| BOX[] 文本输入 | 多元素分号分隔被当作非法数组 | 用真实元素分隔符解析，不能改坏引号中的分隔字符 |
| JSONB[] / TSVECTOR[] 文本输出 | 文本规范形式与参考不同 | 在真实元素输出阶段规范化，保留 NULL 和引用规则 |
| INTERVAL、TIMETZ、JSON、JSONB、TSVECTOR、range 非 NULL 二进制 | 对应标量 codec 仍缺失 | 各自实现真实元素发送格式并加参考字节测试 |
| 数组二进制 Bind 输入 | 尚未由本项实现 | 校验数组头/维度/元素身份并使用真实接收 codec |
| 具名复合/域/用户类型数组 | 不由内置 OID 表扩展支持 | 由实际目录元数据与所属类型 codec 决定 |

这些普通待做项不是自动转为用户暂缓。R111 ARRAY 构造器常量规划也独立开放；
不把本项有限输出验收写成 TYPE-16、PROTO-04 或原 273 项全部完成。
原总账仍 22 complete / 166 partial / 70 unverified / 15 deferred。
R98/R99、R109 候选和 R106 草稿保留，不 push，GitHub Actions 禁用。
