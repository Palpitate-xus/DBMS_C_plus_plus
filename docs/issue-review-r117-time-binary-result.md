# R117：TIME及数组二进制结果拒绝微秒和24时

状态：实现及匹配产物的36入口有限验收全部通过；按本项独立本地提交，不push。

## 问题与影响

TIME已有发送分支却调用仅服务旧heap的parseTimeToSeconds：只支持整秒和
0–23小时。合法微秒、24:00:00及相关TIME[]非NULL元素请求binary结果时
报0A000，文本结果可能正常。不能将此误认为所有TIME、所有数组或参数OID丢失。

最初32条实际180006参考/旧程序观察有16个差异：12个binary发送失败、4个
超微秒文本舍入差异，后者不由输出codec自动修复。永久144协议控制实际参考
全部通过，固定746c947c旧程序114失败、实际退出1；完整121低层控制旧58
失败、实际1。两个旧入口均完整运行，未把编译失败或计数错误当产品红灯。

## 修改方法

仅修PostgresProtocol.cpp的真实OID1083发送分支：校验基本时钟语法、非负
小时0–24、分钟0–59、秒0–60，再复用既有checked整数时钟解析，输出网络
字节序int64微秒。约束总值在0至86400000000，不能把INTERVAL的无界小时、
月/天、符号或单位文本冒充TIME。小数按整数微秒的实际参考舍入，涵盖半偶和
进位；不经double折算整个时钟。短时钟、外部空白和合法闰秒归一的binary
字节也按参考验证。TIME[]继续由真实1183/typelem1083封套调用同一codec。

不修改公共头、现有4字节整秒heap、类型输入/文本规范化、域/用户OID或binary
Bind接收；不会为了发送结果而改写原存储格式。其他缺失标量codec仍拒绝，
不把文本字节伪装成binary，失败时不输出部分DataRow。

## 永久验证

`tests/time_binary_result_protocol_e2e_test.py`：144条控制。
30组由实际PG验证的独立期望微秒值，分别经过真实text Bind标量、带NULL数组、
TIME常量、ARRAY构造器；另有NULL/空/全NULL、负下界、2D/6D、无行结果、
纯Parse/Describe、未改变的canonical文本、混合格式与实际表INSERT RETURNING/
SELECT。实际表标量使用现有heap可存的整秒，数组保留微秒/24时；不以这些
通过掩盖标量TIME的物理精度限制。严格检查OID/长度/typmod/格式/行值/tag/Ready。

`tests/time_binary_result_test.cpp`：121条完整低层控制。同30组独立期望值、
真正8字节和数组头、NULL/下界/维度、20种非法时钟/越界与数组错误形状、
混合行全成或全不发送，以及原INTERVAL/数值/BOOL/文本和缺失codec守卫。
复用现有发送帧夹具模式，不调用被测codec计算期望；通过初始化前隔离器、
O2和fresh stubs运行Engine链接入口。

证据目录 `/tmp/dbms-review-r46.ifu6SWLp/`：
`r117-time-{reference,current}32.log`为最初32条观察；
`r117-reference144-initial.log`、`r117-old144-initial.log`、
`r117-old-native121-initial.log`保存完整永久入口的参考及旧红灯。
固定旧程序`r117-pre-fix-dbms-main` SHA256：
`746c947cb8edba15e29d9a226dac9e05d49b4079ed363be6cd7240f095f9a5ad`。
它含R98/R109保留候选，不冒充clean HEAD；此前观察用的2c8c1b64版本另存原日志。

## 最终验收及范围

normal/repeat各重编1/0个CPP、实际均0，生产和库O2。16个完整native与20个
完整协议的36个不同入口实际全部退出0。TIME121低层/144协议、INTERVAL117/
171、数组182低层、物理长度102低层及默认协议入口均在其中；不把内部控制数
当成不同入口数。`r117-final-{driver,native,core,neighbor}.log`保留真实终态，
`r117-final-{core,neighbor}-results/`保留每个完整协议日志。

生产SHA256 `0579474b74a5f4386563066b0b21e4f9d255d1a1a94c6ab6143fc8e93dd95429`，
缓存签名 `3d74a8c002106b0a87e4cd68bc8aaa023dcef00f027694976be40a625a73285c`，
源/测试/脚本封印 `d017c7ac9f1e1ae737a160cbcc6be2186a113cccb8f50b0bd89925d5e07c0c98`。
正常/测试双缓存、全部58个编译receipt、57份test-library字节及冻结输入复核0。
产物含原有保留候选，不能冒充clean HEAD全量证明。

额外重新完整采集原32条观察：与相同实际PG参考比较，binary差异为0，仍有4个
text格式舍入差异。`r117-time-after-binary32.log`及
`r117-r119-observations-comparison.log`保留该边界；比较脚本修正过一次AST元组
参数计数假设，未改产品、参考数据或重跑测试冒充旧问题修复。额外时间戳64条
观察发现12个独立binary差异，保留`r119-timestamp-{reference,current}64.log`，
后由[R119](issue-review-r119-timestamp-binary-result.md)单独修复并验收；原始发现
及本项36入口结论不改写。TIME输入/文本规范化仍普通开放。

全部工作区静态检查542MD/545Python AST/13Bash及本地链接/清单一致性实际0。
本项提交接受组合为539MD、891native+3Main+521Python=1415；工作区包含保留
候选时为894+3+524=1421。仅暂存本项15个路径和精确hunk；原56933字节候选
补丁的源字节与三个明确保留草稿另行核对，不纳入本项提交。
Main和公共头未改，不复用R118的三真实Main结果宣称本项重跑过它们，也不将
编译0当测试0。尚无本组合的新完整注册回归、真实TLS或整库sanitizer证明。

| 普通未完范围 | 后续方法 |
| --- | --- |
| TIME默认/显式精度的输入/文本规范化 | 在真实datum输入与typmod阶段处理舍入/进位/格式，不靠binary sender修改文本结果 |
| 标量TIME完整微秒存储 | 明确兼容旧4字节行、引入真实微秒存储codec和迁移/重载验证，不静默丢精度 |
| binary Bind接收、TIMETZ等缺失codec | 实现所属类型真实接收/发送并做实际参考字节测试，不猜用户OID |
| 目录SQL/视图origin及其他规划能力 | 保留既有清单方法，各自复现/修复/验证，不计入本项完成 |

原273仍22complete/166partial/70unverified/15deferred，不提升勾选。
R98/R99/R109候选、R106草稿及用户明确暂缓范围不恢复；不push，Actions禁用。
