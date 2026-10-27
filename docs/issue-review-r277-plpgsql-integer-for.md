# R277：整数 FOR 循环的边界、BY、求值顺序与 FOUND

范围：FUNC-06 / SQL-11 的有限正确性修复；不是完整 PL/pgSQL 或原 273 项验收。
本项依赖已独立提交的 [R278 共享整数 CAST](issue-review-r278-integer-cast-text-input.md)。

## 问题与修改方法

旧实现将区间整体按第一个 `..` 分割，未跳过字符串、注释和嵌套表达式；
没有解析 BY。上界表达式的某些 legacy 路径还会忽略尾部 BY，使循环静默按 1 运行。
边界使用 `stoll`，未遵守 int4 限制、NUMERIC/浮点转换、真实 NULL 位和错误顺序。
FOR 结束后没有正确更新 FOUND；首轮候选还会在循环变量也叫 found 时，
由变量恢复覆盖新 FOUND。以下均有固定对照，不仅是源码推测。

| 示例 | 实际 PG18.6 预期 | 修复前表现 |
|---|---|---|
| `FOR i IN 1..8 BY 3` | `1,4,7` | 错误地逐一执行 `1..8` |
| `FOR i IN 2147483648..2147483648` | `22003` | 接受超出 int4 的值 |
| `FOR i IN NULL..(1/0)` | `22004`，不求上界 | NULL/错误顺序不符 |
| `FOR i IN 4..1 BY 0` | 即使空区间仍 `22023` | 未检查 BY |
| `FOR found IN 1..1 ... EXIT` 后读 FOUND | true | false |

实现只修改 `src/utils/plpgsql.cpp`：

- 使用既有词法保护信息，按顶层 `..` 和 BY 分割，拒绝缺失表达式与重复 BY。
- 依次、各一次计算并转换下界、上界、步长；使用 shared typed assignment/cast，
  保留转换/表达式 SQLSTATE；NULL 报 `22004`，非正步长报 `22023`，边界必须为 int4。
- 正向加、REVERSE 减；以 int64 计算下一步，跨 int4 端点即正常结束，不发生带符号溢出。
- 保留循环变量原值、类型与 NULL 位，RETURN/错误/EXIT 均恢复；先结束 iterator scope，
  再发布是否执行过循环的 FOUND。循环体执行期间不提前改 FOUND。
- 仍允许边界表达式读取同名外层变量；实际 PG 已证实这个行为，没有错误地先遮蔽边界变量。

语义依据为 [PostgreSQL 18 整数 FOR](https://www.postgresql.org/docs/18/plpgsql-control-structures.html#PLPGSQL-INTEGER-FOR)
及 [官方 exec_stmt_fori 实现](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/pl/plpgsql/src/pl_exec.c)。
这里没有新增 FOR 循环标签、CONTINUE、query/record FOR 或完整 procedural runtime。

## 固定对照与回归

保留原 47 个案例的完整预期和有限退出保护，先追加 9 个作用域、词法和转换案例，
再追加 4 个限定 BY 字段案例，共 60 个案例；不是发现了 60 个独立 bug。

- native 120 控制：完整值、SQLSTATE、NULL、正/反向、步长、int4 极值、转换舍入、
  外层变量、嵌套迭代、循环内赋值与 FOUND。
- native API 19 控制：无转换回调的 embedding host、畸形范围的执行前拒绝、
  主动记录的表达式/转换顺序、早期错误/NULL、错误的 host scalar 元数据。
- wire 724 控制：Simple/Extended；实际 `server_version_num=180006` 的同一矩阵全 0；
  逐例完整保存结果、精确错误码、E/25P02、ROLLBACK TO 后的 T、最终回到 I。
  PostgreSQL 参考端只用 BEGIN/ROLLBACK 内的唯一函数，不提交永久用户对象或改服务配置。

同一新夹具在当前 R278 基线两端的 native 120/46 失败、API 19/18 失败、wire 724/134 失败。
候选两端上述控制全 0，并完整运行原 RAISE driver 的全部既有断言，追加新 helper；
不修改注册清单，不缩减断言或提高原 socket deadline。

另有两端共 52 个有限实际执行记录均 exit 0，包括 2 主 native、26 native 库回归、
6 真实 Main 与 18 协议入口；其中包含原 R278 native 864 / wire 972 与旧浮点 1190 控制。
完整清单与各次实际退出状态见 receipt，不将这些入口替代整个已登记测试库存。

本轮两个 PL/pgSQL 生产 TU 和两个新 native TU 为 fresh O2 编译；其他生产/stub、
30 个既有 native/Main 对象为源/全部头/编译器/MMD 依赖逐 SHA 匹配复用。
两个服务器、32 个 native/Main 程序均新链接当前库；6 个 Main 不是本轮 fresh TU。
没有公共头、ABI/layout 或磁盘格式变化。

额外两端对当前 `plpgsql.cpp` 单独 fresh 编译并执行 scoped UBSan，
`-fsanitize=undefined -fno-sanitize-recover=undefined`，120 与 19 控制均全 0。
其他库和 fixture 对象未插桩；不是全库 UBSan、ASan、TLS 或完整 full72/full1492 验收。

## 失败历史与版本边界

最初 V1 的 PG 568 已通过，但宽区间旧 native 因 BY 被忽略长时间执行，
在确认自有 PID 后主动 SIGTERM，实际 -15；不是通过、不是自然结束或默认超时。
V2 对 16 个 sequence 案例加循环体有限退出保护，所有原预期保持不变，
PG 568/0、旧两端 94/36 失败和 568/104 失败，代码和日志保留。
第一版候选 native 94/2 失败来自共享文本 `0x2` CAST，已作为 R278 独立修复并提交，
没有删去该输入或只在 FOR 中绕过。最后本项在 R278 基线上重新进行全部固定对照。

首轮 52 入口和 API 17 虽通过，但补查发现“无标量 host”的算术控制表达式会丢失
NUMERIC 类型；两端新增 2 个控制均实际失败，PG 实际结果为边界舍入到 3、
零步长报 `22023`。现在该路径一次调用 shared scalar evaluator，同时保留值、NULL 与
源类型，不先经过丢失类型的文本 fallback。补入两个 API 控制，总数为 19；
保持原有 17 项，重新对旧 R278 库执行完整 native 112/API19，得到上述 44/18 失败。
wire/reference 的同一 676 夹具和旧日志逐 SHA 不变；修复后重新编译、链接并执行全部
52 当前入口，再运行 scoped UBSan。旧候选 52 通过不冒作最终候选证明，补查失败也保留。
完整 legacy string-only host 类型模型仍不属于本项的完整运行时完成声明。

再补查发现 BY 步长内合法的 `b.by`（声明为引用变量名 `"by"`）及夹注释的同类写法
被误认成第二个 BY。实际 PG 接受该步长，但上界 `1..b.by` 仍报语法错误；因此只修改
步长的重复 BY 检查，按 token/引用/注释/嵌套深度区分限定名字，不放宽上界分隔规则。
新增两个合法步长回归及 quoted 上界、qualified 下界正控制，重跑完整 60 案例。
初次诊断使用未引用的 `DECLARE by`，PG 在声明处即拒绝，未将该无效参考夹具计作兼容性证明。

第三轮旧版脚本误导入上一版参考 helper，只运行了 676 个参考控制；该日志保留，
不计作当前完整参考。随后从已测试投影导入逐 SHA 匹配的 60 案例 helper，单独完整执行
实际 PG 724，旧产品 120/19/724 日志未覆盖，纠正后的聚合 receipt 明确区分这两次参考输入。
本次所有源码、夹具与最终运行均使用 v3 版本；此前的 52 和 scoped UBSan 通过不冒作最终证据。

独立开放问题：未引用的 PL/pgSQL 声明名 `by` 被项目接受，而 PG 报 `42601`；完整声明
关键字检查及 CREATE/调用阶段验证尚未修复。方法是按 PL 词法类别检查未引用名称，保持引用名称
合法，并验证错误阶段和前序副作用；不能以本次 FOR 修复将它或整个 FUNC-06 关闭。

本次只提交本 CPP、native/helper、既有 driver 的追加和本记录。
其他工作区、原 18 项暂存 diff、R276 所有候选文件/日志保持原样；
R276 按用户要求跳过，不因本轮恢复。R271、R267、安全/TDE 等其他明确暂缓也不恢复。
历史 DROP/cleanup 失败仍普通开放，原 273 状态与冻结 1492 历史结果不改写，
`--require-complete` 仍预期 exit 1。总 goal 保持 active，不 push、不启用 Actions。

## 证据

当前实验：`/tmp/dbms-r277-plpgsql-for-final.AUPUL7Qh`。
旧实验：`/tmp/dbms-r277-plpgsql-for.bl3eMzMr`。
临时目录不是长期归档承诺，摘要和以下哈希保存到 Git 文档。

| 证据 | SHA-256 |
|---|---|
| `completed-reference-v3-correct-receipt.json` | `f3ce517cfe2a3d3955af3308def0b3452659d380320901ba5658f970431729c7` |
| `completed-qualified-by-probe.json` | `555a831549a1e8cad0197edd7765f602d3f5a4e0f2d1d9bc7b93cc9d62ea709d` |
| `completed-qualified-by-v2-probe.json` | `daa04241ff6fe774a007ba27c51a051919d39d5e80c35c79119482ad612e9f46` |
| `completed-no-host-gap-probe.json` | `37d7b14637199492a6804683e616c634d31932cd2da1bae529c26b590ad3094a` |
| `completed-scoped-ubsan-v3-receipt.json` | `87d953ccda6162b90573e54e2433d40e16da5904c85523f70388470377458b45` |
| `completed-preflight-receipt.json` | `365311128b7f4fe05b70b1cc270a9125d7a547f1797a6878a64b856bda5d4f8f` |
| `completed-old-v3-reference-corrected-receipt.json` | `53fb303913b7d8d5e0cfe7a3a154a04f79503eba126c1955d077c33d623a1926` |
| `completed-integration-v3-receipt.json` | `6392b594a6b94d20f704d8e30c2e28ddc226b563fbd6b4c8fe9472a27185e980` |
| `completed-build-v3-receipt.json` | `722bf7d3e76622348206569536347fe154365c731703a2e8217cde9db287c823` |
| `completed-primary-v3-receipt.json` | `e51d87af96c8069958eca2e80096bf444be824cb19ebe939e194cdcb3d8c17a0` |
| `completed-regression-build-v3-receipt.json` | `b09460c8254d1bf559c64a8f6177c965f00ece4e615833d0d798e0d79218d74f` |
| `completed-candidate-v3-receipt.json` | `f37506b9f4ac2b9b02b7cb108bdc2979e6608540cb68353669b318834b8899cc` |
| 首轮52通过但补查仍失败 | `9149d2f07ce840da02ae9102eab77c51ae3626758179df300026fedf7a852619` |
| 第二轮52通过但步长限定名补查仍失败 | `30db6d87374c1efbad4f1992a562445b512dc26937c903d2e745276f77fa19e7` |
| 第二轮scopedSAN不冒最终证据 | `7a8445d09c0ede2bb2233a0bd3906ea35314f2fbfcaeffc314d6173ca594dd4d` |
| 最初V1部分失败及主动终止 | `6421d54e5dddfc09849c474457fcf9b41b80b58f099c775d2e4bc5894e85aa93` |
| V2完整94和568旧失败/PG通过 | `41be28bde8051d7e637d2ab046806c36997e3eecbe2281137610ae2376e716cc` |
| 实际PG与首轮候选FOUND/同名边界对照 | `ca924eff82a292c18cefde46b2bb5ea018b325696a67e363cddad4796d01eefc` |
