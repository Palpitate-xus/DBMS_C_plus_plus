# 代码复查进度（续接第 178 项）

本表记录本轮局部复查，不能据此认定整个数据库已无缺陷。每项修复单独提交；测试在隔离的临时目录中运行。

> 当前用户要求已扩展为总差距清单的全部问题。总任务仍未完成，按 [总清单执行计划](full-gap-execution-plan.md) 继续；下文“收尾验收”只表示上一批局部修复的历史验收。

| 编号 | 问题及影响 | 修复 | 验证 / 状态 |
| --- | --- | --- | --- |
| 178 | 大对象删除因文件系统错误失败后，大小缓存仍被清空 | 删除失败时保留缓存；保留原有重复删除语义 | `large_object_drop_failure_test`、`phase8_10_test` 通过；`e13279e` |
| 179 | 流式导出到对象自身或其链接时，先截断源文件，导致数据丢失 | 按文件身份识别同一对象，将自导出作为无需复制的成功操作 | 原路径、硬链接、符号链接及缺失对象测试通过；`a144fb1` |
| 180 | 带别名的自相关标量子查询覆盖外层限定列及 NULL 标记，误触发多行错误 | 内层限定列只绑定其可见别名 | 自相关匹配、外层 NULL、NULL 投影和既有标量回归通过；`18bc2a2` |
| 181 | EOF 之后的零字节写入不增加文件大小，却增加缓存大小 | 空写成功后保留原有大小 | EOF 前后、空对象、非法参数、重开一致性测试通过；`4075a59` |
| 182 | 投影列数检查把字符串内的逗号、括号当作 SQL 结构，误拒绝单列或放过多列 | 使用 SQL 解析器的投影列表计算项数 | 引号、转义、函数参数、多列拒绝、自相关及 Volcano SELECT 回归通过；`cda9ac8` |
| 183 | 带谓词或别名的标量子查询读取缺失关系时，可能创建孤立的堆文件 | 扫描前检查关系存在性，缺失时报 `42P01` | 缺表无文件副作用、基数、自相关和异常解锁回归通过；`0e485bd` |
| 184 | EXISTS / NOT EXISTS 将缺失关系或扫描失败当成正常布尔结果 | 扫描前检查关系存在性，并传播扫描失败 | 缺表不创建文件、坏堆路径报 `58030`、空表布尔值及既有 EXISTS / 解锁回归通过；`6614a4e` |
| 185 | 表达式子查询仅持页锁，能够绕过内表的 DDL 关系锁 | 读取内表结构前取得 S / IS 锁，超时报 `55P03`，退出时自动释放 | 标量与 EXISTS、自动提交与显式事务、成功与错误解锁、自相关回归通过；`6fd6e0b` |
| 186 | 表达式子查询把缺失的 TOAST 外部值当成空字符串返回或参与比较 | 在需要求值的内层行上显式检查 TOAST 解析结果，失败则报错 | 正常长文本、缺失外部块、异常解锁、无谓词 EXISTS 不读取投影及相邻子查询回归通过；`6772cc6` |
| 187 | 无 WHERE 的普通表标量投影将常量、算术、限定列和 AS 别名当作字面列名，返回空值 | 普通表统一使用带类型及 NULL 信息的表达式求值；保留虚拟目录提供器 | 常量、算术、限定列、空串 / NULL、事务修改与回滚、基数、缺表和 TOAST 回归通过；`cee3df4` |
| 188 | 标量 `SELECT *` 未展开，单列表返回错误值，多列空表也未报错 | 按可见关系名展开星号，在扫描前校验列数，并保留值和 NULL 状态 | 普通 / 限定 / 别名星号、外层引用及遮蔽、空表、多列拒绝、多行拒绝、解锁与既有标量回归通过；`0a15660` |
| 189 | 带别名的自相关 EXISTS 覆盖外层限定列及 NULL 标记，EXISTS / NOT EXISTS 判断错误 | 内层限定列只绑定可见别名，保留外层值及 NULL 状态 | 自相关匹配、NOT EXISTS、内外层 NULL、裸列遮蔽、既有 EXISTS、标量自相关及关系锁回归通过；`cb5ac06` |
| 190 | EXISTS 命中后仍计算后续行的谓词，可触发无关的除零错误 | 命中后跳过后续表达式求值；底层物理扫描仍继续 | EXISTS / NOT EXISTS 首行命中、无命中、命中前除零及解锁测试通过；缺表、TOAST、关联和关系锁回归通过；`c193b27` |
| 191 | 工作区的集合聚合实现绕过 FILTER / 输入排序，并混淆 NULL、空字符串和数组转义 | 串行 / 并行共用 AST 参数求值、类型化排序、NULL / FILTER / DISTINCT 处理及数组编码；错误通过算子返回 | 集合聚合专项（含 300 行并行、空输入）、parallel_exec、Volcano SELECT 回归通过；`9d41c9b` |
| 192 | 分组键将真实 NULL 与空字符串合并，表达式失败被当作空键 | 在启动分组 worker 前按 AST 和 NULL 元数据求值，结构化编码分组键并传播错误 | 串 / 并行 NULL、空字符串、字面量 NULL、COALESCE、除零测试通过；既有并行和集合聚合回归通过；原 GROUPING SETS / ROLLUP / CUBE SQL 的完整入口断言通过；`2c3042d` |
| 193 | 投影子查询用字符串定位 FROM / WHERE，误读换行、引号内关键字和含空格的表名 | 用关系 AST 和完整词法 token 边界识别子句；保留原表达式 token，避免丢失括号 | 换行 / 制表符、引号内 FROM / WHERE / ORDER BY / LIMIT、引号表名及括号优先级通过；既有投影列数、自相关、星号、TOAST 与关系锁回归通过；`06a6338` |
| 194 | 比较器将数字形状的文本转成数字，破坏文本排序、比较和 GREATEST / LEAST | 两端均为文本类型时按文本比较，保留显式数值转换和定长字符空格语义 | text / varchar、前导零、显式整数转换、定长字符、表达式与布尔回归通过；集合聚合串并行文本排序通过；`103c8a7` |
| 195 | 投影子查询忽略 ORDER BY / LIMIT / OFFSET，标量结果或 EXISTS 判断错误 | 普通单表子查询按类型排序后切片，支持排序别名 / 序号、NULL 位置和 FETCH ONLY；丢弃行不求值投影，保留异常解锁和基数检查 | 行选择专项、解析器回归及 SQL 完整入口通过；WITH TIES 与虚拟目录排序显式报 `0A000`；`11c76de` |
| 196 | SQL 入口白名单、裸列检查及结果排序限制将集合聚合送入旧路径，表达式聚合返回空数组或无行 | 分组 / 非分组集合聚合统一接入支持 AST 的执行器；输入排序传递键表达式，结果排序在聚合后处理 | 完整 SQL 入口的分组表达式、普通分组、非分组、参数表达式、输入 / 输出排序、FILTER、空结果及既有 SUM 回归通过；`fc6c1e0` |

## 总清单续做（未全部完成）

| 编号 | 总清单 ID | 问题与修复 | 验证 | 本地提交 |
| --- | --- | --- | --- | --- |
| 197 | SQL-01 / QRY-04 | `FROM t FETCH` 在尝试解析 JOIN 时移走并丢弃表节点；无 FROM 的 FETCH 被当作投影项。补齐两处子句边界 | 原表节点断言修复前失败，修复后普通 / 省略 FETCH 数量、无 FROM 投影及 parser_phase1 回归通过 | `18c4e20` |
| 198 | P0-16 | 差分工具把空串与 NULL 合并，并修改用户返回的 `OID 123` 等字面值，真实错误被报告为无差异；改为逐值保真比较 | 4 个 Python 回归通过，其中 3 个修复前失败；空串 / NULL 的模拟两端差异现在会报告。完整差分框架仍未完成 | `2aa4582` |
| 199 | SQL-01 / QRY-05 | CTE 处理器在整条 SQL 中查找 WITH，将字符串和子查询中的 WITH 删除；现在只处理当前语句开头的 WITH | 完整入口修复前将 `'with data'` 返回为 `'data'`；修复后 4 项字符串 / 嵌套 / 真正 CTE 回归通过 | `89ca653` |
| 200 | QRY-04 / QRY-10 | 标量 / EXISTS 投影子查询拒绝 FETCH WITH TIES；现在按全部排序键和 NULL 比较保留边界并列行，先 OFFSET 再检查标量基数 | C++ 行选择回归及完整 SQL 入口通过；覆盖单 / 多排序键、NULL 并列、OFFSET、零行、EXISTS 与 `21000`，缺 ORDER BY 报 `42601`；全功能子查询仍为 partial | `f0d06b5` |
| 201 | SQL-01 / QRY-10 | 入口 FETCH 重写会修改字符串并截断嵌套 WITH TIES；仅转换顶层 FETCH ONLY，跳过字符串 / 引号标识符 / 内层查询，支持 FIRST/NEXT、默认数量及 OFFSET | 7 个成功用例及顶层 WITH TIES 显式 `0A000` 通过；CTE 和 SQL 入口回归通过。顶层 WITH TIES 尚未实现，不再静默丢失并列语义 | `4091e72` |
| 202 | P0-02 / QRY-04 | 子查询声明的错误码在协议层依赖英文内容猜测，`42601` / `42P10` 等退化成 `XX000`；引入 `DbError` 分离 code/message，迁移子查询结构、列数、关系、锁、扫描及基数错误，排序回调保留异常类型 | 6 组协议错误及每次错误后的连接恢复通过；C++ 行选择回归检查结构化错误字段、排序异常和解锁。其余执行器及完整错误字段仍待迁移 | `417047a` |
| 203 | P0-16 | 差分工具没有请求参考库 SQLSTATE，却把所有“双方报错”判为相同；显式请求 psql sqlstate 输出并严格比较，参考工具启动失败不再作为空结果成功 | Python 差分回归 9 项通过（新增 5 项，修复前 3 项失败）；实际 PostgreSQL 除零读取得到 `22012`。事务会话、无损参考行解码及 command tag 比较仍未完成 | `0f2fe9e` |
| 204 | SQL-01 / QRY-01 | 常量投影用“第一个单引号必须是末尾引号”判断文本，把 SQL 转义引号当作非法列名；改用 parser 的 Literal / 带符号 Literal 节点识别常量 | 5 项入口回归通过：多个转义引号、单个引号值、含 FETCH 的字符串、文本 / 数值负数；SQLSTATE、FETCH、集合聚合及查询入口回归通过 | `c14257a` |
| 205 | SQL-01 | 布尔重写不识别引号，且把下划线当成单词边界；`'true false'` 变成 `'1 0'`，`is_true_flag` 等列名也被修改。现在跳过字符串 / 引号标识符并使用包含 `_` / `$` 的边界 | 4 种字面值以及包含 true/false 的列名、值和协议列名回归通过；转义文本和 SQL 入口回归通过 | `22d9d68` |
| 206 | P0-16 | 差分工具为获取列名再次执行 SQL，使 nextval / DML RETURNING 等产生第二次副作用；改为 psql 描述语句，CSV 解码列名 | Python 差分回归 13 项通过（本项新增 4 项）；实际 PostgreSQL 对逗号 / 换行列名及无结果命令验证通过，描述不执行原查询；仍保留每语句重连的待改边界 | `ee31a8e` |
| 207 | P0-16 | 参考读取器启用 quiet 后仍按命令标签正则删行，用户文本 `CREATE TABLE` / `BEGIN` 等被丢弃；移除该数据过滤 | Python 差分回归 14 项通过；新增 5 种命令样文本断言修复前失败，实际 PostgreSQL `SELECT 'CREATE TABLE'` 保真通过 | `06dafab` |
| 208 | QRY-10 | LIMIT 0 被当作无限制，OFFSET 到达 / 超过末尾返回全部行；JOIN 的 LIMIT/OFFSET 被读进 ON 条件，且在聚合前截断输入。共用有界切片，区分显式零与 ALL，并在 JOIN 聚合输出后切片 | 18 项完整入口通过：普通 / 标量 / 分组 / 窗口 / JOIN / JOIN 聚合、零行、末尾 / 大 OFFSET、ALL、FETCH 0 和大 LIMIT；无 FROM、完整行数表达式及执行短路语义仍需后续统一管线处理 | `0631974` |
| 209 | SQL-01 | 入口直接删除换行 / 制表符 / CR，将关键字与列名粘连，并修改字符串内容；现在仅把引号外的连续空白规范为分隔空格，引号内保持原值 | 6 项完整入口通过：跨行 SELECT / FROM / WHERE、CRLF ORDER BY 与 FETCH，以及 length 验证字符串中的 LF / TAB / CR / 连续空格未被修改。协议直接输出多行文本仍属于 P0-02 待迁移范围 | `a992fce` |
| 210 | P0-16 | 列描述输入只用 rstrip 删末尾分号，分号后有注释时仍会先执行 SQL；按字符串 / 标识符 / E-string / dollar quote / 嵌套注释边界去掉终止符，拒绝多语句输入 | Python 差分回归 17 项通过（新增 3 组）；断言发送给 psql 的内容不含语句终止符，多语句在调用前拒绝；实际 PostgreSQL 注释结尾描述通过 | `7d088cc` |
| 211 | P0-02 | 旧 SELECT 文本结果按空白拆列，带空格的 quoted alias 被伪造为多列，identifier 中的 `""` 也会丢失；统一 quoted identifier 解码和 legacy header framing，协议适配器成对解码双引号 | 7 类 SQL / 协议回归通过：普通、表达式、聚合、分组、窗口、无 FROM 及 embedded quote alias；review、空白边界和 PostgreSQL 协议相邻回归通过。SELECT 全链路结构化结果仍未完成 | `3052136` |
| 212 | P0-02 / QRY-01 | 无 FROM 的多列 SELECT 由协议层反解析显示文本，含空格 / 换行的值被拆成多行多列，空串、文本 `NULL` 与 SQL NULL 也无法区分；该入口现在发布精确 cells、独立 NULL bitmap 和 SELECT command tag，仅最外层语句可发布 | 新协议回归在修复前返回两行错位数据，修复后精确保真 7 列：空格、空串、文本 NULL、SQL NULL、换行、前后空格和 escaped quote；零行仍发送列描述。quoted alias、literal、空白、FETCH、LIMIT、review 及完整协议相邻回归通过；表查询和 legacy scalar subquery 仍待迁移 | `e6b692d` |
| 213 | P0-16 | 差分工具为同一 case 的每条参考 SQL 启动新 psql，事务、临时对象和 SET 状态全部丢失，列描述还会在另一个 session 执行；改为每 case 一次 psql，通过随机 framing、`:SQLSTATE` / `:ROW_COUNT` 和 stderr markers 分离每条结果、错误及同 session `\gdesc` | Python 回归 20 项通过（新增 session、逐语句错误码、同 session header 3 项）；实际 PostgreSQL 的 BEGIN + TEMP TABLE + INSERT + SELECT + ROLLBACK 保持状态，事务错误依次得到 `22012` / `25P02`；只读 `arith_select` 差分通过。参考行无损编码和 command tag 比较仍待完成 | `b36897b` |
| 214 | P0-16 | psql 的 `NULLMARK` / `\x1f` / 换行文本格式仍会把合法用户值改成 NULL 或拆成额外行列，且没有 command tag；参考端改走 PostgreSQL wire protocol，按 DataRow 长度和 `-1` NULL 标记解码，同一连接读取 RowDescription、ErrorResponse 与 CommandComplete，并与本项目逐语句比较 tag | Python 回归 23 项通过（新增控制字符 / NULL 无损、单连接和 tag mismatch）；实际 PG 对 NULL、空串、文本 `NULLMARK`、换行和 `chr(31)` 返回一个精确 5 列行及 `SELECT 1`，只读 `arith_select` 差分通过。当前参考容器为 PG 17.2，PG 18.6、manifest、并发 / crash / catalog 差分和零 allowlist 发布门仍未完成 | `0305306` |
| 215 | P0-16 | header 差分被 `orows` 条件保护，零行 SELECT 即使 RowDescription 错误也会被判为一致；成功结果只要本项目返回列描述就比较参考 headers，不再依赖是否有 DataRow | 修复前的模拟零行错误列名无差异，修复后报告 `headers differ`；Python 回归 24 项通过，实际 `SELECT 1 WHERE false` 的零行 header / `SELECT 0` tag 差分通过 | `6461934` |
| 216 | P0-16 | wire runner 读取 RowDescription 时只保留列名，类型错误无法发现；解码并逐列比较 PostgreSQL type OID | 模拟 `int4(23)` 对 `text(25)` 现在报告 `column type OIDs differ`，Python 回归 25 项通过；实际文本常量类型差分通过，而 `SELECT 1` 明确复现本项目错误返回 OID 25（PG 为 23），已作为下一项 P0-02 / TYPE 修复输入 | `9a32cdb` |
| 217 | P0-02 / QRY-01 / TYPE-01,02,06,07,20,21 | 无 FROM 结构化结果未发布表达式类型，所有列默认 text；ExprHelper 现在保留 evaluator type，结果发布 `columnTypes`，补 regtype OID，并移除会丢掉类型的 typed-literal 预处理，类型关键字大小写均可识别 | 协议回归精确检查 int4 23、numeric 1700、bool 16、date 1082、timestamp 1114、name 19、regtype 2206 与 `SELECT 1` tag；`arith_select`、`bool_null`、`cast_arith`、`typed_fromless` 四组实际 PG 差分通过，literal / whitespace / review / FETCH / LIMIT / 完整协议相邻回归通过 | `655fdeb` |
| 218 | P0-02 / QRY-01 | 无 FROM 的 current/session user、current database/schema、pg_typeof、version 和 generate_series 特殊分支绕过通用 alias，显式 AS 被静默忽略；所有分支统一优先使用已解析 alias | 协议回归检查 7 个 pseudo-expression alias（含多词 quoted alias），以及 generate_series 的 2 行、int4 OID 和 `SELECT 2` tag；quoted alias 相邻回归通过。差分同时暴露 `FROM generate_series(...)` 仍返回 text OID，保留为下一项待修复 | `066be80` |
| 219 | P0-02 / QRY-01,02 / TYPE-02 | 普通表 SELECT 只把 legacy header 文本交给协议，alias 列失去来源类型；FROM generate_series 又经全 varchar 临时 derived table，integer 列最终成为 text。新增 metadata-only 结果通道（只替换 RowDescription、不虚构结构化 rows），按投影来源发布表列类型，并为整数 literal derived table 保留 int4/int8 schema | 协议回归检查普通列 alias 和两种 FROM generate_series 的 rows、int4 OID、header 与 `SELECT 3` tag；完整协议、fromless、literal、review、LIMIT 相邻回归通过。`builtin_funcs_null` 中 4 个普通 generate_series 查询的类型差异归零，只剩 count/sum aggregate OID 两项待修复 | `e85488b` |
| 220 | P0-02 / TYPE-02,16 / FUNC-02 / QRY-07 | legacy 聚合结果没有类型元数据，普通及分组 `count` / `sum`、分组表达式和 `array_agg` 均退化为 text OID；按 PostgreSQL 聚合返回规则推断数值、布尔、min/max、集合和 JSON/XML 类型，分组键保留原列或算术提升类型，并补常用内建数组 OID 映射 | 实际 PostgreSQL 的 `aggregates`、`agg_order*`、`group_by_expr`、`group_by_ordinal`、`grouping_functions`、`grouping_sets`（不含另列待办的 view 类型）及 `string_agg` 差分通过；协议专项检查 int4 输入的 sum/count 为 int8 OID 20，完整协议、fromless、quoted alias、review 和差分 runner 单元回归通过。差分另复现 `array_agg(DISTINCT ...)` 顺序和聚合 view schema 类型问题，保留为后续独立修复 | `8b6b5e4` |
| 221 | FUNC-02 / QRY-07 | 结构化集合聚合对无显式 ORDER BY 的 DISTINCT 输入仅按扫描顺序去重，`array_agg(DISTINCT int)` 与 PostgreSQL 的类型化排序结果不同；在 transition/dedup 前按全部 aggregate arguments 做 NULLS LAST 的 SQL 类型排序 | 串行 / 并行集合聚合专项新增逆序算术输入，均得到 `{0,1,2,3,4}`；实际 PostgreSQL `array_agg` 全 case 差分由失败转为通过，`string_agg`、aggregate ORDER BY、普通 aggregates、review SQL 和 alias 协议相邻回归通过 | `f227ebd` |
| 222 | P0-02 / CAT-16 / QRY-05,12 | 派生表 / CTE 通过捕获显示文本建临时表，所有非整数字段退化成 varchar；聚合 view 又有一层递归重写，内外结果 descriptor 都未传回协议。增加限定执行深度的内部 typed descriptor capture，按捕获类型建临时关系，并在 view rewrite 边界只转发最终查询元数据 | 新增协议 E2E 覆盖聚合 derived table、CTE、view、view predicate 及 int/numeric/text 类型；实际 PostgreSQL `grouping_sets_views` 差分由两处 OID 失败转为全通过，CTE、fromless、alias、完整协议、review SQL 和总账单元回归通过 | `2a32d48` |
| 223 | P0-02 / TYPE-04 / QRY-01 | 表达式求值器为上下文强制转换保留的 `character varying` 中间类型被直接发布；PostgreSQL 顶层未定型字符串字面量及纯字符串 CASE 应解析为 text。协议发布时仅将未显式 varchar cast 的该中间类型归一为 text | FROM-less 协议专项新增普通 / escaped string、CASE 和显式 varchar 对照，精确得到 OID 25/25/25/1043；实际 PostgreSQL `case_expr`、`string_select`、`format_funcs`、`quoted_alias_headers` 四组差分由失败转为通过，derived/alias 相邻回归通过 | `2b7845e` |
| 224 | P0-02 / TYPE-02 / QRY-01 | 普通表的标量表达式结果仍统一发布 text，算术、比较、CASE、cast、日期运算和标量函数虽返回正确文本，却给客户端错误 OID。新增不执行表达式的 AST 类型推断，结合关系列类型处理 PostgreSQL 数值提升、布尔操作、日期/时间、数组、聚合式函数和内部 `case_when` / `cast` 改写，并在标量查询入口发布精确 descriptor | 18 个 C++ 类型断言及完整 `constraint_expr_test` 通过；实际 PostgreSQL 的 `arith_in_args`、`between_projection`、`boolean_projection`、`casts`、`exists_*`、`func_arith`、`gcd_lcm_width`、`integer_div`、`is_distinct_from`、`mixed_select_list`、`negative_ints`、`numeric_column_path/fns/scale`、`projection_exprs`、`round_trunc`、`strpos_overlay` 等失败组转为通过；专项 CASE / cast / numeric cast / sign 及 5 组协议/SQL E2E 通过。全量 122 组差分由修复前 51 组失败降至 24 组 | `1f17d40` |
| 225 | P0-02 / TYPE-02 / QRY-08 | 窗口查询的 Volcano 快路径在发布结果描述前直接返回，legacy 窗口路径也未发布类型，导致排名、取值及窗口聚合统一成为 text OID。两条路径现在共用窗口返回类型解析，保留普通列类型，并按 PostgreSQL 规则发布排名、分布、ntile、lag/lead/value、数值聚合和数组聚合类型；顶层别名识别跳过窗口参数内部的 `AS` | 新协议 E2E 覆盖 6 组、16 个普通/窗口输出类型；`window_e2e_test` 和协议/alias/derived 相邻 E2E 通过；实际 PostgreSQL 的 `sweep_window`、`window_frames`、`window_nulls`、`window_range_null`、`windows` 五组差分全部通过。全量 122 组差分由 24 组失败降至 18 组 | `f732538` |
| 226 | P0-02 / TYPE-02 / QRY-01 | FROM-less 结构化结果直接发布求值器内部类型，数组构造/函数、JSON 提取、数学重载、日期/时区及 regexp 函数出现 text 或错误数值 OID；将静态表达式类型推断接入该边界，只在更具体或旧类型过宽时覆盖，并严格区分完整 postfix cast 与 cast 后续运算。补齐 JSON `->/#>`、数组布尔操作、精确/浮点函数重载、date/interval、AT TIME ZONE 和数组函数规则 | 新协议 E2E 精确检查 int[]、text[]、float8、timestamptz 6 列；新增 20 个边界断言且完整 `constraint_expr_test` 通过。实际 PostgreSQL 的 `array_funcs`、`json_ops`、`numeric_presentation`、`string_edges`、`sweep_fn`、`sweep_misc`、`text_funcs`、`to_timestamp` 八组差分转为通过；发现并修复的 7 个相邻回归组均恢复通过。全量 122 组差分由 18 组失败降至 10 组 | `b7181a1` |
| 227 | P0-02 / TYPE-02 / QRY-04,07 | 标量子查询只返回显示文本：FROM-less 外层没有 descriptor，分组相关子查询固定为 text，聚合与子查询算术也丢掉两侧类型。新增无副作用的子查询投影类型解析；FROM-less 使用 metadata-only 描述，分组直接保留内表投影类型，组合算术用类型化占位符推断并按数值优先级合并 | `derived_type_protocol_e2e_test` 新增 count 标量、相关 numeric 子查询及聚合算术三组类型/行数/tag 断言；`subquery_sqlstate`、alias、FROM-less、review SQL 相邻 E2E 通过。实际 PostgreSQL 的 `subqueries`、`corr_scalar_group`、`agg_subquery_arith` 三组差分全部通过；全量 122 组差分由 10 组失败降至 7 组 | `bbea2ed` |
| 228 | P0-02 / TYPE-02 / QRY-03 | 二表 JOIN 仍依赖显示文本生成 RowDescription，右表 numeric 等类型和纯 JOIN 聚合统一退化成 text。按最终投影顺序构建左右表联合类型环境，`SELECT *` 保留两侧 schema，显式列按来源定位，JOIN 聚合复用表达式返回规则发布 metadata-only descriptor | 新 `join_type_protocol_e2e_test` 覆盖 INNER/LEFT/RIGHT、显式左右投影及 CROSS JOIN count 共 5 组；`multijoin_e2e`、通用协议、派生类型相邻 E2E 通过；实际 PostgreSQL `joins` 全组差分通过。全量 122 组差分由 7 组失败降至 6 组 | `9bb01ce` |
| 229 | P0-02 / TYPE-02,06 / QRY-01 | 日期算术、EXTRACT 和 AGE 的表标量投影在输入为 SQL NULL 时输出真实空字符串，且 `date - DATE literal` 的协议类型错误为 date；在表达式求值阶段基于行 NULL bitmap 保留 NULL，并补齐 typed date literal 的 int4 类型推断 | `constraint_expr_test`、`date_component_projection_test` 和派生类型协议 E2E 通过；实际 PostgreSQL `date_funcs`、`interval_age` 差分通过，全量 122 组差分由 6 组失败降至 4 组 | `bf37679` |
| 230 | P0-02 / DML-01 / PROTO-02 | 结构化 DML 和 legacy DML 成功后都丢失受影响行数，协议固定返回 `UPDATE 0` / `DELETE 0`；存储层新增无行镜像开销的可选计数输出，事务失败时归零，两个执行入口均发布结构化 command tag，协议只对 DML 采用该标签 | 新协议 E2E 覆盖 UPDATE/DELETE 的 0、1、2 行；DML RETURNING、update/delete atomicity 相邻回归通过；实际 PostgreSQL `ddl_dml_basic` 差分通过，全量 122 组差分由 4 组失败降至 3 组 | `5f28d15` |
| 231 | TYPE-06 / PROTO-05 | interval 文本格式把 `-1` 的年月日字段误用单数，timestamp 负差值输出 `-1 day`，与 PostgreSQL 的负数字段复数规则不一致；仅正数 `1` 使用单数 | `interval_arith_test` 增加负一天和负 interval 回归并通过；实际 PostgreSQL `ts_subtract` 差分通过，全量 122 组差分由 3 组失败降至 2 组 | `e8a158c` |
| 232 | P0-02 / FUNC-05 / PROTO-08 | 未定型字符串或 NULL 传给 `sum` / `avg` 时，在重载解析前被降为 text 并误报 `42883`；按 AST 保留 unknown literal，报告 PostgreSQL 的候选函数不唯一 `42725`，协议层同时识别显式 SQLSTATE 标记 | 表达式回归覆盖 `sum('abc')`、`avg(NULL)` 和显式 text cast 对照；`constraint_expr_test` 与实际 PostgreSQL `errors` 差分通过，全量 122 组差分由 2 组失败降至 1 组 | `2bbcbec` |
| 233 | P0-02 / PROTO-08 / DIV-01,05,09,10 | 默认 `postgresql18` 模式把项目专属语法统一误报为 feature-not-supported `0A000`；按 PostgreSQL parser 行为区分无效语法 `42601` 与单词 `SHOW` 参数不存在 `42704`，保留扩展模式功能和迁移提示 | DIV 门禁 E2E 覆盖 USE、DESC/DESCRIBE、VIEW、SHOW、备份/恢复/计划缓存及 replication slot；实际 PostgreSQL `div_commands` 差分通过，全量 122 组差分达到 `failed=0` | `250796e` |
| 234 | P0-02 / SQL-12 / QRY-01 / PROTO-04 | 普通表 SELECT 仍把行拼成空格/换行显示文本再由协议层反解析，混淆 SQL NULL、文本 `NULL`、空串、嵌入换行、引号和边界空格；基础表投影新增精确 cells/NULL bitmap 通道，并修正 StorageEngine 路径 DESC 默认 NULLS FIRST | 新协议 E2E 覆盖 7 种保真值、列重排及 4 种 NULL 排序；协议、派生类型、review SQL、3 个 C++ 排序/NULL 相邻回归通过，实际 PostgreSQL 122 组差分保持 `failed=0`。复杂 operator tuple 仍待结构化 | `03346a0` |
| 235 | P0-02 / SQL-12 / QRY-01 / PROTO-04 | 普通表标量投影仍以显示行返回，文本 `NULL` 被误发为 SQL NULL，嵌入换行被拆行；拼接求值还把空串和文本 `NULL` 当成 NULL。为 `queryExpr` 增加精确 cells/NULL bitmap 重载，基础标量入口直接发布结构化结果，并以行 NULL 位而非值文本判断拼接传播 | 协议 E2E 覆盖 `upper`、`coalesce`、拼接以及空串、SQL NULL、文本 NULL、换行、引号和边界空格；表达式、NULL、排序、标量子查询及 6 个协议/SQL 相邻回归通过，实际 PostgreSQL 122 组差分保持 `failed=0`。输出别名排序、谓词、DISTINCT/LIMIT、SRF/UDF 及复杂 operator tuple 留待各自结构化迁移 | `8183624` |
| 236 | P0-02 / SQL-12 / QRY-01,10 / PROTO-04 | 标量结果按输出 alias / ordinal 排序时仍拆解显示文本，既破坏精确 cells，也把文本型数字按数值排序并忽略显式 NULL 位置。改为对结构化 cell/NULL 位建立稳定排列，按结果类型比较，再用同一排列重排 CLI 与协议结果 | 协议 E2E 覆盖 alias、ordinal、ASC 默认 NULLS LAST、DESC NULLS LAST、文本数字 `10`/`2` 及第 235 项全部保真值；协议、派生类型、review SQL 相邻回归通过，实际 PostgreSQL 122 组差分保持 `failed=0`。任意 ORDER BY expression 与 external sort 仍属 QRY-10 | `5170f81` |
| 237 | P0-02 / SQL-12 / QRY-01 / PROTO-04 | 带 WHERE 的标量表投影无条件退回显示文本，即使谓词只产生一个合取执行组，也再次损坏 NULL、换行和边界空格；单执行组现在直接从 `queryExpr` 收集结构化 cells/NULL bitmap | 协议 E2E 覆盖双条件筛选后的空串、SQL NULL、文本 NULL、换行、引号和边界空格；review SQL 及实际 PostgreSQL 122 组差分通过。多 OR 组仍需按行身份合并后全局排序 | `2daf1cf` |
| 238 | P0-02 / SQL-12 / QRY-01 / PROTO-04 | 标量 OR 谓词按显示行做集合合并，错误删除投影值相同的不同物理行，并继续损坏文本 `NULL` 与换行。`queryExpr` 结构化重载返回内部 row id，多分支按行身份去重并保留精确 cells/NULL bitmap | 协议 E2E 用重叠 OR 覆盖重复投影值、文本 NULL、SQL NULL和嵌入换行，确认 5 个物理行各返回一次；协议、review SQL、异常解锁及实际 PostgreSQL 122 组差分通过。多 OR 后按未投影表列的全局排序仍待迁移 | `e4ee2dd` |
| 239 | P0-02 / SQL-12 / QRY-01,10 / PROTO-04 | 多 OR 标量查询按未投影表列排序时，各分支分别排序后串接且退回显示文本，结果既非全局有序又不保真。把物理排序列作为隐藏结构化 cell，行身份合并后按类型、方向和 NULL 位置全局稳定排序，再删除隐藏列 | 协议 E2E 覆盖重叠 OR、`ORDER BY id DESC`、重复值、文本 NULL、SQL NULL 和换行；LIMIT/OFFSET、协议、review SQL 及实际 PostgreSQL 122 组差分通过。物理列与输出表达式混合排序仍待统一 comparator | `2ad43ad` |
| 240 | P0-02 / SQL-12 / QRY-01,09,10 / PROTO-04 | 标量 DISTINCT 用显示文本去重，混淆 SQL NULL、文本 `NULL` 与换行；LIMIT/OFFSET 只切显示行。改用完整 cells+NULL bitmap 作为 DISTINCT 键，并同步切片结构化 rows/nulls；同时在移除 DISTINCT 后建立 alias map，修复 `ORDER BY alias` | 协议 E2E 覆盖 DISTINCT 重复值、SQL NULL/文本 NULL/换行、alias 排序，以及 LIMIT 3 OFFSET 2 的精确保真；LIMIT/FETCH、协议及实际 PostgreSQL 122 组差分通过。DISTINCT ON 约束、collation 和 spill 仍属于 QRY-09 | `fd303b2` |

本批新增的待修复复现（仍计入总清单）：

- P0-02：quoted alias、无 FROM 普通投影、基础表列和基础表标量投影已由第 211–212、234–240 项迁移；混合物理列/输出表达式排序、DISTINCT ON、任意表达式排序、聚合 / JOIN / 窗口 rows、CTE / set operation、legacy scalar subquery、SRF / UDF 及二进制值仍存在显示文本边界，继续计入总清单。
- P0-02 / TYPE：第 217 项已修复无 FROM 基础表达式的已知 OID 回退；表查询、复杂表达式、数组 / composite / domain、typmod 和 binary format 类型元数据仍需完整差分。

### 本批验证记录（第 197–220 项）

- 27 个 C++ 测试通过：15 个子查询回归、parser_phase1，以及表达式、布尔、数组、集合聚合、分组键、并行、Volcano、窗口、布尔聚合、分位数和异常解锁的 11 个相邻测试。使用当前生产库对象重新链接，在隔离目录运行。
- Python 单元测试 31 项通过：总账校验 6 项、差分工具 25 项。
- 当前 `build/dbms_review_main` 的 11 组 SQL / 协议 E2E 通过：review_sql、CTE、FETCH、子查询 SQLSTATE、转义文本、布尔边界、LIMIT/OFFSET、多行 SQL、窗口、EXPLAIN ANALYZE、PostgreSQL 协议。
- 实际参考 PostgreSQL 的错误码、CSV 列描述、带注释的终止符及命令样文本读取验证通过；未对参考库做持久化数据修改。
- 总账完整覆盖 273 项；目前 complete = 0、partial = 35、unverified = 223、用户延期 = 15。`--require-complete` 正确返回非零。122 组差分归零不代表 273 项功能族完成。
- 仓库只有 `ci.yml.disabled`，没有启用的 workflow；修复均为本地 commit，未 push。

## 上批局部收尾验收（2026-09-08）

以下仅是此前第 178–196 项的历史验收记录，不适用于 273 项总清单，也不包含本批新增待办。此前要求跳过的安全专项未重新展开。

| 检查点 | 验收结果 |
| --- | --- |
| C++ 回归 | 清单对应 20 个测试，加表达式、集合聚合、解析器、并行、Volcano、窗口、布尔聚合及分位数的 11 个相邻测试，共 31 个全部通过 |
| 完整 SQL 入口 | `review_sql_e2e_test.py` 的 17 个查询断言通过；窗口 E2E 13 项及 EXPLAIN ANALYZE E2E 6 项通过 |
| 主程序 | 按生产源码清单和当前构建选项编译、链接 `build/dbms_review_main` 并用于上述 E2E；未覆盖已有 `dbms_main` 或停止其他运行实例 |
| 工作区 | 起始的集合聚合改动和分组 SQL 已纳入提交，临时 `DSH_DBG156` 调试输出已移除；修复及验收记录全部本地提交 |
| 自动化与推送 | 仓库工作流仅保留 `ci.yml.disabled`，没有启用的 workflow；未执行 git push |

投影子查询上批支持普通单表的解析及行选择。JOIN、分组、DISTINCT、集合运算、CTE、锁定子句、命名窗口等未实现形态，以及虚拟目录排序，显式返回 `0A000`，不再当作普通无修饰查询静默执行。WITH TIES 的后续补充见第 200 项。这是明确的支持边界，不是 PostgreSQL 子查询全语法实现。

本次执行范围、阶段及验收见 [收尾计划](review-closeout-plan.md)。
