# R221：TSVECTOR 拼接按词项、位置和权重执行

## 问题与范围

修复目标是 `tsvector || tsvector`，不是文本相加，也不是完整全文检索实现。
修复前，`'a:1'::tsvector || 'b:1'::tsvector` 返回 `a:1b:1` 或类似的原始文本，
结果声明还会落到 TEXT；重复词项、位置偏移和实际 binary 结果同样错误。
短语检索因此可能得到错误结果。原 R215 已记录此问题，本项另行修复。

| 问题 | 修改方法 |
| --- | --- |
| 原始显示字符串直接相连 | 使用拥有自身数据的 `TsVectorValue`，解析词项和带权重的位置；按字节序线性合并词项。 |
| 右侧位置不偏移、重复词项丢失 | 先取左侧最大位置，右侧逐项偏移后追加到对应词项；保留权重和无位置词项。 |
| 位置或个数溢出 | 位置封顶 16383，每个词项至多 256 个位置；原输入的同位置重复取较高权重，拼接遇到末端上限时不再追加。 |
| NULL/UNKNOWN/声明不一致 | NULL 严格传播且结果仍为 TSVECTOR；仅真正 UNKNOWN 常量或参数选用另一侧 TSVECTOR 输入函数。TEXT 保留文本拼接，非法已知组合报 42883，不能靠 NULL 绕过。 |
| 返回类型与执行分离 | binder、SQL helper 和协议参数推导保持真实 TSVECTOR/OID 3614；现有结构化 AST 入口承接该拼接，未实现新的物理 ProjectSet。 |
| 常见生产者无返回声明 | 实际 `to_tsvector` 和 `setweight` 内置回调拥有 TSVECTOR 返回声明；同名原生替换不继承声明，也不为推断类型执行回调。不是完整函数签名/重载修复。 |
| binary 结果仍是文本 | 发送词项个数、NUL 终止的词项、位置个数及带权重的网络序 16 位位置；接收端逐边界校验并保留 SQLSTATE。 |

公开行为参考 [PostgreSQL 18 文本检索运算符](https://www.postgresql.org/docs/18/functions-textsearch.html)。
位置追加/封顶的参考实现为 [tsvector_op.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/tsvector_op.c)，
文本与网络表示参考 [tsvector.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/tsvector.c)。
实际边界断言以私有 PostgreSQL 18.6 完整执行结果及同一安装目录的源码为准。

## 验证记录

本地资料目录：`/tmp/dbms-r221-tsvector-concat.ZFKu5uQQ`。
修复前基线来自父提交 `302fb062b89e8aab3cceb8e4193a8be64e108677` 的接受树及
保留既有未提交改动的工作区树，二者的头文件和对象分别匹配，未交叉链接。

| 同一组测试 | 修复前接受树 | 修复前工作区树 | 独立参考或检查 |
| --- | --- | --- | --- |
| 最终原生声明/求值 478 项 | 468 项失败，退出 1 | 468 项失败，退出 1 | 保留最初 441 项，追加常见生产者及回调所有权测试。 |
| 最终真实协议 356 项 | 341 项失败，退出 1 | 341 项失败，退出 1 | 私有 PostgreSQL 18.6 全部通过，正常停止。 |
| 输入协议 43 项 | 39 项失败，退出 1 | 39 项失败，退出 1 | 私有 PostgreSQL 18.6 全部通过，正常停止。 |
| 独立值编码 327 项 | 新模块测试，不声称旧实现可编译 | 同左 | O2 与 ASan/UBSan 均通过。 |

原生用例核对声明、statement output 的实际表达式所有者、prepared 求值、raw 求值和 SQL helper；
协议用例核对完整消息序列、OID、宽度、typmod、格式、实际 binary 字节、NULL、空串、
多次 Bind、未知参数推导、表数据、空表、WHERE false/LIMIT 0 和短语检索结果。
独立 Python 期望直接由词项/位置建立，没有调用产品解析器生成预期。

首次协议参考的 341 项全部通过，但夹具统计误写成 375，实际因此退出 1。
只修正固定计数后重新完整通过；再追加 15 项常见生产者检查，最终 356 项再次完整通过。
首次输入参考 42 项中，仅“2047 字节文本词项应可接受”的预期错误。
私有 18.6 实际拒绝该值，安装源码为 `toklen >= MAXSTRLEN`，错误上限为 2046。
修正正控制到 2046，并新增 2047 应报 54000 的反控制，最终 43 项完整通过。
两次失败的原始日志和 receipt 均保留，没有修改为成功。
独立 20000 位位置输入与 40000 词项合并是本实现的有界/性能检查，不冒充 PG 同规模兼容证明。

最终两侧均完成 60 个生产 TU 和 129 个自有头文件的独立全量构建，未混用头文件或对象。
Root 初次正常构建实际 60 次编译及一次链接，重复零编译零链接；各自 test stub 重新编译。
首轮 56 次实际 54 次通过、2 个协议驱动退出 1：两侧各 6 个短语检索控制，值正确但
`@@` 声明错为 TSVECTOR/OID 3614，连带 binary 错误。补齐 binder/helper 的 BOOLEAN
声明，并追加原生正/反短语查询的 10 个声明与求值控制；未改动协议 356 项任何断言。
随后两侧各重编 2 个生产 TU，其余 58 个源文件/头文件/选项完全匹配的对象复用，
不能称最终又全 60 fresh；Root 增量正常 2 次编译/1 次链接、重复仍零编译零链接。
以下最终 56 次是完整重新运行，不是把初次失败改成成功，也不是只拼接先前通过记录。
两侧各运行 13 个原生程序、15 个协议驱动，合计 56 次，实际全部退出 0。
完整新 478/327/356/43 项通过，同时保留旧 tsearch/fulltext_search、JSONB 676/480/826/40、
R215 188/709、原 1262、quoted-unqualified 791、数组 binary/concat、布尔/internal-char
拼接、scalar 签名、投影声明和 catalog type 身份等有限回归。
这是本项相关验证，不是整套新 1524 个入口的运行，也不是 TYPE-12 或总清单完成。

接受树源文件/脚本/测试共 2242；注册清单为 935 native、586 Python 和 3 real-main，合计 1524。
工作区保留 10 项额外既有协议注册，不把两树的注册数混为一谈。
版本、文档状态、兼容声明、进度和测试注册检查通过后，才以替代 index 提交本项 15 个路径；
所有既有修改及 staged 18 路径保持原字节，不将用户暂缓项混入该提交。

| 本地完整记录 | SHA256 |
| --- | --- |
| 最终旧基线 | `e3140169b6934e78e49ed6b69850ec82a35923305458e8a6cdd4fef9300ae5dc` |
| 输入旧基线 | `d6e1b5a90af6b4c94c6251e83868b9d3c6eacbabb61a9e769324fb18dffe5343` |
| 独立值/内存检查 | `d7c60b40a7377ee7a49fd308b43a90c05e1caafe0ae65ac17f4041248d2525e1` |
| 完整生产构建 | `163f1907507aeffe38bd739fa1d93e9e4d2a71d47f2bfcf64be7623bf41b7818` |
| 首轮实际失败 | `1a981239509cf91c30612aa10915e6832277553f44a41fde056c8f689313b4ad` |
| 结果声明增量构建 | `0ee0926e48ea36d2a43f3257394ad83cc7a51593c921eccd73e886e94fd17d75` |
| 最终 56 次运行 | `0eff043531bef2b9bc5fe85dbd75ebfe84f12f813e040da0fc292a228863274b` |

完整 356 项 PG 参考：`/tmp/dbms-private-pg18-full-fixture.mldq7frw/completed-reference-receipt.json`，SHA256 `c1acf553e9b38c8292406bfc7c799eb7fade549f416ac757da2d587cde047d70`。
完整 43 项 PG 参考：`/tmp/dbms-private-pg18-full-fixture.xywl6w3b/completed-reference-receipt.json`，SHA256 `636963bb163fbd2d7e4001610aad116ff4f0da4634806437fa7a2a9247a56460`。


## 尚未完成与用户跳过项

TYPE-12/FUNC-01/IDX-06 仍开放。本项未完成配置、字典、词干/停用词、完整 tsquery tree、
headline/rank 语义、GIN/GiST、完整函数签名，以及所有 TSVECTOR 数组和非规范 binary 输入。
TableManage 既有独立规范化和旧全文检索解析器没有在本项整体替换。
尤其是 binary 接收后仍经过现有文本参数通路，最大词项等非规范边界不能由这组有限测试
推成完整 typed datum 通路已完成；须另行复现和处理，不能借此恢复用户暂缓的总载体重构。

本轮用户要求跳过当前项：R220 已提交的 JSONB 修复不撤销，但其后续检查保持暂缓，
包括该项 review Markdown 中已观察到的一个 NUL 字节；此处记录而不声称已修复。
其他所有明确暂缓项保持原状态。原始 273 项状态和冻结的 1492 项结果不被本项专项绿覆盖。
不运行用户暂缓的默认完整协议 main，不 push，不启用 GitHub Actions，不混入既有 staged 18 路径。
