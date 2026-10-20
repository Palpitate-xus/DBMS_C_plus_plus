# R199：文本大小写转换保留字符、collation 与 CHAR 语义

状态：有限修复通过下列完整定向验证；不表示字符或 collation 完整需求已完成。

## 问题和修改方法

| 问题 | 已核实影响 | 候选方法 |
| --- | --- | --- |
| 常量／表达式按字节转换 | `upper('é' COLLATE "C.utf8")` 返回原值，INITCAP 把多字节字母当分隔符 | 解码完整 UTF-8 码点，用选定字符 locale 的大小写和字母数字分类处理，再完整编码 |
| 表列绕过同一实现 | LOWER/UPPER 使用另一套 wchar 路径，与常量、INITCAP 和显式 collation 不一致 | 仅将这三个函数的已有表投影分支接到现有 `evalStringWithNulls`，传入实际列类型、NULL 和 collation hints |
| 四字节字符输出损坏 | 旧表路径把 emoji／Deseret 码点只编码为三字节，实际输出无效 UTF-8 | 共用现有严格 UTF-8 解码／编码，不再调用该旧转换分支 |
| CHAR 填充泄漏 | LOWER/UPPER 把定长 CHAR 尾部空格放进 TEXT 结果，INITCAP 却正确去掉 | 保留既有文本实参转换，统一由声明及 `textArgumentValue` 决定 CHAR 到 TEXT 的规则 |

旧 Unicode 帮手还被其他模块使用，本项没有全局修改它们。
没有改变公共 API、对象布局或存储格式，也没有恢复整库结构化结果重构专项。
范围类型的 LOWER/UPPER 重载保留在原回调分支；原函数所有权与替换回调规则不变。

显式 `C`、`POSIX`、`ucs_basic` 采用 ASCII 字符规则；`C.utf8`、英文 UTF-8 使用单码点字符映射。
不使用会把 ß 展开为 SS 或产生上下文 final-sigma 的完整 Unicode 大小写转换。
未指定字符 locale 的调用沿用原表路径现有 LC_CTYPE；主程序原本就从进程环境设置该值。
因此候选也把常量的默认大小写行为与既有表路径统一，不是数据库 LC_CTYPE catalog/GUC 的完整实现。
转换本身不调用全局 setlocale，不修改其他线程的字符环境；独立显式 locale 使用已有只读 locale 对象。

## 已完成的对照

同一完整协议夹具在新建实际 PostgreSQL 18.6（180006）上通过 360 项。
覆盖 Simple、Extended text、Extended binary，显式 C.utf8/C/POSIX，
TEXT/VARCHAR/CHAR、空文本、SQL NULL、字符串 NULL、Unicode 四字节、
希腊字母、组合大小写和数字／标点边界，包含真实表列而不只是常量。

同一严格夹具在两套修复前程序上均执行完 360 项，各有 117 项不符并退出 1。
新原生夹具在两套修复前库上均执行完 259 项，各有 54 项失败并退出 1，
包含实际 ExprValue collation、CHAR 尾空格、严格 NULL、非法 UTF-8、opaque 回调及全局 locale 不变检查。
这些是重复路径的失败控制，不是 117 或 54 个独立 bug。

最初旧协议运行在第 279 项后因客户端遇到无效 UTF-8 中止。
随后只让夹具把原始帧十六进制保留为失败、继续后续检查；不采用替换解码，
不修改 SQL、值、OID 或错误预期，也不缩短原矩阵。该初始失败日志仍保留。
早期 PG270 和 PG360 参考是较早夹具版本的记录，不代替最终严格 360 项参考。

## 修复后构建及完整定向验证

两套程序各完成 8 个原生入口、17 个协议入口，共 50 个实际进程，均退出 0 并核对完整完成标记。

- 原生：新字符 locale 259、原 string_functions、range_functions、range_bound_projection、
  scalar_signature、native_scalar_signature_ownership、utf8_split_linear 76、OVERLAY 新增 12 项及全部原检查。
- 协议：新字符 locale 360、table_structured、table_case_ast、scalar_function_order_structured、
  scalar_signature 132、qualified_projection_routine_identity 637、builtin_range_catalog 121、
  plpgsql_collation_label、join_collation_role、computed_query_host_metadata 331、
  prepared_global_aggregate 67、pg_class_query、OVERLAY 94、utf8_split_linear 329、
  string_length_signature 654、quoted_unqualified_scalar_projection 791、ordinary_aggregate_identity 906。

每侧 fresh 编译 TableManage、ExprEvaluator 两个生产单元并重新链接；另各 fresh 编译 8 个原生测试单元。
其余 56 个生产对象、stub、127 个公共头、编译器和参数逐字节核对复用，不冒称全部 58 单元或 Main fresh。
工作区实际正常构建只编译这两个 CPP 并链接，重复零编译／零链接；最后四个额外协议进程结束后，
再次实际正常构建也零编译／零链接。实际默认 dbms_main 参与上述工作区完整检查。

首轮验证驱动实际退出 1：46 个数据库检查进程均退出 0，但驱动把两个原夹具的 footer 写成了错误的
68／906 和名称，造成两套共四个标记误判。原首轮 receipt 不改写。
核对原夹具自身的 132／637 断言后，仅修正驱动期望并按字节核验已完成命令、对象和日志，
得到完整 46 项审计；随后另实际运行两侧各 791／906 全夹具，形成最终 50 项证明。
没有把 637 夹具说成执行了 906 项，也没有修改原测试、缩减 SQL 或放宽错误检查。

## 本机证据及范围

- 本项目录：`/tmp/dbms-r199-case-locale.sZRWLJWg/`。
- 完整旧红：`completed-baseline-receipt.json`，SHA256 `afa470fa3b46cb3c022f1598f1a09a7378a57a3089f7aa7454b764a264e01746`。
- 候选准备：`completed-prepared-receipt.json`，SHA256 `da5093d1026971d0e589c7d0919da06f758f4039dc3178735fec7b1ea9ff255c`。
- 严格完整 PG360：`/tmp/dbms-private-pg18-full-fixture.qzl7wwxa/completed-reference-receipt.json`，
  SHA256 `3c590ae20c187b15235f44c09fd9df3beb69d7f8c3c45b5bfbd1697f94720cd0`；实例正常停止，默认 fsync 保留。
- 首轮驱动失败：`completed-candidate-receipt.json`，SHA256 `414afe11621fe86b7782af597c0c42cd9bd744246699e4effdfb0cbfce669751`。
- 正确完成标记审计：`completed-candidate-v2-receipt.json`，SHA256 `2d6fd9bbf4ce8ab38ae4c036302211ce54a202be52bbbd544131e2592420fa0b`。
- 最终 50 项与实际正常重复构建：`completed-final50-receipt.json`，SHA256 `97bd6dd31e7a9165c1752feca124ae5068aa6e8635e266225c6ec23e7a1bacf8`。
- 独立提交和后续核对分别保存在 `completed-commit-receipt.json`、`completed-postcommit-receipt.json`。

这些临时路径是本机证据，不是可移植发布包；工作区仍含其他未接受候选，其通过不代表接受它们。
上述定向通过不关闭字符或 collation 完整需求。
实际 PG360 使用显式 collation，不能证明未指定 locale、ICU provider、用户定义 locale、
数据库 LC_CTYPE catalog/GUC、所有查询节点或全部 Unicode 版本的完整一致性。
现有表字符串结果载体未整体重构；其他入口的潜在问题仍需独立复现。

用户跳过的 R197 COPY、R137 外键、R146 主键、R185 排序中断、默认协议 search_path 和其他暂缓项不恢复。
原 273 项状态仍为 22 complete、166 partial、70 unverified、15 deferred；总目标未完成。
冻结 1492 的 1481 PASS / 11 FAIL 不改写。仅本地 commit，不 push，GitHub Actions 保持禁用。
