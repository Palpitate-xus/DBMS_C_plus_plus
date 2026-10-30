# R229：带引号的已知系统虚拟目录无法定位（用户跳过）

用户最新要求 `skip it` 后，本项停止。已终止本次自有的两个编译进程；
生产候选、两个完整协议夹具及失败记录保留为未接受草稿，不提交、不计作已修复。
普通 continue 不恢复本项，原 100 项中的 20 个失败不能删去或改写为通过。
跳过前，新增 247 项及原 100 项在独立 PostgreSQL 18.6 均通过；旧产品两侧
均实际返回失败。候选尚未完成编译或运行验收，下面的拟修复不代表接受结果。
资料目录：`/tmp/dbms-r229-quoted-catalog.SEA3glBQ`；参考回执：
`/tmp/dbms-private-pg18-full-fixture.y9m28p_l/completed-reference-receipt.json`。

R228 完整 100 项参考在独立 PG 18.6 全部通过。OID 发送修复后，两侧产品仍各有
20 项失败，均使用 `"pg_catalog"."pg_class"` 并返回 42P01；普通 `pg_class` 与
`pg_catalog.pg_class` 的同源读取、参数和二进制发送正常。不能删掉限定名检查来验收整份草稿。

原因是 Main 的旧目录分派按字符串判断 schema/关系名；带引号的真实 SQL 限定名没有
获得已知 pg_class 虚拟源身份，之后被交给物理表路径。协议 Describe 能识别该目录，
所以可能先返回目录元数据，再在执行时报告关系不存在，不能以 Describe 通过替代执行。

拟修复：按 SQLParser/CatalogManager 的真实限定标识符规范化，只有已支持的系统虚拟源
才转给其现有接收器。保留未知关系、同名物理用户关系、权限和完整目录未支持边界；
不靠去引号字符串替换、不扫描目录造行。需补带引号/未引号、别名、空结果、参数、
数据与错误恢复的完整参考及旧/新两侧验证，之后独立 commit。

本项不是用户已跳过的 R226：R226 关注 pg_catalog 中有真实存储的普通用户关系，
其生产定位修复仍暂缓，不借本项恢复。这里先记录已确认的 pg_class 虚拟源错误，
其他目录需要实际验证，不据名称推测为已修复。

证据：`/tmp/dbms-r228-oid-binary-send.GmJIEtJP/completed-v2-runtime-observations.json`，
参考：`/tmp/dbms-private-pg18-full-fixture.vsdri76d/completed-reference-receipt.json`。
完整 100 项源码和失败记录保留。生产实现仅为工作区候选，没有本项验收或提交。
