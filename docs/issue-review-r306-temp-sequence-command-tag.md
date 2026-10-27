# R306：CREATE TEMP SEQUENCE 的 CommandComplete 标签（OPEN）

状态：未修复、非用户暂缓。来自 R305 副作用对照的独立协议差异；不是创建
失败或格式码问题，不将它混算成 R305 数据计算错误。

同一 `CREATE TEMP SEQUENCE r305_effect START 1` 在 PG 与产品均成功，后续
setval/nextval 正控有效；PG tag 为 `CREATE SEQUENCE`，Root 为 `CREATE TEMP`，pure 为 `CREATE TEMP`。
当前没有本项生产修改。方法：追踪实际 CREATE SEQUENCE 命令身份到协议
CommandComplete，避免用第一个词或固定短语丢失 TEMP/TEMPORARY 等合法修饰。
应添加普通/TEMP/TEMPORARY、大小写/注释、Simple/Extended 及错误后的 tag
控制，再独立提交；不借此恢复用户暂缓的 catalog/通用 grammar 模块。

证据与 R305 共用三侧完整 123 采集的 setup 第三条，原帧/SQL/state/tag 保留：
`/tmp/dbms-r303-prepared-planning.S0DOGkOK/completed-format-effect-observations-receipt.json`。
原 273 总清单未完成；不 push，Actions 禁用。
