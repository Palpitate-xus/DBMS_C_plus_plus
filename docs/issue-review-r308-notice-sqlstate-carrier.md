# R308：IF NOT EXISTS 通知 SQLSTATE 被协议默认值覆盖（OPEN）

状态：实际复现、未修复、非用户暂缓。与 R307 重名 ERROR 的代码缺失是独立
载体问题；通知保持成功不等于通知元数据正确。

R307 完整 260 主源 PG18.6 全通过。十八个 IF NOT EXISTS 请求（普通/自定义/
临时命名空间、已有 sequence/table/view、Simple/Extended）两侧均 NOTICE
后 CREATE SEQUENCE 成功，原对象和值不变，但产品通知 C=00000，PG C=42P07。
完整 severity/SQLSTATE 断言保留，不弱化成“只要收到 N 就算通过”。

已读接受副本 NetworkServer.cpp 的旧式 stdout NOTICE/WARNING 消费处，确实
将 SQLSTATE 硬编码为 00000/01000；当前 DDL 通知文本也不携带该状态元数据。
方法应在真实诊断产生处保留 severity/code/message，按语句和线程生命周期
传递至协议/CLI 消费者；不能从任意用户通知文字或对象名猜码、也不能把普通
RAISE NOTICE 的字面 `(SQLSTATE ...)` 误当元数据。需覆盖用户原样通知、显式
代码、嵌套诊断、无通知语句不串消息和并发隔离后独立提交。

当前不实现新的通知 carrier，不采用 Root 中未接受的 OutputCapture/触发器
诊断通道草稿；R307 只改实际 ERROR 分支。源、全部原始帧和失败见
/tmp/dbms-r307-sequence-duplicate.h7qqyppH/completed-original-duplicate-reference-old-receipt.json。
原 273 未完成；全部已暂缓模块不恢复，不 push，Actions 禁用。
