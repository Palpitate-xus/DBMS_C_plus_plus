# SQL 命令列表

## DDL - 数据定义
- create database
- drop database
- use database
- create table
- drop table
- alter table add column
- alter table drop column
- create view
- drop view
- create index
- drop index

## DML - 数据操纵
- insert into ... values
- select ... from ... where ... order by ... limit ... offset
- update ... set ... where
- delete from ... where

## DQL - 高级查询
- select ... join ... on
- select ... left join ... on
- select ... right join ... on
- select ... union ... / union all
- select ... group by ... having
- select distinct ...
- select count(*)/max/min/sum/avg
- select ... into outfile
- load data infile ... into table
- explain select ...
- analyze table

## TCL - 事务控制
- begin
- commit
- rollback
- checkpoint

## DCL - 权限控制
- create user
- grant ... on ... to
- revoke ... on ... from

## 其他
- view table
- view database
- show connections
- show status
- prepare ... from
- execute ... using
- deallocate prepare
