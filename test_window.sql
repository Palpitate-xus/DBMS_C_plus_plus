admin
admin
create database testdb
use database testdb
create table users (id int not null primary key, name varchar 50, score int)
insert into users (id, name, score) values (1, 'Alice', 85)
insert into users (id, name, score) values (2, 'Bob', 92)
insert into users (id, name, score) values (3, 'Charlie', 78)
insert into users (id, name, score) values (4, 'Diana', 92)
insert into users (id, name, score) values (5, 'Eve', 65)
select name, row_number() over (order by score) from users
select name, rank() over (order by score desc) from users
select name, lag(score) over (order by id) from users
select name, lead(score) over (order by id) from users
drop database testdb
