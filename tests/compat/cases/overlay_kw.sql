-- overlay keyword with column
DROP TABLE IF EXISTS ovt
CREATE TABLE ovt (s text)
INSERT INTO ovt VALUES ('Txxxxas')
SELECT overlay(s placing 'hom' from 2 for 4) FROM ovt
