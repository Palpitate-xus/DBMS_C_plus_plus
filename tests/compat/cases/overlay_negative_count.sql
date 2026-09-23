-- PostgreSQL OVERLAY permits a negative FOR count; SUBSTRING does not.
SELECT overlay('abcdef' placing 'X' from 2 for -1);
SELECT overlay('abcdef' placing 'X' from 7 for -2);
SELECT overlay('aé中z' placing '界' from 2 for -1);
