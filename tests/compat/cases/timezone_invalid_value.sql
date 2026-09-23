-- Invalid TimeZone values must fail without mutating the current setting.
SET TIME ZONE 'Asia/Shanghai';
SHOW TimeZone;
SET TIME ZONE 'Mars/Phobos';
SHOW TimeZone;
SET TIME ZONE '+bogus';
SHOW TimeZone;
SET TIME ZONE '';
SHOW TimeZone;
SET TIME ZONE 'UTC';
