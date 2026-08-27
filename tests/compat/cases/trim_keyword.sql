-- trim keyword forms
SELECT trim(both 'x' from 'xxhixx')
SELECT trim(leading ' ' from '  hi')
SELECT trim(trailing 'x' from 'hixx')
SELECT trim(both from '  hi  ')
