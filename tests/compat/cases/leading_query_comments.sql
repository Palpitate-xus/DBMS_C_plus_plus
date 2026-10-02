/* preamble */ SELECT 1 AS n, NULL AS absent, '-- /* literal */' AS data;
/* outer /* nested */ end */ SELECT 'text' AS value;
/* preamble */ BEGIN READ ONLY;
/* preamble */ BEGIN READ WRITE;
/* preamble */ SELECT 1;
/* preamble */ ROLLBACK;
