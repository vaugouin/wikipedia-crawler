-- What is the database doing right now? Long statements, open transactions and lock waits.
SELECT ID, USER, COMMAND, TIME, STATE, LEFT(INFO, 200) AS QUERY
FROM information_schema.PROCESSLIST WHERE COMMAND <> 'Sleep' ORDER BY TIME DESC;

SELECT trx_id, trx_state, trx_started, trx_rows_locked, trx_rows_modified, trx_mysql_thread_id, LEFT(trx_query, 200) AS QUERY
FROM information_schema.INNODB_TRX ORDER BY trx_started;

SELECT * FROM information_schema.INNODB_LOCK_WAITS;
