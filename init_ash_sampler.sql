
 CREATE DATABASE aas_schema;
 
 USE aas_schema;
 
 CREATE ALGORITHM=TEMPTABLE  SQL SECURITY INVOKER VIEW `aas_schema`.`aas_source`
 (`thd_id`,`conn_id`,`user`,`db`,`command`,`state`,`time`,`current_statement`,`statement_latency`,`progress`,`lock_latency`,`rows_examined`,`rows_sent`,`rows_affected`,`tmp_tables`,`tmp_disk_tables`,`full_scan`,`last_statement`,`last_statement_latency`,`current_memory`,`last_wait`,`last_wait_latency`,`source`,`trx_latency`,`trx_state`,`trx_autocommit`,`pid`,`program_name`,`blocking_thd_id`) 
 AS select 
 `pps`.`THREAD_ID` AS `thd_id`,
 `pps`.`PROCESSLIST_ID` AS `conn_id`,
 if((`pps`.`NAME` in ('thread/sql/one_connection','thread/thread_pool/tp_one_connection')),
 concat(`pps`.`PROCESSLIST_USER`,'@',`pps`.`PROCESSLIST_HOST`),replace(`pps`.`NAME`,'thread/','')) AS `user`,
 `pps`.`PROCESSLIST_DB` AS `db`,
 `pps`.`PROCESSLIST_COMMAND` AS `command`,
 `pps`.`PROCESSLIST_STATE` AS `state`,
 `pps`.`PROCESSLIST_TIME` AS `time`,
 `pps`.`PROCESSLIST_INFO` AS `current_statement`,
 if(isnull(`esc`.`END_EVENT_ID`),`esc`.`TIMER_WAIT`,NULL) AS `statement_latency`,
 if(isnull(`esc`.`END_EVENT_ID`),round((100 * (`estc`.`WORK_COMPLETED` / `estc`.`WORK_ESTIMATED`)),2),NULL) AS `progress`,
 `esc`.`LOCK_TIME` AS `lock_latency`,
 `esc`.`ROWS_EXAMINED` AS `rows_examined`,
 `esc`.`ROWS_SENT` AS `rows_sent`,
 `esc`.`ROWS_AFFECTED` AS `rows_affected`,
 `esc`.`CREATED_TMP_TABLES` AS `tmp_tables`,
 `esc`.`CREATED_TMP_DISK_TABLES` AS `tmp_disk_tables`,
 if(((`esc`.`NO_GOOD_INDEX_USED` > 0) or (`esc`.`NO_INDEX_USED` > 0)),'YES','NO') AS `full_scan`,
 if((`esc`.`END_EVENT_ID` is not null),`esc`.`SQL_TEXT`,NULL) AS `last_statement`,
 if((`esc`.`END_EVENT_ID` is not null),`esc`.`TIMER_WAIT`,NULL) AS `last_statement_latency`,
 `mem`.`current_allocated` AS `current_memory`,
 `ewc`.`EVENT_NAME` AS `last_wait`,
 if((isnull(`ewc`.`END_EVENT_ID`) and (`ewc`.`EVENT_NAME` is not null)),'Still Waiting', `ewc`.`TIMER_WAIT`) AS `last_wait_latency`,
 `ewc`.`SOURCE` AS `source`,
 `etc`.`TIMER_WAIT` AS `trx_latency`,
 `etc`.`STATE` AS `trx_state`,
 `etc`.`AUTOCOMMIT` AS `trx_autocommit`,
 `conattr_pid`.`ATTR_VALUE` AS `pid`,
 `conattr_progname`.`ATTR_VALUE` AS `program_name` ,
 `dlw`.`BLOCKING_THREAD_ID` AS `blocking_thd_id`
 from ((((((((`performance_schema`.`threads` `pps` 
 left join `performance_schema`.`events_waits_current` `ewc` on((`pps`.`THREAD_ID` = `ewc`.`THREAD_ID`)))
 left join `performance_schema`.`events_stages_current` `estc` on((`pps`.`THREAD_ID` = `estc`.`THREAD_ID`)))
 left join `performance_schema`.`events_statements_current` `esc` on((`pps`.`THREAD_ID` = `esc`.`THREAD_ID`)))
 left join `performance_schema`.`events_transactions_current` `etc` on((`pps`.`THREAD_ID` = `etc`.`THREAD_ID`)))
 left join `sys`.`x$memory_by_thread_by_current_bytes` `mem` on((`pps`.`THREAD_ID` = `mem`.`thread_id`)))
 left join `performance_schema`.`session_connect_attrs` `conattr_pid` on(((`conattr_pid`.`PROCESSLIST_ID` = `pps`.`PROCESSLIST_ID`) and (`conattr_pid`.`ATTR_NAME` = '_pid')))) 
 left join `performance_schema`.`session_connect_attrs` `conattr_progname` on(((`conattr_progname`.`PROCESSLIST_ID` = `pps`.`PROCESSLIST_ID`) and (`conattr_progname`.`ATTR_NAME` = 'program_name'))))
 left join `performance_schema`.`data_lock_waits` `dlw`  on((`pps`.`THREAD_ID` = `dlw`.`REQUESTING_THREAD_ID`))) WHERE `pps`.`PROCESSLIST_ID` IS NOT NULL AND `pps`.`PROCESSLIST_COMMAND` != 'Daemon' AND `pps`.`PROCESSLIST_COMMAND` != 'Sleep';
 


 CREATE TABLE aas1 ( 
 snap_time               datetime                                ,
 thd_id                  bigint(20) unsigned                     ,
 conn_id                 bigint(20) unsigned                     ,
 user                    varchar(128)                            ,
 db                      varchar(64)                             ,
 command                 varchar(16)                             ,
 state                   varchar(64)                             ,
 time                    bigint(20)                              ,
 statement_digest        varchar(64)                             ,
 current_statement       varchar(128)                            ,
 statement_latency       bigint(20) unsigned                     ,
 progress                decimal(26,2)                           ,
 lock_latency            bigint(20) unsigned                     ,
 rows_examined           bigint(20) unsigned                     ,
 rows_sent               bigint(20) unsigned                     ,
 rows_affected           bigint(20) unsigned                     ,
 tmp_tables              bigint(20) unsigned                     ,
 tmp_disk_tables         bigint(20) unsigned                     ,
 full_scan               varchar(3)                              ,
 last_statement_digest   varchar(64)                             ,
 last_statement          varchar(128)                            ,
 last_statement_latency  bigint(20) unsigned                     ,
 current_memory          decimal(41,0)                           ,
 last_wait               varchar(128)                            ,
 last_wait_latency       varchar(20)                             ,
 source                  varchar(64)                             ,
 trx_latency             bigint(20) unsigned                     ,
 trx_state               enum('ACTIVE','COMMITTED','ROLLED BACK'),
 trx_autocommit          enum('YES','NO')                        ,
 pid                     varchar(10)                             ,
 program_name            varchar(32)                             ,
 blocking_thd_id         bigint(20) unsigned  )   ENGINE=MEMORY ;
 
 
 
 
 CREATE TABLE aas2 ( 
 snap_time               datetime                                ,
 thd_id                  bigint(20) unsigned                     ,
 conn_id                 bigint(20) unsigned                     ,
 user                    varchar(128)                            ,
 db                      varchar(64)                             ,
 command                 varchar(16)                             ,
 state                   varchar(64)                             ,
 time                    bigint(20)                              ,
 statement_digest        varchar(64)                             ,
 current_statement       varchar(128)                            ,
 statement_latency       bigint(20) unsigned                     ,
 progress                decimal(26,2)                           ,
 lock_latency            bigint(20) unsigned                     ,
 rows_examined           bigint(20) unsigned                     ,
 rows_sent               bigint(20) unsigned                     ,
 rows_affected           bigint(20) unsigned                     ,
 tmp_tables              bigint(20) unsigned                     ,
 tmp_disk_tables         bigint(20) unsigned                     ,
 full_scan               varchar(3)                              ,
 last_statement_digest   varchar(64)                             ,
 last_statement          varchar(128)                            ,
 last_statement_latency  bigint(20) unsigned                     ,
 current_memory          decimal(41,0)                           ,
 last_wait               varchar(128)                            ,
 last_wait_latency       varchar(20)                             ,
 source                  varchar(64)                             ,
 trx_latency             bigint(20) unsigned                     ,
 trx_state               enum('ACTIVE','COMMITTED','ROLLED BACK'),
 trx_autocommit          enum('YES','NO')                        ,
 pid                     varchar(10)                             ,
 program_name            varchar(32)                             ,
 blocking_thd_id         bigint(20) unsigned  )   ENGINE=MEMORY ;
 
 
 
 
 CREATE TABLE aas3 ( 
 snap_time               datetime                                ,
 thd_id                  bigint(20) unsigned                     ,
 conn_id                 bigint(20) unsigned                     ,
 user                    varchar(128)                            ,
 db                      varchar(64)                             ,
 command                 varchar(16)                             ,
 state                   varchar(64)                             ,
 time                    bigint(20)                              ,
 statement_digest        varchar(64)                             ,
 current_statement       varchar(128)                            ,
 statement_latency       bigint(20) unsigned                     ,
 progress                decimal(26,2)                           ,
 lock_latency            bigint(20) unsigned                     ,
 rows_examined           bigint(20) unsigned                     ,
 rows_sent               bigint(20) unsigned                     ,
 rows_affected           bigint(20) unsigned                     ,
 tmp_tables              bigint(20) unsigned                     ,
 tmp_disk_tables         bigint(20) unsigned                     ,
 full_scan               varchar(3)                              ,
 last_statement_digest   varchar(64)                             ,
 last_statement          varchar(128)                            ,
 last_statement_latency  bigint(20) unsigned                     ,
 current_memory          decimal(41,0)                           ,
 last_wait               varchar(128)                            ,
 last_wait_latency       varchar(20)                             ,
 source                  varchar(64)                             ,
 trx_latency             bigint(20) unsigned                     ,
 trx_state               enum('ACTIVE','COMMITTED','ROLLED BACK'),
 trx_autocommit          enum('YES','NO')                        ,
 pid                     varchar(10)                             ,
 program_name            varchar(32)                             ,
 blocking_thd_id         bigint(20) unsigned  )   ENGINE=MEMORY ;
 
 
 
 
 CREATE TABLE aas4 ( 
 snap_time               datetime                                ,
 thd_id                  bigint(20) unsigned                     ,
 conn_id                 bigint(20) unsigned                     ,
 user                    varchar(128)                            ,
 db                      varchar(64)                             ,
 command                 varchar(16)                             ,
 state                   varchar(64)                             ,
 time                    bigint(20)                              ,
 statement_digest        varchar(64)                             ,
 current_statement       varchar(128)                            ,
 statement_latency       bigint(20) unsigned                     ,
 progress                decimal(26,2)                           ,
 lock_latency            bigint(20) unsigned                     ,
 rows_examined           bigint(20) unsigned                     ,
 rows_sent               bigint(20) unsigned                     ,
 rows_affected           bigint(20) unsigned                     ,
 tmp_tables              bigint(20) unsigned                     ,
 tmp_disk_tables         bigint(20) unsigned                     ,
 full_scan               varchar(3)                              ,
 last_statement_digest   varchar(64)                             ,
 last_statement          varchar(128)                            ,
 last_statement_latency  bigint(20) unsigned                     ,
 current_memory          decimal(41,0)                           ,
 last_wait               varchar(128)                            ,
 last_wait_latency       varchar(20)                             ,
 source                  varchar(64)                             ,
 trx_latency             bigint(20) unsigned                     ,
 trx_state               enum('ACTIVE','COMMITTED','ROLLED BACK'),
 trx_autocommit          enum('YES','NO')                        ,
 pid                     varchar(10)                             ,
 program_name            varchar(32)                             ,
 blocking_thd_id         bigint(20) unsigned  )   ENGINE=MEMORY ;
 
 
 CREATE ALGORITHM=TEMPTABLE  SQL SECURITY INVOKER VIEW `aas_schema`.`active_session_history` as 
 select * from `aas_schema`.`aas1` 
 union all
 select * from `aas_schema`.`aas2` 
 union all 
 select * from `aas_schema`.`aas3` 
 union all
 select * from `aas_schema`.`aas4` order by snap_time desc;
 
 

 
