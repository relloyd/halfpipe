------------------------------------------------------------
-- create Oracle sample data for integration testing.
------------------------------------------------------------

-- SOURCE

drop sequence halfpipe_test_seq;
create sequence halfpipe_test_seq start with 1 increment by 1 cache 1000;

drop table halfpipe_test_a purge;
create table halfpipe_test_a as select 1 as sequence, a.* from all_objects a where 1=0;

alter table halfpipe_test_a add (last_modified_date date);

-- create a view with sample data ranging over the last 20 days...
create or replace view v_test_data as (select * from (
        -- grab some data from ALL_OBJECTS and a dummy sequence...
        (select a.* from all_objects a where owner != 'RICHARD' and rownum <= 50000)
        cross join 
        -- generate a range of dates...
        (select sysdate - 20 + level last_modified_date
            from dual
            connect by level <= 20
        )
    )
);

-- create a view with sample data that contains sysdate as the LAST_MODIFIED_DATE...
CREATE OR REPLACE VIEW v_test_data_now as
  (select "OWNER","OBJECT_NAME","SUBOBJECT_NAME","OBJECT_ID","DATA_OBJECT_ID","OBJECT_TYPE","CREATED","LAST_DDL_TIME",
        "TIMESTAMP","STATUS","TEMPORARY","GENERATED","SECONDARY","NAMESPACE","EDITION_NAME","SHARING","EDITIONABLE",
        "ORACLE_MAINTAINED","APPLICATION","DEFAULT_COLLATION","DUPLICATED","SHARDED","CREATED_APPID","CREATED_VSNID",
        "MODIFIED_APPID","MODIFIED_VSNID","LAST_MODIFIED_DATE" from (
        -- grab some data from ALL_OBJECTS and a dummy sequence...
        (select a.* from all_objects a where owner != 'RICHARD' and rownum <= 50000)
        cross join 
        -- generate a range of dates...
        (select sysdate last_modified_date
            from dual
            connect by level <= 20
        )
    )
);


insert into halfpipe_test_a (select halfpipe_test_seq.nextval, a.* from v_test_data a where ROWNUM < 1000);
alter table halfpipe_test_a add primary key (sequence);

-- TARGET

drop table halfpipe_test_b purge;
create table halfpipe_test_b as select * from halfpipe_test_a where 1=0;
alter table halfpipe_test_b add primary key (sequence);

exit
