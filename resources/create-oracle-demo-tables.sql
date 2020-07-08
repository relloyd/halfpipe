-----------------------------------------------------------------------------------------------------------------------
-- Create simple tables with sample data from ALL_OBJECTS
-- ready to demo the following Halfpipe commands:
--
--   hp cp snap      # snapshot pipeline
--   hp cp delta     # incremental pipeline
--   hp sync batch   # full comparison and synchronisation pipeline
--   hp sync events  # real-time pipeline using Continuous Query Notification
--
-- Prerequisites for using Continuous Query Notifications
-- setup by 'hp sync events' command:

--   GRANT CHANGE NOTIFICATION TO <user>;
-----------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------
-- Cleanup
-----------------------------------------------------------------------------------------------------------------------

declare 
    procedure pr_drop(l_ddl in varchar2) is
    begin
        execute immediate l_ddl;
    exception 
        when others then null;
    end;
begin
    pr_drop('drop view v_all_objects');
    pr_drop('drop table hp_demo_1_snapshot purge');
    pr_drop('drop table hp_demo_2_delta purge');
    pr_drop('drop table hp_demo_3_sync purge');
    pr_drop('drop sequence seq_hp_demo');
end;    
/

-----------------------------------------------------------------------------------------------------------------------
-- DEMO TABLE FOR 'hp cp delta' COMMAND:
-----------------------------------------------------------------------------------------------------------------------

-- create a view of all objects (1m rows) with added dumb sequence number and a last modified date...
-- the value of SEQUENCE will be fixed by the trigger below.
create or replace view v_all_objects as (
    select * from (
            -- grab some data from ALL_OBJECTS...
            (select 1 as sequence, a.* from all_objects a where rownum <= 10000)
            cross join 
            -- generate a range of dates...
            (select sysdate - 100 + level last_modified_date
                from dual
                connect by level <= 100)
    )
);

create sequence seq_hp_demo start with 1 increment by 1 cache 50;

create table hp_demo_2_delta (	
    sequence number,
    owner varchar2(128 byte), 
	object_name varchar2(128 byte), 
	subobject_name varchar2(128 byte), 
	object_id number, 
	data_object_id number, 
	object_type varchar2(23 byte), 
	created date not null enable, 
	last_ddl_time date, 
	timestamp varchar2(19 byte), 
	status varchar2(7 byte), 
	temporary varchar2(1 byte), 
	generated varchar2(1 byte), 
	secondary varchar2(1 byte), 
	namespace number, 
	edition_name varchar2(128 byte), 
	sharing varchar2(18 byte), 
	editionable varchar2(1 byte), 
	oracle_maintained varchar2(1 byte), 
	application varchar2(1 byte), 
	default_collation varchar2(100 byte), 
	duplicated varchar2(1 byte), 
	sharded varchar2(1 byte), 
	created_appid number, 
	created_vsnid number, 
	modified_appid number, 
	modified_vsnid number, 
	last_modified_date date
);

-- ensure sequence is fixed in the dmeo table...

create or replace trigger trg_hp_demo_2_delta_seq
    before insert on hp_demo_2_delta for each row
    declare
    begin
        :new.sequence := seq_hp_demo.nextval;   
    end;
/

-- insert rows 1m...

insert into hp_demo_2_delta select * from v_all_objects;

-- create primary key index...

alter table hp_demo_2_delta add primary key (sequence);

-- track changes to the data in table hp_demo_2_delta with a trigger that 
-- sets LAST_MODIFIED_DATE on INSERT or UPDATE, for each row.

create or replace trigger trg_hp_demo_2_delta_lstmod
    before update or insert on  hp_demo_2_delta for each row
    declare
    begin
      :new.last_modified_date := sysdate;
    end;
/

-- add an index on LAST_MODIFIED_DATE so we can quickly find changed records...

create index idx_hp_demo_2_delta_1 on hp_demo_2_delta (last_modified_date);

-----------------------------------------------------------------------------------------------------------------------
-- DEMO TABLE FOR 'hp cp snap' COMMAND:
-----------------------------------------------------------------------------------------------------------------------

create table hp_demo_1_snapshot as select * from hp_demo_2_delta;

-----------------------------------------------------------------------------------------------------------------------
-- DEMO TABLE FOR 'hp sync batch' COMMAND:
-----------------------------------------------------------------------------------------------------------------------

create table hp_demo_3_sync as select * from hp_demo_2_delta;
alter table hp_demo_3_sync add primary key (sequence);
create index idx_hp_demo_3_sync_1 on hp_demo_3_sync (last_modified_date);

-- ensure sequence is fixed in table hp_demo_3_sync...

create or replace trigger trg_hp_demo_3_sync_seq
    before insert on hp_demo_3_sync for each row
    declare
    begin
        :new.sequence := seq_hp_demo.nextval;   
    end;
/

-----------------------------------------------------------------------------------------------------------------------
-- DONE...
-----------------------------------------------------------------------------------------------------------------------

exit
