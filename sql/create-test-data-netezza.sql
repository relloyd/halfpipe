drop table test_data_types;

create table test_data_types_1 (
                                   BIGINT_1	bigint	,
                                   BOOLEAN_1	boolean	,
                                   BPCHAR_1	bpchar	,
                                   BYTEINT_1	byteint	,
                                   CHAR_1	char(10)	,
                                   DATE_1	date	,
                                   DECIMAL_1	decimal	,
                                   DOUBLE_1	double	,
                                   FLOAT_1	float	,
                                   INTEGER_1	integer ,
                                   INTERVAL_DAY_TO_HOUR_1	interval day to hour ,
                                   INTERVAL_DAY_TO_MINUTE_1	interval day to minute	,
                                   INTERVAL_DAY_TO_SECOND_1	interval day to second	,
                                   INTERVAL_DAY_1	interval day	,
                                   INTERVAL_HOUR_TO_MINUTE_1	interval hour to minute	,
                                   INTERVAL_HOUR_TO_SECOND_1	interval hour to second	,
                                   INTERVAL_HOUR_1	interval hour	,
                                   INTERVAL_MINUTE_TO_SECOND_1	interval minute to second ,
                                   INTERVAL_MINUTE_1	interval minute	,
                                   INTERVAL_MONTH_1	interval month	,
                                   INTERVAL_SECOND_1	interval second	,
                                   INTERVAL_YEAR_TO_MONTH_1	interval year to month	,
                                   INTERVAL_YEAR_1	interval year ,
                                   JSONB_1	jsonb	,
--  moved to dedicatede table:     JSONPATH_1	jsonpath	,
--  moved to dedicatede table:     JSON_1	json	,
                                   NCHAR_1	nchar(20)	,
                                   NUMERIC_1	numeric(10,10) ,
                                   NVARCHAR_1	nvarchar(30)	,
                                   REAL_1	real	,
                                   SMALLINT_1	smallint	,
                                   ST_GEOMETRY_1	st_geometry(10)	,
                                   TIMESTAMP_1	timestamp	,
                                   TIMETZ_1	timetz	,
                                   TIME_1	time	,
                                   VARBINARY_1	varbinary(8)	,
                                   VARCHAR_1	varchar(40)
);

--truncate table TEST_DATA_TYPES_1;

insert into test_data_types_1 (
    BIGINT_1, BOOLEAN_1, BPCHAR_1, BYTEINT_1, CHAR_1, DATE_1, DECIMAL_1, DOUBLE_1, FLOAT_1, INTEGER_1,
    INTERVAL_DAY_TO_HOUR_1,
    INTERVAL_DAY_TO_MINUTE_1,
    INTERVAL_DAY_TO_SECOND_1,
    INTERVAL_DAY_1,
    INTERVAL_HOUR_TO_MINUTE_1,
    INTERVAL_HOUR_TO_SECOND_1,
    INTERVAL_HOUR_1,
    INTERVAL_MINUTE_TO_SECOND_1,
    INTERVAL_MINUTE_1,
    INTERVAL_MONTH_1,
    INTERVAL_SECOND_1,
    INTERVAL_YEAR_TO_MONTH_1,
    INTERVAL_YEAR_1,
    JSONB_1,
    NCHAR_1,
    NUMERIC_1,
    NVARCHAR_1,
    REAL_1,
    SMALLINT_1,
    --missing geo field: ST_GEOMETRY_1
    TIMESTAMP_1,
    TIMETZ_1,
    TIME_1,
    VARBINARY_1, VARCHAR_1
)
values (
    123, 'True', 'V', 1, 'A', '2021-10-10 23:00:00', 11, 100, 0.123, 5,
    '1 day 1 hour',
    '1 day 1 minute',
    '1 day 1 second',
    '1 day',
    '1 hour 1 minute',
    '1 hour 1 second',
    '1 hour',
    '1 minute 1 second',
    '1 minute',
    '1 month',
    '1 second',
    '1 year 1 month',
    '1 year',
    '{"key":"value"}',
    'N',
    0.123,
    'N',
    0.1234, -- real
    1,
    -- TODO: missing ST_GEOMETRY field on this line.
    '2021-07-07 22:00:00.00',
    '13:21:45+01:00',
    '14:31:59',
    hex_to_binary('FF'),
    'V'
);

select * from TEST_DATA_TYPES_1;

-- JSONPATH_1
select * from TEST_DATA_TYPES_2;

insert into TEST_DATA_TYPES_2 (JSONPATH_1) values ('$.path.to.json');

-- JSON_1
select * from TEST_DATA_TYPES_3;

insert into TEST_DATA_TYPES_3 (JSON_1) values ('{"key": "value"}');

