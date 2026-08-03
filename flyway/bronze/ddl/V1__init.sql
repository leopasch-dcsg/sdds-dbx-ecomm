create table watermarks
(
    catalog_name string not null COMMENT 'The catalog of the destination table that is being incrementally updated.',
    schema_name string not null COMMENT 'The schema name of the destination table that is being incrementally updated.',
    table_name string not null  COMMENT 'The destination table name that is being incrementally updated.',
    watermark_epoch_sec_utc bigint COMMENT 'The watermark timestamp in UTC tz that is the last time we fetched data for the destination table',
    constraint watermark_pk primary key (catalog_name, schema_name, table_name)
)  COMMENT 'This table acts as a watermark indicator for incrementally fetching data from one or more source tables into a destination table. This table is used internally by the E-Commerce Data Engineering team.';