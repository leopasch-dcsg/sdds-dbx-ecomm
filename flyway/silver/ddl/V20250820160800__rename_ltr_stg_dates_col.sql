ALTER TABLE ltr_stg_dates SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

ALTER TABLE ltr_stg_dates rename column lookback_days_minus_one to rolling_window_minus_one;