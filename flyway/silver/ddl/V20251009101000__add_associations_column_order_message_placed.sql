ALTER TABLE co_stage_order_message_placed
ADD COLUMNS (
  associations ARRAY<STRUCT<
    associationtype STRING,
    associationdetails ARRAY<STRUCT<
      ordernumber STRING,
      linenumber STRING,
      sequence STRING
    >>
  >>
);

ALTER TABLE co_stage_order_message_placed
ALTER COLUMN associations AFTER aosassociateid;



ALTER TABLE co_stage_order_message_placed_quarantine
ADD COLUMNS (
  associations ARRAY<STRUCT<
    associationtype STRING,
    associationdetails ARRAY<STRUCT<
      ordernumber STRING,
      linenumber STRING,
      sequence STRING
    >>
  >>
);

ALTER TABLE co_stage_order_message_placed_quarantine
ALTER COLUMN associations AFTER aosassociateid;
