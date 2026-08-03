ALTER TABLE ${co_stage_schema}.order_message_placed
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

ALTER TABLE ${co_stage_schema}.order_message_placed
ALTER COLUMN associations AFTER aosassociateid;