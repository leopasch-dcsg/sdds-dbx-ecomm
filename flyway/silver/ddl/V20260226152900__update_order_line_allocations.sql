CREATE OR REPLACE VIEW order_line_allocations (
        capture_timestamp_eastern,
        released_timestamp_eastern,
        last_updated_timestamp_eastern,
        fulfillment_mode COMMENT 'Fulfillment mode chosen by Athlete at checkout.',
        order_id COMMENT 'Customer order number as defined by Athlete Order.',
        line_number COMMENT 'Customer order line number as defined by Athlete Order. A unique line number is given to each unique sku on the order, starting from 1.',
        line_seq_number COMMENT 'Customer order line sequence number as defined by Athlete Order. A unique line sequence number is given to each unique item on a given line, starting from 0.',
        fr_number,
        demand_unit,
        sku COMMENT 'Item sku as defined by Athlete Order.',
        facility_number COMMENT 'Facility number where item will be picked up.')
WITH SCHEMA COMPENSATION
AS WITH completed_frs (
        SELECT convert_timezone('UTC', 'America/New_York', o.order_capture_timestamp)   capture_timestamp_eastern,
               convert_timezone('UTC', 'America/New_York', fr.released_timestamp)       released_timestamp_eastern,
               convert_timezone('UTC', 'America/New_York', i.nmn_updated_timestamp)     last_updated_timestamp_eastern,
               COALESCE(fr.executed_fulfillment_mode,i.ordered_fulfillment_mode)        fulfillment_mode,
               fr.order_id                                                              order_id,
               fr.line_number                                                           line_number,
               fr.line_seq_number                                                       line_seq_number,
               CONCAT(fr.order_id, '.', LPAD(fr.fr_sequence_number, 3, 0))              fr_number,
               1                                                                        demand_unit,
               fr.sku                                                                   sku,
               fr.pickup_facility_number                                                facility_number
          FROM ${oso_silver_catalog}.oso.newman_fr_history fr
            LEFT JOIN ${oso_silver_catalog}.oso.newman_order o
                ON o.order_id = fr.order_id
            LEFT JOIN ${oso_silver_catalog}.oso.newman_item i
                ON i.order_id = fr.order_id
               AND i.line_number = fr.line_number
               AND i.line_seq_number = fr.line_seq_number
         WHERE fr.managed_by = 'newman'
           AND fr.routing_partner = 'fit'
           AND fr.source_facility_type <> 'DC'
    ),
    open_frs (
        SELECT convert_timezone('UTC', 'America/New_York', o.order_capture_timestamp)   capture_timestamp_eastern,
               convert_timezone('UTC', 'America/New_York', i.released_timestamp)        released_timestamp_eastern,
               convert_timezone('UTC', 'America/New_York', i.nmn_updated_timestamp)     last_updated_timestamp_eastern,
               i.ordered_fulfillment_mode                                               fulfillment_mode,
               i.order_id                                                               order_id,
               i.line_number                                                            line_number,
               i.line_seq_number                                                        line_seq_number,
               CONCAT(i.order_id, '.', LPAD(i.fr_sequence_number, 3, 0))                fr_number,
               1                                                                        demand_unit,
               i.sku                                                                    sku,
               i.pickup_facility_number                                                 facility_number
          FROM ${oso_silver_catalog}.oso.newman_item i
            LEFT JOIN ${oso_silver_catalog}.oso.newman_order o
                ON o.order_id = i.order_id
       WHERE i.managed_by = 'newman' and nvl(i.routing_partner, 'fit') = 'fit'
         AND i.item_status >= 300 AND i.item_status < 790
         AND i.source_facility_type <> 'DC'
         AND NOT EXISTS (
            SELECT 1
            FROM completed_frs c
            WHERE c.order_id = i.order_id
              AND c.line_number = i.line_number
              AND c.line_seq_number = i.line_seq_number
              AND c.fr_number= CONCAT(i.order_id, '.', LPAD(i.fr_sequence_number, 3, 0))
        )
    ),
    all_frs (
        SELECT * FROM completed_frs
        UNION ALL
        SELECT * FROM open_frs
    )

    SELECT *
      FROM all_frs
    ORDER BY released_timestamp_eastern;
