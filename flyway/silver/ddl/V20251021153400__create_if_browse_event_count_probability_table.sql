CREATE TABLE if_browse_event_counts_probabilities(
            date_key DATE COMMENT 'The date key for the run of this data set in the format of YYYY-MM-DD as an integer.',
            webstore STRING COMMENT 'The normalized web store name.',
            event_type STRING COMMENT 'The type of the event the table is aggregating on.',
            event_value STRING COMMENT 'The category ID of the family page that was browsed by an athlete.',
            facet STRING COMMENT 'The facet applied to the family page.',
            browse_events_last_x_days BIGINT COMMENT 'The number of times the family page was navigated to in the last x days in a specific webstore.',
            p_event_last_x_days DOUBLE COMMENT 'The probability that any browse event in a specific webstore over the last x days went to this family page.',
            facet_count_last_x_days BIGINT COMMENT 'The number of times a facet was applied to this family page in the last x days.',
            p_facet_last_x_days DOUBLE COMMENT 'The probability that the family page has this specific facet applied to it over the last x days, assuming a facet was applied to the family page.',
            CONSTRAINT `browse_event_pk` PRIMARY KEY (`date_key`, `webstore`, `event_value`, `facet`))
        USING delta
        CLUSTER BY (date_key, webstore)
        TBLPROPERTIES (
                'delta.enableDeletionVectors' = 'true',
                'delta.feature.appendOnly' = 'supported',
                'delta.feature.deletionVectors' = 'supported',
                'delta.feature.invariants' = 'supported',
                'delta.minReaderVersion' = '3',
                'delta.minWriterVersion' = '7');