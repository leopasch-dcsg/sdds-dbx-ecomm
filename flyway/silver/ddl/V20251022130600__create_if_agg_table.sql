CREATE TABLE if_event_metrics_model(
            date_key DATE COMMENT 'The date key for the run of this data set in the format of YYYY-MM-DD as an integer.',
            webstore STRING COMMENT 'The normalized web store name.',
            event_type STRING COMMENT 'The type of the event the table is aggregating on.',
            event_value STRING COMMENT 'The category ID of the family page that was browsed by an athlete, or the search term that was used to execute a site search.',
            facet STRING COMMENT 'The facet applied to the family page or the search term.',
            event_count_last_x_days BIGINT COMMENT 'The number of times the family page was navigated to in the last x days in a specific webstore or the number of times the search term was used in the last x days in a specific webstore.',
            p_event_last_x_days DOUBLE COMMENT 'The probability that any browse event or search in a specific webstore over the last x days went to this family page.',
            facet_count_last_x_days BIGINT COMMENT 'The number of times a facet was applied to this family page or search term in the last x days.',
            p_facet_last_x_days DOUBLE COMMENT 'The probability that the family page or search term has this specific facet applied to it over the last x days, assuming a facet was applied to the family page.',
            CONSTRAINT `if_agg_pk` PRIMARY KEY (`date_key`, `webstore`, `event_type`, `event_value`, `facet`))
        USING delta
        CLUSTER BY AUTO
        TBLPROPERTIES (
                'delta.enableDeletionVectors' = 'true',
                'delta.feature.appendOnly' = 'supported',
                'delta.feature.deletionVectors' = 'supported',
                'delta.feature.invariants' = 'supported',
                'delta.minReaderVersion' = '3',
                'delta.minWriterVersion' = '7');