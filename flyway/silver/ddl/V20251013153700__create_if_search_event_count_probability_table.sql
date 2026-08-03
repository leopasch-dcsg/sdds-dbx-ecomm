CREATE TABLE if_search_event_counts_probabilities(
            date_key DATE COMMENT 'The date key for the run of this data set in the format of YYYY-MM-DD as an integer.',
            webstore STRING COMMENT 'The normalized web store name.',
            event_type STRING COMMENT 'The type of the event the table is aggregating on.',
            event_value STRING COMMENT 'The search term that was used to execute a site search.',
            facet STRING COMMENT 'The facet applied to the search term.',
            search_events_last_x_days BIGINT COMMENT 'The number of times the search term was used in the last x days in a specific webstore.',
            p_event_last_x_days DOUBLE COMMENT 'The probability that any search in a specific webstore over the last x days was this search event.',
            facet_count_last_x_days BIGINT COMMENT 'The number of times a facet was applied to a search term in the last x days.',
            p_facet_last_x_days DOUBLE COMMENT 'The probability that the search term has this specific facet applied to it over the last x days, assuming a facet was applied to the search term.',
            CONSTRAINT `search_event_pk` PRIMARY KEY (`date_key`, `webstore`, `event_value`, `facet`))
        USING delta
        CLUSTER BY (date_key)
        TBLPROPERTIES (
                'delta.enableDeletionVectors' = 'true',
                'delta.feature.appendOnly' = 'supported',
                'delta.feature.deletionVectors' = 'supported',
                'delta.feature.invariants' = 'supported',
                'delta.minReaderVersion' = '3',
                'delta.minWriterVersion' = '7');