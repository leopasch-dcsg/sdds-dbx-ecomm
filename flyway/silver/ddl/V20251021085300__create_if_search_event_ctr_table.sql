CREATE TABLE if_search_event_ctr(
            date_key DATE COMMENT 'The date key for the run of this data set in the format of YYYY-MM-DD as an integer.',
            webstore STRING COMMENT 'The normalized web store name.',
            event_type STRING COMMENT 'The type of the event the table is aggregating on.',
            event_value STRING COMMENT 'The search term that was used to execute a site search.',
            facet STRING COMMENT 'The facet applied to the search term.',
            total_clicks_last_x_days BIGINT COMMENT 'The number of clicks generated from search events with this search term and facet combination over the last x days.',
            total_impressions_last_x_days BIGINT COMMENT 'The number of impressions generated from search events with this search term and facet combination over the last x days.',
            ctr DOUBLE COMMENT 'The click through rate of a search event with this search term and facet combination over the last x days.',
            CONSTRAINT `search_event_ctr_pk` PRIMARY KEY (`date_key`, `webstore`, `event_value`, `facet`))
        USING delta
        CLUSTER BY (date_key, webstore)
        TBLPROPERTIES (
                'delta.enableDeletionVectors' = 'true',
                'delta.feature.appendOnly' = 'supported',
                'delta.feature.deletionVectors' = 'supported',
                'delta.feature.invariants' = 'supported',
                'delta.minReaderVersion' = '3',
                'delta.minWriterVersion' = '7');