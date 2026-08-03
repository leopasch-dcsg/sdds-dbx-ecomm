CREATE TABLE if_search_event_financials(
            date_key DATE COMMENT 'The date key for the run of this data set in the format of YYYY-MM-DD as an integer.',
            webstore STRING COMMENT 'The normalized web store name.',
            event_type STRING COMMENT 'The type of the event the table is aggregating on.',
            event_value STRING COMMENT 'The search term that was used to execute a site search.',
            facet STRING COMMENT 'The facet applied to the search term.',
            avg_cost_last_x_days DECIMAL(38,6) COMMENT 'The average cost of a search event with this search term and facet combination over the last x days.',
            avg_revenue_last_x_days DOUBLE COMMENT 'The average revenue generated from a search event with this search term and facet combination over the last x days.',
            avg_profit_last_x_days DOUBLE COMMENT 'The average profit generated from a search event with this search term and facet combination over the last x days.',
            CONSTRAINT `search_event_fin_pk` PRIMARY KEY (`date_key`, `webstore`, `event_value`, `facet`))
        USING delta
        CLUSTER BY (date_key)
        TBLPROPERTIES (
                'delta.enableDeletionVectors' = 'true',
                'delta.feature.appendOnly' = 'supported',
                'delta.feature.deletionVectors' = 'supported',
                'delta.feature.invariants' = 'supported',
                'delta.minReaderVersion' = '3',
                'delta.minWriterVersion' = '7');