from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from ecmde_ecomm.common import AppendOperation


class IntelligentFilteringMetricsModelAppend(AppendOperation):
    def batch_dataframe(self) -> DataFrame:
        self.spark.sql("CLEAR CACHE")
        end_date = datetime.strptime(self.date_info.end_date_est, "%Y-%m-%d").date()

        df = (self.spark.sql( f"""
                    SELECT 
                      date_key,
                      webstore,
                      event_type,
                      event_value,
                      facet,
                      browse_events_last_x_days as event_count_last_x_days,
                      p_event_last_x_days,
                      facet_count_last_x_days,
                      p_facet_last_x_days 
                    FROM {self.source.catalog}.{self.source.schema}.if_browse_event_counts_probabilities
                    WHERE date_key = to_date('{end_date}')
                    UNION ALL
                    SELECT 
                      date_key,
                      webstore,
                      event_type,
                      event_value,
                      facet,
                      search_events_last_x_days as event_count_last_x_days,
                      p_event_last_x_days,
                      facet_count_last_x_days,
                      p_facet_last_x_days
                    FROM {self.source.catalog}.{self.source.schema}.if_search_event_counts_probabilities
                    WHERE date_key = to_date('{end_date}')
                """)
              .select("*"))

        return df