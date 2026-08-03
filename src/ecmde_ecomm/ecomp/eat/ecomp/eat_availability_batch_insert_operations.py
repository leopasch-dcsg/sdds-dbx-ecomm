from datetime import datetime

from pyspark.sql import DataFrame

from ecmde_ecomm.common.dbx.etl.batch_insert import BatchInsert


class StageNrtInventoryBatchInsert(BatchInsert):
    def batch_dataframe(self, last_batch_date_utc: datetime) -> DataFrame:
        return self.spark.sql(
            f"""
            select inv.sku                          item_id,
                   inv.location_id                  store_id,
                   inv.atp_qty                      atp_inventory_qty,
                   inv.inventory_change_event_utc   create_date
              from {self.config.source_table_qualified()} inv
             where inv.location_id = 0
               and (
                    inv.silver_created_on_utc >= '{last_batch_date_utc}'
                 or inv.silver_updated_on_utc >= '{last_batch_date_utc}'
               );
            """
        )
