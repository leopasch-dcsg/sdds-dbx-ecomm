from uuid import uuid5, NAMESPACE_OID
import pandas as pd
from pyspark.sql.types import StringType
from pyspark.sql.functions import pandas_udf


@pandas_udf(StringType())
def create_uuid5(value: pd.Series) -> pd.Series:
    return value.apply(lambda x: str(uuid5(NAMESPACE_OID, str(x))))
