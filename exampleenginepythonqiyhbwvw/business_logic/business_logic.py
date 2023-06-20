from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as f


class BusinessLogic:
    """
    Just a wrapper class to ease the user code execution
    """

    def __init__(self):
        """
        Constructor
        """

        self.__logger = get_user_logger(BusinessLogic.__qualname__)

    def filter_by_age_and_vip(self, df: DataFrame) -> DataFrame :
        self.__logger.info("Applying filter by age and vip status")
        return df.filter((f.col("edad") >= 30) & (f.col("edad") <= 50) & (f.col("vip") == "true"))

    def join_tables(self,clients_df: DataFrame,contracts_df: DataFrame, products_df: DataFrame) -> DataFrame:
        self.__logger.info("Applying join process")
        full_join = clients_df.join(contracts_df, f.col("cod_client") == f.col("cod_titular"),"inner")\
            .join(products_df, ["cod_producto"], "inner")
        return full_join

    def filter_by_number_of_contracts(self, df: DataFrame):
        self.__logger.info("Filtering by number of contracts")
        filter_number = df.select(*df.columns,\
                                  f.count("cod_client").over(Window.partitionBy("cod_client"))\
                                  .alias("count")).filter(f.col("count") > 3).drop("count")
        return filter_number

    def hash_columns(self,df: DataFrame) -> DataFrame:
        self.__logger.info("Generating hash column")
        return df.select(*df.columns,f.sha2(f.concat_ws("||",*df.columns),256).alias("hash"))
