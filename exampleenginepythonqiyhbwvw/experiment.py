
from typing import Dict
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import SparkSession, DataFrame

from exampleenginepythonqiyhbwvw.business_logic.business_logic import BusinessLogic


class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(DataprocExperiment.__qualname__)
        self.__spark = SparkSession.builder.getOrCreate()

    def run(self, **parameters: Dict) -> None:
        """
        Execute the code written by the user.

        Args:
            parameters: The config file parameters
        """
        self.__logger.info("Executing experiment")
        clients_df = self.read_csv("clients",parameters)
        contracts_df = self.read_csv("contracts", parameters)
        products_df = self.read_csv("products", parameters)

        logic = BusinessLogic()
        filtered_clients = logic.filter_by_age_and_vip(clients_df)
        joined_df: DataFrame = logic.join_tables(filtered_clients,contracts_df,products_df)
        filter_number:DataFrame = logic.filter_by_number_of_contracts(joined_df)
        hash_columns: DataFrame = logic.hash_columns(filter_number)
        hash_columns.show()

    def read_csv(self,table_id, parameter):
        return self.__spark.read\
            .option("header","true")\
            .option("delimiter",",")\
            .csv(str(parameter[table_id]))
