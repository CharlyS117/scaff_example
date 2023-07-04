from typing import Dict

from dataproc_sdk import DatioPysparkSession
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, when, col, lit

from exampleenginepythonqiyhbwvw.business_logic.business_logic import BusinessLogic
from exampleenginepythonqiyhbwvw.io.init_values import InitValues


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
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def run(self, **parameters: Dict) -> None:
        """
        Execute the code written by the user.

        Args:
            parameters: The config file parameters
        """

        self.__logger.info("Executing experiment")

        init_values = InitValues()
        clients_df, contracts_df, products_df, output_path, output_schem = \
            init_values.initialize_inputs(parameters)

        """phones_df, customers_df, output_path_phones, output_schema = \
            init_values.initialize_inputs(parameters)"""


        """clients_df = self.read_csv("clients",parameters)
        contracts_df = self.read_csv("contracts", parameters)
        products_df = self.read_csv("products", parameters)"""

        logic = BusinessLogic()
        filtered_clients = logic.filter_by_age_and_vip(clients_df)
        joined_df: DataFrame = logic.join_tables(filtered_clients,contracts_df,products_df)
        filter_number:DataFrame = logic.filter_by_number_of_contracts(joined_df)
        hash_columns: DataFrame = logic.hash_columns(filter_number)

        final_df: DataFrame = hash_columns\
            .withColumn("hash", when(col("activo") == "false", lit("0")).otherwise(col("hash")))\
            .filter(col("hash")==0)
        final_df.show()

        """final_df.write.mode("overwrite").partitionBy("cod_producto","activo")\
            .option("partitionOverwriteMode","dynamic")\
            .parquet(parameters["output"])"""

        self.__datio_pyspark_session.write().mode("overwrite")\
            .option("partitionOverwriteMode","dynamic") \
            .partition_by(["cod_producto", "activo"]) \
            .parquet(final_df,output_path)




        """phones_df = self.read_parquet("phones",parameters)
        customers_df = self.read_parquet("costumers",parameters)

        filter_phones = logic.filter_phones(phones_df)
        filter_curtomers = logic.filter_customers(customers_df)
        join_dfs = logic.join_customers_phone(filter_phones,filter_curtomers)

        vip_df = logic.customer_vip(join_dfs)
        discount = logic.discount(vip_df)
        final_price = logic.final_prices(discount)
        jwk_date = logic.paste_date(final_price,parameters["jwkDate"])
        age = logic.age(jwk_date)
        age.show()
"""
        """self.__datio_pyspark_session.write().mode("overwrite") \
            .option("partitionOverwriteMode", "dynamic") \
            .partition_by(["jwk_date"]) \
            .datio_schema(output_schema)\
            .parquet(logic.select_schema(age), output_path_phones)"""

        """age.write.mode("overwrite").partitionBy("jwk_date")\
            .option("partitionOverwriteMode","dynamic")\
            .parquet(parameters["output"])"""


    def read_csv(self,table_id, parameter):
        return self.__spark.read\
            .option("header","true")\
            .option("delimiter",",")\
            .csv(str(parameter[table_id]))

    def read_parquet(self,table_id,parameter):
        return self.__spark.read\
            .parquet(str(parameter[table_id]))
