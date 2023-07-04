from dataproc_sdk import DatioPysparkSession, DatioSchema
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
import exampleenginepythonqiyhbwvw.common.constants as c
class InitValues:

    def __init__(self):
        self.__logger = get_user_logger(InitValues.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def initialize_inputs(self,parameters):
        self.__logger.info("Using given configuration")

        clients_df = self.get_input_df(parameters,c.CLIENTS_PATH,c.CLIENTS_SCHEMA)
        contracts_df = self.get_input_df(parameters, c.CONTRACTS_PATH, c.CONTRACTS_SCHEMA)
        products_df = self.get_input_df(parameters, c.PRODUCTS_PATH, c.PRODUCTS_SCHEMA)
        #phones_df = self.get_input_df_parquet(parameters,c.PHONE_PATH,c.PHONE_SCHEMA)
        #costumers_df = self.get_input_df_parquet(parameters,c.COSTUMERS_PATH,c.COSTUMERS_SCHEMA)

        output_path,output_schema = self.get_config_by_name(parameters,"output","output_schema")

        return clients_df,contracts_df,products_df,output_path,output_schema
        """return phones_df, costumers_df,output_path, output_schema"""

    def get_config_by_name(self,parameters, key_path, key_schema):
        self.__logger.info("Get config for " + key_path)
        io_path = parameters[key_path]
        io_schema = DatioSchema.getBuilder().fromURI(parameters[key_schema]).build()
        print("------------------------------------"+str(type(io_schema)))
        return  io_path,io_schema

    def get_input_df_parquet(self,parameters,key_path,key_schema):
        self.__logger.info("Reading from "+key_path)
        io_path,io_schema = self.get_config_by_name(parameters,key_path,key_schema)
        return self.__datio_pyspark_session.read().datioSchema(io_schema)\
            .parquet(io_path)

    def get_input_df(self,parameters, key_path, key_schema):
        self.__logger.info("Reading from " + key_path)
        io_path, io_schema = self.get_config_by_name(parameters,key_path,key_schema)
        return self.__datio_pyspark_session.read().datioSchema(io_schema)\
            .option("header",c.TRUE_VALUE)\
            .option("delimiter",",")\
            .csv(io_path)
