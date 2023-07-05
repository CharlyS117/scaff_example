from unittest.mock import MagicMock

import dataproc_sdk
from dataproc_sdk import DatioSchema
from pyspark.sql import DataFrame

import exampleenginepythonqiyhbwvw.common.constants as c
from exampleenginepythonqiyhbwvw.business_logic.business_logic import BusinessLogic
from exampleenginepythonqiyhbwvw.config import get_params_from_runtime
from exampleenginepythonqiyhbwvw.io.init_values import InitValues


def test_init_values(spark_test):
    parameters = {"output": "resources/data/output/final_table",
                  "clients": "resources/data/input/clients.csv",
                  "contracts": "resources/data/input/contracts.csv",
                  "products": "resources/data/input/products.csv",
                  "clients_schema": "resources/schemas/clients_schema.json",
                  "contracts_schema": "resources/schemas/contracts_schema.json",
                  "products_schema": "resources/schemas/products_schema.json",
                  "output_schema": "resources/schemas/output_schema.json"
                  }

    """config_loader = spark_test._jvm.com.datio.dataproc.sdk.laucher.process.config.ProcessConfigLoader()
    config = config_loader.fromPath("resources/application.conf")

    runtimeContext = MagicMock()
    runtimeContext.getConfig.return_value = config
    root_key = "EnvironmentVarsPM"
    parameters = get_params_from_runtime(runtimeContext,root_key)"""

    init_values = InitValues()

    clients_df,contracts_df,products_df,output_path,output_schema = init_values.initialize_inputs(parameters)

    assert type(clients_df) == DataFrame
    assert type(contracts_df) == DataFrame
    assert type(products_df) == DataFrame
    assert type(output_path) == str
    assert type(output_schema) == DatioSchema

    assert products_df.columns == [c.COD_PRODUCTO,c.DESC_PRODUCTO]

def test_get_inputs(spark_test):
    parameters = {"output": "resources/data/output/final_table",
                  "clients": "resources/data/input/clients.csv",
                  "contracts": "resources/data/input/contracts.csv",
                  "products": "resources/data/input/products.csv",
                  "clients_schema": "resources/schemas/clients_schema.json",
                  "contracts_schema": "resources/schemas/contracts_schema.json",
                  "products_schema": "resources/schemas/products_schema.json",
                  "output_schema": "resources/schemas/output_schema.json"
                  }
    init_values = InitValues()
    clients_df = init_values.get_input_df(parameters, c.CLIENTS_PATH, c.CLIENTS_SCHEMA)
    contracts_df = init_values.get_input_df(parameters, c.CONTRACTS_PATH, c.CONTRACTS_SCHEMA)
    products_df = init_values.get_input_df(parameters, c.PRODUCTS_PATH, c.PRODUCTS_SCHEMA)

    assert type(clients_df) == DataFrame
    assert type(contracts_df) == DataFrame
    assert type(products_df) == DataFrame


def test_get_config_by_name(spark_test):
    parameters = {"output": "resources/data/output/final_table",
                  "clients": "resources/data/input/clients.csv",
                  "contracts": "resources/data/input/contracts.csv",
                  "products": "resources/data/input/products.csv",
                  "clients_schema": "resources/schemas/clients_schema.json",
                  "contracts_schema": "resources/schemas/contracts_schema.json",
                  "products_schema": "resources/schemas/products_schema.json",
                  "output_schema": "resources/schemas/output_schema.json"
                  }
    init_values = InitValues()
    io_path,io_schema = init_values.get_config_by_name(parameters,"output","output_schema")
    assert type(io_path) == str
    assert type(io_schema) == dataproc_sdk.dataproc_sdk_schema.datioschema.DatioSchema
