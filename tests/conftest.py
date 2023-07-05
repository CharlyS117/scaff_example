"""Execute the pytest initialization.

Remove it under your own responsibility.
"""
import glob
import sys
from pathlib import Path

from pyspark.sql.types import StructType, Row, StructField, StringType

import exampleenginepythonqiyhbwvw.common.constants as c
import pytest
from pyspark.sql import SparkSession

from exampleenginepythonqiyhbwvw.business_logic.business_logic import BusinessLogic
from exampleenginepythonqiyhbwvw.io.init_values import InitValues


@pytest.fixture(scope="function", autouse=True)
def spark_test():
    """Execute the module setup and initialize the SparkSession.

    Warning: if making any modifications, please make sure that this fixture is executed
    at the beginning of the test session, as the DatioPysparkSession will rely on it internally.

    Yields:
        SparkSession: obtained spark session

    Raises:
        FileNotFoundError: if the jar of Dataproc SDK is not found.
    """
    # Get Dataproc SDK jar path
    jars_path = str(Path(sys.prefix) / 'share' / 'sdk' / 'jars' / 'dataproc-sdk-all-*.jar')
    jars_list = glob.glob(jars_path)
    if jars_list:
        sdk_path = jars_list[0]
    else:
        raise FileNotFoundError(f"Dataproc SDK jar not found in {jars_path}, have you installed the requirements?")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("unittest_job") \
        .config("spark.jars", sdk_path) \
        .master("local[*]") \
        .getOrCreate()

    yield spark

    spark.stop()

@pytest.fixture(scope="function",autouse=True)
def business_logic():
    yield BusinessLogic()

@pytest.fixture(scope="function",autouse=True)
def init_values():
    yield InitValues()

@pytest.fixture(scope="function",autouse=True)
def clients_df(init_values):
    parameters_clients = {
        "clients" : "resources/data/input/clients.csv",
        "clients_schema" : "resources/schemas/clients_schema.json"
    }
    yield init_values.get_input_df(parameters_clients,c.CLIENTS_PATH,c.CLIENTS_SCHEMA)

@pytest.fixture(scope="function",autouse=True)
def contracts_df(init_values):
    parameters_clients = {
        "contracts" : "resources/data/input/contracts.csv",
        "contracts_schema" : "resources/schemas/contracts_schema.json"
    }
    yield init_values.get_input_df(parameters_clients,c.CONTRACTS_PATH,c.CONTRACTS_SCHEMA)

@pytest.fixture(scope="function",autouse=True)
def products_df(init_values):
    parameters_clients = {
        "products" : "resources/data/input/products.csv",
        "products_schema" : "resources/schemas/products_schema.json"
    }
    yield init_values.get_input_df(parameters_clients,c.PRODUCTS_PATH,c.PRODUCTS_SCHEMA)

@pytest.fixture(scope="function",autouse=True)
def contracts_dummy_df(spark_test):
    data = [Row("111","aaa"),Row("123","bbb")]
    schema = StructType([
        StructField("cod_titular",StringType()),
        StructField("cod_producto", StringType())
    ])

    yield spark_test.createDataFrame(data,schema)

@pytest.fixture(scope="function",autouse=True)
def clients_dummy_df(spark_test):
    data = [Row("111"),Row("111"),Row("111"),Row("111"),Row("123"),Row("123"),Row("444"),Row("444"),Row("555"),Row("777")]
    schema = StructType([StructField("cod_client",StringType())])

    yield spark_test.createDataFrame(data,schema)

@pytest.fixture(scope="function",autouse=True)
def products_dummy_df(spark_test):
    data = [Row("aaa"),Row("bbb")]
    schema = StructType([StructField("cod_producto",StringType())])

    yield spark_test.createDataFrame(data,schema)
