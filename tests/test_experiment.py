from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from exampleenginepythonqiyhbwvw.experiment import DataprocExperiment

class TestApp(TestCase):
    def test_run_experiment(self):
        parameters = {"output":"resources/data/output/final_table",
                      "clients" : "resources/data/input/clients.csv",
                      "contracts" : "resources/data/input/contracts.csv",
                      "products" : "resources/data/input/products.csv",
                      "clients_schema" : "resources/schemas/clients_schema.json",
                      "contracts_schema" : "resources/schemas/contracts_schema.json",
                      "products_schema" : "resources/schemas/products_schema.json",
                      "output_schema" : "resources/schemas/output_schema.json"
                    }

        experiment = DataprocExperiment()
        experiment.run(**parameters)

        spark = SparkSession.builder.appName("Test_unite").master("local[*]").getOrCreate()

        out_df = spark.read.parquet(parameters["output"])

        self.assertIsInstance(out_df,DataFrame)
