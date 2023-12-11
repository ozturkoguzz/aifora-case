import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import yaml
import os
from pyspark.sql import SparkSession


class SparkDuplicateChecker:
    def __init__(self, config_file='config.yml'):
        current_directory = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_directory, config_file)
        logging.basicConfig(level=logging.INFO)
        
        self.config = self.load_config(config_path)
        self.spark_app_name = self.config.get('SparkConfig', {}).get('app_name', 'duplicate_check_app')
        self.spark_master = self.config.get('SparkConfig', {}).get('master', 'local[*]')
        self.spark = self.create_spark_session(self.spark_app_name, self.spark_master)

    def load_config(self, file_path):
        try:
            with open(file_path, 'r') as file:
                config = yaml.safe_load(file)
            return config
        except Exception as e:
            logging.error(f"Error loading config: {str(e)}")
            return {}

    def create_spark_session(self, app_name, spark_master):
        return SparkSession.builder.appName(app_name).master(spark_master).getOrCreate()

    def check_duplicates(self, dataframe, columns):
        if not self.spark or not dataframe:
            raise ValueError("Spark session and DataFrame are required.")

        if not isinstance(columns, list):
            raise ValueError("Columns should be a list.")

        for col in columns:
            if col not in dataframe.columns:
                raise ValueError(f"Column '{col}' not found in the DataFrame.")

        window_spec = Window.partitionBy(columns).orderBy(F.col(columns[0]))
        dataframe = dataframe.withColumn("count", F.count("*").over(window_spec))

        duplicates = dataframe.filter(dataframe["count"] > 1)

        count = duplicates.count()

        samples = (
            duplicates
            .groupBy(columns)
            .agg(F.count("*").alias("number_of_duplicates"))
        )

        result = {'count': count, 'samples': samples}

        return result

    def run_duplicate_check(self, dataframe, columns):
        try:
            result = self.check_duplicates(dataframe, columns)
            logging.info(f"Duplicates for {columns}")
            result['samples'].show()

        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
        finally:
            self.spark.stop()