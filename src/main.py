import time
import importlib
import configparser
from pyspark.sql import SparkSession


CONFIG = configparser.ConfigParser()
CONFIG.read("../config.ini")


def main():
    """ Main function excecuted by spark-submit command"""

    spark = SparkSession.builder.appName(CONFIG["spark"]["app_name"]).getOrCreate()
    job_name = "analyse_transaction"
    job_module = importlib.import_module(f"jobs.{job_name}")

    start_time = time.time()
    job_module.run_job(spark, CONFIG)
    print(f"time elapsed: {(time.time() - start_time):.2f}s")


if __name__ == "__main__":
    main()
