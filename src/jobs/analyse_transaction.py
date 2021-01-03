import csv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F


DATA_SCHEMA = "data INT, info STRING, amount INT"
FILERS_SCHEMA = "name STRING, reg_filter STRING"


def load_data(spark, config):
    transactions = spark.read.csv(config["data"]["csv"], sep=";", schema=DATA_SCHEMA)
    filters = spark.read.csv(config["data"]["filters"], sep=";", schema=FILERS_SCHEMA)
    ignores = spark.read.csv(config["data"]["ignores"], sep=";", schema=FILERS_SCHEMA)
    return transactions, filters, ignores


def filter_by_time(data, year, month):
    date = datetime(year=year, month=month, day=1)
    start = date.timestamp()
    stop = (date + relativedelta(months=+1) - timedelta(days=1)).timestamp()
    return data.filter(data.data.between(start, stop))


def process_data(transactions, filters, ignores):
    transactions = transactions.filter(~transactions.info.rlike(ignores.select(ignores.reg_filter).collect()[0][0]))
    filters = [x[0] for x in filters.select(filters.reg_filter).collect()]
    data = list()
    for month in range(1, 13):
        df = filter_by_time(transactions, 2020, month)
        result = [df.filter(df.info.rlike(x)).agg(F.sum("amount")).collect()[0][0] for x in filters]
        result = [abs(x) if x else 0 for x in result]
        data.append([month] + result + [sum(result)])

    return data


def save_result(spark, config, result):
    with open(config["data"]["result"], "w+") as file:
        csv_writer = csv.writer(file, delimiter=";")
        csv_writer.writerows(result)
    spark.stop()
    return None


def run_job(spark, config):
    return save_result(spark, config, process_data(*load_data(spark, config)))
