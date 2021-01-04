import csv
import functools
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as F


DATA_SCHEMA = "data INT, info STRING, amount INT"
FILERS_SCHEMA = "name STRING, reg_filter STRING"


def load_data(spark, config):
    transactions = spark.read.csv(config["data"]["csv"], sep=";", schema=DATA_SCHEMA)
    filters = spark.read.csv(config["data"]["filters"], sep=";", schema=FILERS_SCHEMA)
    ignores = spark.read.csv(config["data"]["ignores"], sep=";", schema=FILERS_SCHEMA)
    year = int(config["data"]["year"])
    return transactions, filters, ignores, year


def combine_filters(filters):
    return functools.reduce(lambda a, b: a | b, filters)


def filter_by_time(data, year, month):
    date = datetime(year=year, month=month, day=1)
    start = date.timestamp()
    stop = (date + relativedelta(months=+1) - timedelta(days=1)).timestamp()
    return data.filter(data.data.between(start, stop))


def process_data(transactions, filters, ignores, year):
    ignore_filter = transactions.info.rlike(ignores.select(ignores.reg_filter).collect()[0][0])
    transactions = transactions.filter(~ignore_filter)
    filters = [x[0] for x in filters.select(filters.reg_filter).collect()]
    data = list()
    for month in range(1, 13):
        df = filter_by_time(transactions, year, month)
        result = [df.filter(df.info.rlike(x)).agg(F.sum("amount")).collect()[0][0] for x in filters]
        result = [abs(x)/100. if x else 0 for x in result]
        data.append([month] + result + [sum(result)])
        if False:
            print(f"YEAR: {year} month {month}")
            not_matched = df.filter(~combine_filters([df.info.rlike(x) for x in filters] + [ignore_filter]))
            not_matched.show(not_matched.count(), truncate=False)

    return data


def save_result(spark, config, result):
    with open(config["data"]["result"], "w+") as file:
        csv_writer = csv.writer(file, delimiter=";")
        csv_writer.writerows(result)
    spark.stop()
    return None


def run_job(spark, config):
    return save_result(spark, config, process_data(*load_data(spark, config)))
