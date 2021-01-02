#!venv/bin/python
import time
import configparser
import functools
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from google_sheet_api import GoogleSheetApi

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CONFIG = configparser.ConfigParser()
CONFIG.read("config.ini")

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = CONFIG["sheet"]["id"]
FILTERS_RANGE = CONFIG["sheet"]["filters"]
IGNORE_FILTER = CONFIG["sheet"]["ignore"]
TOKEN_PICKLE = "token.pickle"
CREDENTIALS_JSON = "credentials.json"
SHEETS_API_VERSION = "v4"

APP_NAME = "CsvAnalyser"
MASTER_NAME = "local[1]"
DF_SCHEMA = "data INT, info STRING, amount INT"


class CsvReader:
    def __init__(self, cvs_path):
        self.spark = SparkSession.builder.appName(APP_NAME).master(MASTER_NAME).getOrCreate()
        self.df: DataFrame
        self._load_csv(cvs_path)

    def print_data_frame(self):
        self.df.printSchema()

    def show(self):
        pass
        # self.df.show()

    def analyse_month(self, filters, date, ignore=None):
        df = self._filter_df(self.df, self._prepare_time_filter(date))
        if ignore:
            df = self._filter_df(df, ~self._prepare_filters(list(*ignore)))
        filters = {key: self._prepare_filters(value) for key, value in filters.items()}
        summary = {key: self._sum_amount(key, self._filter_df(df, value)) for key, value in filters.items()}
        not_filtered = self._filter_df(df, ~self._combine_filters(filters.values()))
        # not_filtered.show(not_filtered.count(), truncate=False)
        summary["Sum"] = sum(summary.values())
        return summary

    @staticmethod
    def _sum_amount(key, df):
        if df.count() == 0:
            return 0
        # print(key)
        # df.show(df.count(), truncate=False)
        return abs(df.rdd.map(lambda x: (1, x.amount)).reduceByKey(lambda x, y: x + y).collect()[0][1] / 100.)

    @staticmethod
    def _filter_df(df, data_filter):
        return df.filter(data_filter)

    @staticmethod
    def _combine_filters(filters):
        return functools.reduce(lambda a, b: a | b, filters)

    def _prepare_filters(self, filters):
        return self._combine_filters([self.df.info.contains(word) for word in filters])

    def _prepare_time_filter(self, date: datetime):
        start = date.timestamp()
        stop = (date + relativedelta(months=+1) - timedelta(days=1)).timestamp()
        return self.df.data.between(start, stop)

    def _load_csv(self, csv_path):
        self.df = self.spark.read.csv(csv_path, sep=";", schema=DF_SCHEMA)


class CsvAnalyser:
    def __init__(self):
        self.csv = CsvReader(CONFIG["data"]["csv"])
        self.sheet = GoogleSheetApi()

    def analyse(self, year):
        filters = self.sheet.get_filters()
        to_ignore = self.sheet.get_ignores().values()
        for month in range(1, 13):
            date = datetime(year=year, month=month, day=1)
            data = self.csv.analyse_month(filters, date, ignore=to_ignore)
            print(data)
            # self.sheet.update_sheet([str(month)] + [*data.values()], month)

    def update_sheet(self):
        self.sheet.update_sheet(None, None)


if __name__ == '__main__':
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    start_time = time.time()
    csv_analyser = CsvAnalyser()
    csv_analyser.analyse(2020)
    print(f"time elapsed: {(time.time() - start_time):.2f}s")
    # 1.v ~40s
