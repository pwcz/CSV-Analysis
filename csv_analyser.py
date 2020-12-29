import os
import pickle
import configparser
import os.path
import tempfile
import functools
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

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
        self.file_name = None
        self._prepare_csv(cvs_path)

    def __del__(self):
        if self.file_name:
            os.remove(self.file_name)

    def print_data_frame(self):
        self.df.printSchema()

    def show(self):
        self.df.show()

    def analyse_month(self, filters, date, ignore=None):
        df = self._filter_df(self.df, self._prepare_time_filter(date))
        if ignore:
            df = self._filter_df(df, ~self._prepare_filters(list(*ignore)))
        filters = {key: self._prepare_filters(value) for key, value in filters.items()}
        summary = {key: self._sum_amount(key, self._filter_df(df, value)) for key, value in filters.items()}
        not_filtered = self._filter_df(df, ~self._combine_filters(filters.values()))
        not_filtered.show(not_filtered.count(), truncate=False)
        summary["Sum"] = sum(summary.values())
        return summary

    @staticmethod
    def _sum_amount(key, df):
        if df.count() == 0:
            return 0
        print(key)
        df.show(df.count(), truncate=False)
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

    def _prepare_csv(self, csv_path):
        with open(csv_path, "r") as file, tempfile.NamedTemporaryFile(delete=False) as fp:
            data = [self._convert_line(line) for line in file.readlines()]
            fp.writelines(data[1:])  # skip first header row
            fp.close()
            self.file_name = fp.name
            self.df = self.spark.read.csv(fp.name, sep=";", schema=DF_SCHEMA)

    @staticmethod
    def _convert_line(line):
        data = line.split(";")
        date = str(int(datetime.strptime(data[1], "%d-%m-%Y").timestamp()))
        try:
            amount = str(int(float(data[5].replace(",", ".")) * 100.))
        except ValueError:
            print(data[5])
            amount = "0"
        replay = ";".join([date, (data[2] + " " + data[3]).lower(), amount]) + "\n"
        return bytearray(replay, encoding='utf8')


class GoogleSheetApi:
    def __init__(self) -> None:
        self.service = None
        self.sheet = None
        self._connect()

    def _connect(self) -> None:
        credentials = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(TOKEN_PICKLE):
            with open(TOKEN_PICKLE, 'rb') as token:
                credentials = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_JSON, SCOPES)
                credentials = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(TOKEN_PICKLE, 'wb') as token:
                pickle.dump(credentials, token)

        self.service = build('sheets', SHEETS_API_VERSION, credentials=credentials)
        self.sheet = self.service.spreadsheets()

    def _get(self, sheet_id: str, sheet_range: str) -> dict:
        data = self.sheet.values().get(spreadsheetId=sheet_id, range=sheet_range).execute().get('values', [])
        return {key: value.lower().split(";") for key, value in data}

    def get_filters(self) -> dict:
        return self._get(SAMPLE_SPREADSHEET_ID, FILTERS_RANGE)

    def get_ignores(self) -> dict:
        return self._get(SAMPLE_SPREADSHEET_ID, IGNORE_FILTER)

    def update_sheet(self, summary_data, month):
        values = [
            summary_data,
        ]
        body = {
            'values': values
        }
        self.service.spreadsheets().values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                                    range=self._get_sheet_range_by_month(month),
                                                    valueInputOption="RAW", body=body).execute()

    @staticmethod
    def _get_sheet_range_by_month(month):
        row = month + 1
        return f"Expenses!A{row}:J{row}"


class CsvAnalyser:
    def __init__(self, csv_path):
        self.csv = CsvReader(csv_path)
        self.sheet = GoogleSheetApi()

    def analyse(self, year):
        filters = self.sheet.get_filters()
        to_ignore = self.sheet.get_ignores().values()
        for month in range(1, 13):
            date = datetime(year=year, month=month, day=1)
            data = self.csv.analyse_month(filters, date, ignore=to_ignore)
            self.sheet.update_sheet([str(month)] + [*data.values()], month)

    def update_sheet(self):
        self.sheet.update_sheet(None, None)


if __name__ == '__main__':
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    csv_analyser = CsvAnalyser("2020.csv")
    csv_analyser.analyse(2020)
