#!venv/bin/python
import csv
import sys
import configparser
from datetime import datetime
from src.utils.google_sheet_api import GoogleSheetApi
from src.utils.transactions_db import get_transactions

CONFIG = configparser.ConfigParser()
CONFIG.read("config.ini")


class PrepareData:
    def __init__(self, year, month):
        self.year = year
        self.month = month

    @staticmethod
    def format_line(transaction):
        description = transaction["description"].lower()
        amount = transaction["amount"]
        date = int(transaction["date"].timestamp())
        return f"{date};{description};{amount}\n"

    def prepare_csv(self):
        with open(CONFIG["data"]["csv"], "w+") as file:
            transactions = get_transactions(self.year, self.month)
            file.writelines([PrepareData.format_line(line) for line in transactions])

    @staticmethod
    def save_csv(data, file_name):
        with open(file_name, "w+") as file:
            csv_writer = csv.writer(file, delimiter=";")
            csv_writer.writerows(data)

    def prepare_data(self):
        self.prepare_csv()
        sheet = GoogleSheetApi()
        self.save_csv(sheet.get_ignores(), CONFIG["data"]["ignores"])
        self.save_csv(sheet.get_filters(), CONFIG["data"]["filters"])


if __name__ == "__main__":
    try:
        PrepareData(sys.argv[1]).prepare_data()
    except IndexError:
        raise SystemExit(f"Usage: {sys.argv[0]} <file name>") from None
