#!venv/bin/python
import csv
import sys
import configparser
from datetime import datetime
from src.utils.google_sheet_api import GoogleSheetApi

CONFIG = configparser.ConfigParser()
CONFIG.read("config.ini")


class PrepareData:
    def __init__(self, data_file):
        self.file = data_file

    @staticmethod
    def format_line(line):
        data = line.split(";")
        date = int(datetime.strptime(data[1], "%d-%m-%Y").timestamp())
        try:
            amount = int(float(data[5].replace(",", ".")) * 100.)
        except ValueError:
            amount = 0
        return f"{date};{data[2].lower()} {data[3].lower()};{amount}\n"

    def prepare_csv(self):
        with open(self.file, "r") as csv_file, open(CONFIG["data"]["csv"], "w+") as file:
            file.writelines([PrepareData.format_line(line) for line in csv_file.readlines()])

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
