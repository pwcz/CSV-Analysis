import sys
from src.utils.prepare_data import PrepareData
from src.run_job import run_job
from src.utils.google_sheet_api import GoogleSheetApi


def execute_pipeline():
    month = input("provide month number if you want to update sheet: ")
    if month == "\n":
        print("done.")
    else:
        try:
            month = int(month)
        except ValueError:
            print("invalid value")
            sys.exit(-1)

    PrepareData(2022, month).prepare_data()
    run_job()

    if "y" == input("upload result to google sheet? (y/n)"):
        sheet = GoogleSheetApi()
        sheet.upload_result(month)


if __name__ == "__main__":
    execute_pipeline()
