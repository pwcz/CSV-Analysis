import sys
from src.utils.prepare_data import PrepareData
from src.run_job import run_job
from src.utils.google_sheet_api import GoogleSheetApi


def execute_pipeline(file_name: str):
    PrepareData(file_name).prepare_data()
    run_job()
    month = input("provide month number if you want to update sheet: ")
    if month == "\n":
        print("done.")
    else:
        try:
            month = int(month)
        except ValueError:
            print("invalid value")
            sys.exit(-1)

        sheet = GoogleSheetApi()
        sheet.upload_result(month)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <file name>")
        sys.exit(-1)
    else:
        execute_pipeline(sys.argv[1])
