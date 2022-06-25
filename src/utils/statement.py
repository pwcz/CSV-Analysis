import csv
from datetime import datetime

from utils.transactions_db import insert_transactions


def is_mbank_csv(filename: str):
    with open(filename, "r", encoding="ISO-8859-2") as file:
        csv_reader = csv.reader(file, delimiter=';')
        first_row = next(csv_reader)
        return first_row[0].startswith("mBank")


def is_santander_csv(filename: str):
    with open(filename, "r") as file:
        try:
            csv_reader = csv.reader(file, delimiter=";")
            first_row = next(csv_reader)
            return "05 1090" in first_row[2]
        except (UnicodeDecodeError, IndexError):
            return False


def get_unified_amount(value: str) -> int:
    return int(float(value.replace("PLN", "").replace(",", ".").replace(" ", "")) * 100)


def process_mbank(filename: str):
    with open(filename, "r", encoding="ISO-8859-2") as file:
        csv_reader = csv.reader(file, delimiter=';')
        for line in csv_reader:
            if len(line) > 0 and "#Data operacji" in line[0]:
                data = list(csv.DictReader(file, [ll.replace("#", "") for ll in line if ll], delimiter=";"))

    return [{"date": datetime.strptime(operation["Data operacji"], "%Y-%m-%d"),
             "description": operation["Opis operacji"],
             "amount": get_unified_amount(operation["Kwota"])
             } for operation in data]


def process_santander(filename: str):
    with open(filename, "r") as file:
        csv_reader = csv.reader(file, delimiter=';')
        next(csv_reader)
        return [{"date": datetime.strptime(operation[1], "%d-%m-%Y"),
                 "description": f"{operation[2]} {operation[3]}",
                 "amount": get_unified_amount(operation[5])
                 } for operation in csv_reader]


def process_statement(filename: str):
    if is_santander_csv(filename):
        transaction_data = process_santander(filename)
        t_type = "santander"
    elif is_mbank_csv(filename):
        transaction_data = process_mbank(filename)
        t_type = "mbank"
    else:
        return {"status": "error", "info": "file not recognised"}

    response = insert_transactions(transaction_data)
    response.update({"status": "ok", "type": t_type})

    return response
