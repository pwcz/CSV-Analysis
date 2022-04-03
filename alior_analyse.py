import sys
import csv
from collections import defaultdict


EXCHANGE_TRANSACTION_TITLE = "Rozliczenie transakcji Kantor Walutowy"
DESCRIPTION_TRANSACTION_TITLE = "Nazwa transakcji i opis"
EXCHANGE_CURRENCY_TRANSACTION_TITLE = 'Waluta'
AMOUNT_TRANSACTION_TITLE = 'Kwota'
CARD_TRANSACTION = "Transakcja kart"


account = {"PLN": 0,
           "USD": {"value": 0, "avg_er": 0},
           "EUR": {"value": 0, "avg_er": 0},
           "CHF": {"value": 0, "avg_er": 0}
           }

ex_transaction = defaultdict(dict)


def perform_exchange_transaction(tran_id: int):
    currency = ex_transaction[tran_id]["currency"]
    current_value_in_pln = account[currency]["avg_er"] * account[currency]["value"] - ex_transaction[tran_id]["amount"]
    foreign_currency_amount = account[ex_transaction[tran_id]["currency"]]["value"] + ex_transaction[tran_id]["value"]
    account[currency]["avg_er"] = current_value_in_pln / foreign_currency_amount
    account[currency]["value"] += ex_transaction[tran_id]["value"]
    account["PLN"] += ex_transaction[tran_id]["amount"]


def calculate_account_balance(file: str) -> dict:
    with open(file, "r",  encoding="ISO-8859-2") as file:
        data = csv.DictReader(file, delimiter=";")

        for line in list(data)[::-1]:
            desc = line[DESCRIPTION_TRANSACTION_TITLE]
            currency = line[EXCHANGE_CURRENCY_TRANSACTION_TITLE]
            amount = int(float(line[AMOUNT_TRANSACTION_TITLE])*100.)
            print(f"{desc.strip()[:149]:150}| {amount/100:>9} {currency}")

            if desc.startswith(EXCHANGE_TRANSACTION_TITLE):
                transaction_id = int(desc.lstrip(EXCHANGE_TRANSACTION_TITLE))
                if currency == "PLN":
                    ex_transaction[transaction_id]["amount"] = amount
                else:
                    ex_transaction[transaction_id]["currency"] = currency
                    ex_transaction[transaction_id]["value"] = amount

                if len(ex_transaction[transaction_id].keys()) == 3:
                    perform_exchange_transaction(transaction_id)

            elif desc.startswith(CARD_TRANSACTION) and currency != "PLN":
                account[currency]["value"] += amount
            else:
                account["PLN"] += amount
    return account


def print_account_balance(account_data: dict) -> None:
    print("#"*165, "\nAccount balance:")
    for key, value in account_data.items():
        if isinstance(value, dict):
            print(f"{value['value']/100} {key} average ex. rate {value['avg_er']}")
        else:
            print(f"{value/100} {key}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <file name>")
        sys.exit(-1)
    else:
        acc_data = calculate_account_balance(sys.argv[1])
        print_account_balance(acc_data)
