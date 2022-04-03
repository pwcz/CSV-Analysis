import sys
from collections import defaultdict
import csv
from typing import Optional


stocks = defaultdict(dict)
stock_transaction = {}
cash_flow = {"PLN": {"value": 0},
             "EUR": {"value": 0, "er": 0},
             "USD": {"value": 0, "er": 0}
             }


def get_amount(value: str) -> Optional[int]:
    return int(float(value.replace(",", ".")) * 100.) if value else None


def get_exchange(value: str) -> Optional[float]:
    return float(value.replace(",", ".")) if value else None


def update_cash(currency: str, amount: int, exchange_rate: float) -> None:
    current_value = cash_flow[currency]["value"]
    cash_flow[currency]["er"] = (current_value*cash_flow[currency]["er"]+exchange_rate*amount)/(current_value+amount)
    cash_flow[currency]["value"] += amount


def calculate_account_balance(file: str) -> None:
    fx_transaction = {}

    with open(file, "r") as file:
        data = csv.reader(file, delimiter=",")
        next(data)
        for line in list(data)[::-1]:
            stock_name = line[3]
            stock_isin = line[4]
            description = line[5]
            er = get_exchange(line[6])
            amount = get_amount(line[8])
            currency = line[9]
            account_balance = get_amount(line[10])
            transaction_uuid = line[11]
            print(f"{description.strip()[:99]: <100} | {amount or '': >7} {currency} ",
                  transaction_uuid if transaction_uuid else "")
            if transaction_uuid and stock_isin not in stocks:
                stocks[stock_isin]["name"] = stock_name
                stocks[stock_isin]["price"] = 0
                stocks[stock_isin]["amount"] = 0

            if "Depozyt" in description:
                cash_flow[currency]["value"] += amount
            elif "FX Withdrawal" in description:
                fx_transaction["FX Withdrawal"] = (currency, amount)
                if er:
                    fx_transaction["er"] = er
            elif "FX Credit" in description:
                fx_transaction["FX Credit"] = (currency, amount)
                if er:
                    fx_transaction["er"] = er

            if "FX Withdrawal" in fx_transaction and "FX Credit" in fx_transaction:
                w_currency, w_amount = fx_transaction["FX Withdrawal"]
                c_currency, c_amount = fx_transaction["FX Credit"]
                cash_flow[w_currency]["value"] += w_amount
                if w_currency == "PLN":
                    update_cash(c_currency, c_amount, fx_transaction["er"])
                else:
                    exchange_r = abs(cash_flow[w_currency]["er"]*w_amount/c_amount)
                    update_cash(c_currency, c_amount, exchange_r)
                fx_transaction = {}

            if transaction_uuid:
                if description.startswith("DEGIRO "):
                    stocks[stock_isin]["price"] += cash_flow[currency]["er"]*abs(amount)
                    cash_flow[currency]["value"] += amount
                if description.startswith("Kupno "):
                    stock_amount = int(description.split(" ")[1])
                    stocks[stock_isin]["amount"] += stock_amount
                    if account_balance > 0:
                        stocks[stock_isin]["price"] += cash_flow[currency]["er"] * abs(amount)
                if "FX Credit" in description:
                    stocks[stock_isin]["price"] += cash_flow[currency]["er"] * abs(amount)


def print_stocks():
    print("#"*152)
    for key, val in stocks.items():
        value = str(round(val['price']/100., 2)).replace(".", ",")
        print(f"{val['name']:<24} {val['amount']} at {value:>7}")


def print_cash_flow():
    print("#"*152)
    for key, val in cash_flow.items():
        if "er" in val:
            print(f"{val['value']/100.} {key} at {round(val['er'], 4)}")
        else:
            print(f"{val['value']/100.} {key}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <file name>")
        sys.exit(-1)
    else:
        calculate_account_balance(sys.argv[1])
        print_stocks()
        print_cash_flow()
