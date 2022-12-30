import os
from datetime import datetime

from dateutil.relativedelta import relativedelta
import pymongo

mongo_address = os.getenv("MONGO_ADDRESS") or "localhost"

client = pymongo.MongoClient(mongo_address, 27017)
transactions_db = client['transactions']

transactions_db.post.create_index([('description', pymongo.ASCENDING),
                                   ('date', pymongo.ASCENDING),
                                   ('amount', pymongo.ASCENDING)], unique=True)


def insert_transactions(transactions_list: list):
    duplicates, inserted = 0, 0
    for transaction in transactions_list:
        try:
            transactions_db.post.insert_one(transaction)
            inserted += 1
        except pymongo.errors.DuplicateKeyError:
            duplicates += 1

    return {"duplicates": duplicates, "inserted": inserted}


def get_transactions(year: int, month: int):
    start_date = datetime(year=year, month=month, day=1)
    stop_date = start_date + relativedelta(months=1)
    return transactions_db.post.find({"date": {"$gte": start_date, "$lt": stop_date}})


def get_transactions_with_filter(year: int, month: int, filter_regex: str):
    start_date = datetime(year=year, month=month, day=1)
    stop_date = start_date + relativedelta(months=1)
    return transactions_db.post.find({"date": {"$gte": start_date, "$lt": stop_date}, "description": {
        "$regex": filter_regex, "$options": "i"}})


def cleanup_database():
    my_query = {"description": {"$regex": "transakcja nierozliczona", "$options": "i"}}
    transactions_db.post.delete_many(my_query)


if __name__ == "__main__":
    cleanup_database()
