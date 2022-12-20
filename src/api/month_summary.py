from fastapi import APIRouter
from pydantic import BaseModel
from typing import List

from src.utils.google_sheet_api import GoogleSheetApi
from src.utils.transactions_db import get_transactions, get_transactions_with_filter

router = APIRouter()


class TransactionsSummary(BaseModel):
    summary: List[float]


def format_transaction(transaction):
    return {
        "date": transaction["date"].strftime("%m/%d/%Y"),
        "amount": transaction["amount"]/100,
        "description": transaction["description"],
    }


@router.get("/summary/{year}/{month}")
async def post_upload_file(year: int, month: int):
    all_transactions = list(get_transactions(year, month))
    filters = {k: l for k, l in GoogleSheetApi().get_filters()}

    result = {}
    all_filtered = []
    for filter_name, filter_regex in filters.items():
        filtered_data = list(get_transactions_with_filter(year, month, filter_regex))
        all_filtered += [str(transaction["_id"]) for transaction in filtered_data]
        result[filter_name] = {
            "transactions": [format_transaction(transaction) for transaction in filtered_data],
            "total_amount": -sum([transaction["amount"] for transaction in filtered_data])/100,
        }

    all_transactions_set = set([str(transaction["_id"]) for transaction in all_transactions])
    filtered_set = set(all_filtered)
    not_matched = all_transactions_set.difference(filtered_set)

    result["not_matched"] = [
        format_transaction(transaction) for transaction in all_transactions if str(transaction["_id"]) in not_matched
    ]

    return result


@router.post("/summary/{year}/{month}")
async def handle_post_summary_data(year: int, month: int, item: TransactionsSummary):
    data = item.summary
    data.append(sum(data))
    try:
        GoogleSheetApi().update_sheet(year, month, item.summary)
        return {"status": "ok"}
    except KeyError:
        return {"status": "error", "details": "key error"}
