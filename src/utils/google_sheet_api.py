import csv
import os
import os.path
import pickle
import configparser
from googleapiclient.discovery import build, Resource
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CONFIG = configparser.ConfigParser()
CONFIG.read("config/config.ini")

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = CONFIG["sheet"]["id"]
FILTERS_RANGE = CONFIG["sheet"]["filters"]
IGNORE_FILTER = CONFIG["sheet"]["ignore"]
TOKEN_PICKLE = "config/token.pickle"
CREDENTIALS_JSON = "config/credentials.json"
SHEETS_API_VERSION = "v4"


class GoogleSheetApi:
    def __init__(self) -> None:
        self.service: Resource
        self.sheet: Resource
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

    def _get(self, sheet_id: str, sheet_range: str) -> tuple:
        data = self.sheet.values().get(spreadsheetId=sheet_id, range=sheet_range).execute().get('values', [])
        return tuple((key, "|".join(value.lower().split(";"))) for key, value in data)

    def get_filters(self) -> tuple:
        return self._get(SAMPLE_SPREADSHEET_ID, FILTERS_RANGE)

    def get_ignores(self) -> tuple:
        return self._get(SAMPLE_SPREADSHEET_ID, IGNORE_FILTER)

    def update_sheet(self, year, month, summary_data):
        values = [
            summary_data,
        ]
        body = {
            'values': values
        }
        print(body)
        self.service.spreadsheets().values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                                    range=self._get_sheet_range_by_month(year, month),
                                                    valueInputOption="RAW", body=body).execute()

    @staticmethod
    def _get_sheet_range_by_month(year, month):
        return f"Expenses {year}!A{month+1}:K{month+1}"
