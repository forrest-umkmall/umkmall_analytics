"""
Google Sheets client for accessing and reading sheets data.
"""
import os
from typing import Optional, List, Dict, Any
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class GSheetsClient:
    """Client for accessing Google Sheets API."""

    # If modifying these scopes, delete the token file
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def __init__(self, credentials_path: Optional[str] = None):
        """
        Initialize Google Sheets client.

        Supports three methods of authentication (in order of priority):
        1. Pass credentials_path directly
        2. Use GOOGLE_APPLICATION_CREDENTIALS env var (path to JSON file)
        3. Use GOOGLE_CLIENT_EMAIL and GOOGLE_PRIVATE_KEY env vars (for deployed environments)

        Args:
            credentials_path: Path to service account credentials JSON file.
        """
        # Method 1: Direct path provided
        if credentials_path:
            self.credentials = Credentials.from_service_account_file(
                credentials_path,
                scopes=self.SCOPES
            )
        # Method 2: GOOGLE_APPLICATION_CREDENTIALS env var
        elif os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
            self.credentials = Credentials.from_service_account_file(
                os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
                scopes=self.SCOPES
            )
        # Method 3: Individual env vars (GOOGLE_CLIENT_EMAIL and GOOGLE_PRIVATE_KEY)
        elif os.getenv('GOOGLE_CLIENT_EMAIL') and os.getenv('GOOGLE_PRIVATE_KEY'):
            client_email = os.getenv('GOOGLE_CLIENT_EMAIL')
            private_key = os.getenv('GOOGLE_PRIVATE_KEY')

            # Handle escaped newlines in private key (common in env vars)
            private_key = private_key.replace('\\n', '\n')

            service_account_info = {
                "type": "service_account",
                "project_id": os.getenv('GOOGLE_PROJECT_ID', ''),
                "private_key_id": os.getenv('GOOGLE_PRIVATE_KEY_ID', ''),
                "private_key": private_key,
                "client_email": client_email,
                "client_id": os.getenv('GOOGLE_CLIENT_ID', ''),
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email}"
            }

            self.credentials = Credentials.from_service_account_info(
                service_account_info,
                scopes=self.SCOPES
            )
        else:
            raise ValueError(
                "No credentials provided. Please either:\n"
                "  1. Pass credentials_path parameter, OR\n"
                "  2. Set GOOGLE_APPLICATION_CREDENTIALS env var (path to JSON), OR\n"
                "  3. Set GOOGLE_CLIENT_EMAIL and GOOGLE_PRIVATE_KEY env vars"
            )

        self.service = build('sheets', 'v4', credentials=self.credentials)

    def read_sheet(
        self,
        spreadsheet_id: str,
        range_name: str,
        value_render_option: str = 'FORMATTED_VALUE'
    ) -> List[List[Any]]:
        """
        Read data from a Google Sheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet (from the URL)
            range_name: The A1 notation of the range to retrieve (e.g., 'Sheet1!A1:Z1000')
            value_render_option: How values should be represented in the output
                                ('FORMATTED_VALUE', 'UNFORMATTED_VALUE', 'FORMULA')

        Returns:
            List of lists representing rows and columns
        """
        try:
            sheet = self.service.spreadsheets()
            result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueRenderOption=value_render_option
            ).execute()

            values = result.get('values', [])
            return values

        except HttpError as error:
            print(f"An error occurred: {error}")
            raise

    def read_sheet_to_dataframe(
        self,
        spreadsheet_id: str,
        range_name: str,
        header_row: int = 0
    ) -> pd.DataFrame:
        """
        Read a Google Sheet directly into a pandas DataFrame.

        Args:
            spreadsheet_id: The ID of the spreadsheet
            range_name: The A1 notation of the range
            header_row: Which row to use as column headers (0-indexed)

        Returns:
            DataFrame with the sheet data
        """
        values = self.read_sheet(spreadsheet_id, range_name)

        if not values:
            return pd.DataFrame()

        # Extract headers and data
        headers = values[header_row]
        data = values[header_row + 1:]

        # Ensure all rows have the same length as headers
        max_cols = len(headers)
        normalized_data = []
        for row in data:
            # Pad rows that are shorter than headers
            padded_row = row + [''] * (max_cols - len(row))
            normalized_data.append(padded_row[:max_cols])

        df = pd.DataFrame(normalized_data, columns=headers)

        # Convert column names to lowercase and replace spaces with underscores
        df.columns = (df.columns
                      .str.lower()
                      .str.replace(' ', '_', regex=False)
                      .str.replace(r'[^a-z0-9_]', '_', regex=True)  # Replace special chars with underscore
                      .str.replace(r'_+', '_', regex=True)  # Replace multiple underscores with single
                      .str.strip('_'))  # Remove leading/trailing underscores

        return df

    def get_all_sheets_metadata(self, spreadsheet_id: str) -> List[Dict[str, Any]]:
        """
        Get metadata about all sheets in a spreadsheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet

        Returns:
            List of sheet metadata dictionaries
        """
        try:
            sheet_metadata = self.service.spreadsheets().get(
                spreadsheetId=spreadsheet_id
            ).execute()

            sheets = sheet_metadata.get('sheets', [])
            return [
                {
                    'title': sheet['properties']['title'],
                    'sheetId': sheet['properties']['sheetId'],
                    'index': sheet['properties']['index'],
                    'rowCount': sheet['properties']['gridProperties']['rowCount'],
                    'columnCount': sheet['properties']['gridProperties']['columnCount']
                }
                for sheet in sheets
            ]
        except HttpError as error:
            print(f"An error occurred: {error}")
            raise

    def read_all_sheets_to_dict(
        self,
        spreadsheet_id: str,
        header_row: int = 0,
        exclude_sheets: Optional[List[str]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Read all sheets from a spreadsheet into a dictionary of DataFrames.

        Args:
            spreadsheet_id: The ID of the spreadsheet
            header_row: Which row to use as column headers (0-indexed)
            exclude_sheets: List of sheet names to exclude from reading

        Returns:
            Dictionary mapping sheet names to DataFrames
        """
        sheets_metadata = self.get_all_sheets_metadata(spreadsheet_id)
        exclude_sheets = exclude_sheets or []

        result = {}
        for sheet_info in sheets_metadata:
            sheet_name = sheet_info['title']

            # Skip excluded sheets
            if sheet_name in exclude_sheets:
                print(f"Skipping excluded sheet: {sheet_name}")
                continue

            print(f"Reading sheet: {sheet_name}")

            # Read the entire sheet
            df = self.read_sheet_to_dataframe(
                spreadsheet_id,
                range_name=f"'{sheet_name}'",
                header_row=header_row
            )

            result[sheet_name] = df
            print(f"  Loaded {len(df)} rows, {len(df.columns)} columns")

        return result

    def clear_sheet(
        self,
        spreadsheet_id: str,
        range_name: str
    ) -> None:
        """
        Clear data from a Google Sheet range.

        Args:
            spreadsheet_id: The ID of the spreadsheet
            range_name: The A1 notation of the range to clear (e.g., 'Sheet1' or 'Sheet1!A1:Z1000')
        """
        try:
            self.service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                body={}
            ).execute()
            print(f"Cleared range: {range_name}")
        except HttpError as error:
            print(f"An error occurred: {error}")
            raise

    def write_dataframe(
        self,
        spreadsheet_id: str,
        range_name: str,
        df: pd.DataFrame,
        include_index: bool = False,
        clear_before_write: bool = True
    ) -> None:
        """
        Write a pandas DataFrame to a Google Sheet.

        Args:
            spreadsheet_id: The ID of the spreadsheet
            range_name: The A1 notation of the range to write to (e.g., 'Sheet1' or 'Sheet1!A1')
            df: DataFrame to write
            include_index: Whether to include the DataFrame index as a column
            clear_before_write: Whether to clear the sheet before writing new data
        """
        try:
            # Clear the sheet first if requested
            if clear_before_write:
                self.clear_sheet(spreadsheet_id, range_name)

            # Convert DataFrame to list of lists
            if include_index:
                # Reset index to make it a column
                df = df.reset_index()

            # Make a copy to avoid modifying the original
            df_clean = df.copy()

            # Convert datetime columns to strings to handle NaT values
            for col in df_clean.columns:
                if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
                    df_clean[col] = df_clean[col].astype(str).replace('NaT', '')

            # Replace NaN values with empty strings for JSON serialization
            df_clean = df_clean.fillna('')

            # Get column headers
            headers = df_clean.columns.tolist()

            # Get data rows
            data_rows = df_clean.values.tolist()

            # Combine headers and data
            values = [headers] + data_rows

            # Write to sheet
            body = {
                'values': values
            }

            result = self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',  # Use 'USER_ENTERED' to parse formulas
                body=body
            ).execute()

            print(f"✓ Wrote {len(df)} rows and {len(df.columns)} columns to {range_name}")
            print(f"  Updated {result.get('updatedCells')} cells")

        except HttpError as error:
            print(f"An error occurred: {error}")
            raise


# Convenience function for quick access
def read_gsheet(
    spreadsheet_id: str,
    range_name: str,
    credentials_path: Optional[str] = None
) -> pd.DataFrame:
    """
    Convenience function to quickly read a Google Sheet into a DataFrame.

    Args:
        spreadsheet_id: The ID of the spreadsheet
        range_name: The A1 notation of the range
        credentials_path: Path to credentials file

    Returns:
        DataFrame with the sheet data
    """
    client = GSheetsClient(credentials_path)
    return client.read_sheet_to_dataframe(spreadsheet_id, range_name)
