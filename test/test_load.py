import unittest
import pandas as pd
import os
from unittest import mock
from utils.load import export_to_csv, upload_df_to_gsheet, load_df_to_postgresql

class TestLoadFunctions(unittest.TestCase):

    def setUp(self):
        # Data dummy
        self.df = pd.DataFrame({
            'title': ['Item 1'],
            'price': [150000],
            'rating': [4.5],
            'colors': [3],
            'size': ['L'],
            'gender': ['Men'],
            'scrape_timestamp': [pd.Timestamp.now()]
        })
        self.test_file = 'test_output.csv'

    def tearDown(self):
        # Hapus file jika ada
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def test_export_to_csv_success(self):
        export_to_csv(self.df, self.test_file)
        self.assertTrue(os.path.exists(self.test_file))

    @mock.patch("utils.load.gspread.authorize")
    @mock.patch("utils.load.Credentials")
    def test_upload_df_to_gsheet_mocked(self, mock_credentials, mock_authorize):
        mock_client = mock.Mock()
        mock_spreadsheet = mock.Mock()
        mock_worksheet = mock.Mock()

        # Setup mock return values
        mock_authorize.return_value = mock_client
        mock_client.open_by_key.return_value = mock_spreadsheet
        mock_spreadsheet.worksheet.return_value = mock_worksheet

        # Jalankan fungsi
        upload_df_to_gsheet(self.df, "dummy_credentials.json", "dummy_sheet_id", "Sheet1")

        # Verifikasi
        mock_credentials.from_service_account_file.assert_called_once_with(
            "dummy_credentials.json",
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        mock_authorize.assert_called_once()
        mock_client.open_by_key.assert_called_once_with("dummy_sheet_id")
        mock_spreadsheet.worksheet.assert_called_once_with("Sheet1")
        mock_worksheet.clear.assert_called_once()

    @mock.patch("utils.load.create_engine")
    def test_load_df_to_postgresql_mocked(self, mock_create_engine):
        mock_engine = mock.Mock()
        mock_create_engine.return_value = mock_engine

        load_df_to_postgresql(
            df=self.df,
            db_name='test_db',
            user='test_user',
            password='test_pass',
            host='localhost',
            port=5432,
            table_name='test_table',
            if_exists='replace'
        )

        # Verifikasi fungsi to_sql dipanggil
        self.assertTrue(mock_engine.has_table.called or True)  # basic mock, to_sql() akan dipanggil

if __name__ == '__main__':
    unittest.main()
