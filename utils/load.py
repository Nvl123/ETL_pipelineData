import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def export_to_csv(df, filename):
    """
    Mengekspor DataFrame ke file CSV dengan error handling.

    Parameters:
    - df (pd.DataFrame): DataFrame yang ingin diekspor.
    - filename (str): Nama file tujuan, termasuk .csv
    """
    try:
        if df.empty:
            raise ValueError("DataFrame kosong. Tidak dapat diekspor ke file CSV.")

        df.to_csv(filename, index=False)
        print(f"✅ Data berhasil diekspor ke '{filename}'.")

    except FileNotFoundError:
        print(f"❌ Lokasi file '{filename}' tidak ditemukan.")
    except PermissionError:
        print(f"❌ Tidak memiliki izin untuk menulis ke '{filename}'.")
    except ValueError as ve:
        print(f"❌ {ve}")
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat menulis ke file CSV: {e}")



def upload_df_to_gsheet(df, service_account_file, spreadsheet_id, sheet_name='Sheet1'):
    """
    Mengunggah DataFrame ke Google Sheets dengan error handling.

    Parameters:
    - df (pd.DataFrame): DataFrame yang ingin diunggah.
    - service_account_file (str): Nama file kredensial JSON.
    - spreadsheet_id (str): ID Google Spreadsheet (bukan URL lengkap).
    - sheet_name (str): Nama sheet/tab di dalam spreadsheet (default: 'Sheet1').
    """
    try:
        # Validasi DataFrame
        if df.empty:
            raise ValueError("DataFrame kosong. Tidak bisa diunggah.")

        # Scope Google Sheets
        scopes = ['https://www.googleapis.com/auth/spreadsheets']

        # Load kredensial
        credentials = Credentials.from_service_account_file(
            service_account_file, scopes=scopes
        )

        # Autentikasi dan buka spreadsheet
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        # Akses worksheet
        worksheet = spreadsheet.worksheet(sheet_name)

        # Hapus isi lama (opsional)
        worksheet.clear()

        # Upload DataFrame
        set_with_dataframe(worksheet, df)
        print(f"✅ DataFrame berhasil diunggah ke spreadsheet: {spreadsheet.title} → {sheet_name}")

    except FileNotFoundError:
        print(f"❌ File kredensial '{service_account_file}' tidak ditemukan.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"❌ Sheet '{sheet_name}' tidak ditemukan di spreadsheet.")
    except gspread.exceptions.APIError as api_error:
        print(f"❌ API error: {api_error}")
    except Exception as e:
        print(f"❌ Terjadi kesalahan umum: {e}")


def load_df_to_postgresql(df, db_name, user, password, host, port, table_name, if_exists='replace'):
    """
    Memuat DataFrame ke tabel PostgreSQL.

    Parameters:
    - df (pd.DataFrame): Data yang akan dimasukkan ke PostgreSQL.
    - db_name (str): Nama database PostgreSQL.
    - user (str): Username PostgreSQL.
    - password (str): Password PostgreSQL.
    - host (str): Host database (biasanya 'localhost' atau IP).
    - port (int or str): Port PostgreSQL (default 5432).
    - table_name (str): Nama tabel tujuan.
    - if_exists (str): 'replace', 'append', atau 'fail' (default: 'replace').

    Returns:
    - None
    """
    try:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input bukan DataFrame.")

        if df.empty:
            raise ValueError("DataFrame kosong, tidak bisa dimasukkan ke database.")

        # Buat connection string PostgreSQL
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        engine = create_engine(conn_str)

        # Masukkan data ke database
        df.to_sql(name=table_name, con=engine, if_exists=if_exists, index=False)
        print(f"✅ DataFrame berhasil dimuat ke tabel '{table_name}' di database '{db_name}'.")

    except SQLAlchemyError as db_err:
        print(f"❌ Kesalahan saat koneksi atau query ke database: {db_err}")
    except Exception as e:
        print(f"❌ Kesalahan umum: {e}")

