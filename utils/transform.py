import pandas as pd
from datetime import datetime

def transform_to_DataFrame(data):
    """Mengubah data menjadi DataFrame dengan error handling."""
    try:
        if not data:
            raise ValueError("Data kosong. Tidak bisa diubah menjadi DataFrame.")

        df = pd.DataFrame(data)

        if df.empty:
            raise ValueError("DataFrame yang dihasilkan kosong.")

        return df

    except (ValueError, TypeError) as e:
        print(f"❌ Terjadi kesalahan saat mengubah data ke DataFrame: {e}")
        return pd.DataFrame()  # Mengembalikan DataFrame kosong sebagai fallback


def deleteUnknownProduct(df):
    """
    Menghapus baris yang memiliki nilai 'Unknown Product' di kolom 'title',
    tanpa mempedulikan huruf kapital/spasi. Dilengkapi dengan error handling.
    """
    try:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input bukan DataFrame.")

        if 'title' not in df.columns:
            raise KeyError("Kolom 'title' tidak ditemukan dalam DataFrame.")

        # Hapus baris yang memiliki nilai 'unknown product' di kolom 'title' (case-insensitive, strip whitespace)
        original_len = len(df)
        df.drop(df[df['title'].str.strip().str.lower() == 'unknown product'].index, inplace=True)
        removed = original_len - len(df)
        print(f"✅ {removed} baris dengan title 'Unknown Product' berhasil dihapus.")

    except TypeError as te:
        print(f"❌ Tipe input tidak valid: {te}")
    except KeyError as ke:
        print(f"❌ Kolom tidak ditemukan: {ke}")
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat menghapus baris: {e}")


def deletePriceUnavailable(df):
    """
    Menghapus baris yang memiliki nilai 'Price Unavailable' di kolom 'price',
    tanpa mempedulikan huruf kapital/spasi. Dilengkapi dengan error handling.
    """
    try:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input bukan DataFrame.")

        if 'price' not in df.columns:
            raise KeyError("Kolom 'price' tidak ditemukan dalam DataFrame.")

        # Pastikan kolom price bertipe string sementara untuk pengecekan
        df['price'] = df['price'].astype(str)

        original_len = len(df)
        df.drop(df[df['price'].str.strip().str.lower() == 'price unavailable'].index, inplace=True)
        removed = original_len - len(df)
        print(f"✅ {removed} baris dengan 'Price Unavailable' berhasil dihapus.")

    except TypeError as te:
        print(f"❌ Tipe input tidak valid: {te}")
    except KeyError as ke:
        print(f"❌ Kolom tidak ditemukan: {ke}")
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat menghapus baris: {e}")

def transformData(df):
    """
    Membersihkan dan mentransformasi kolom-kolom dalam DataFrame:
    - 'title': ubah ke string
    - 'price': hapus simbol $, ubah ke float
    - 'rating': ambil angka dari teks, ubah ke float
    - 'colors': ambil angka, ubah ke integer
    - 'size': hapus label 'Size:', ubah ke string
    - 'gender': hapus label 'Gender:', ubah ke kategori

    Dilengkapi dengan error handling.
    """
    try:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input bukan DataFrame.")

        required_columns = ['title', 'price', 'rating', 'colors', 'size', 'gender']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise KeyError(f"Kolom yang hilang: {missing_columns}")

        # title
        df['title'] = df['title'].astype('string')

        # price
        df['price'] = df['price'].replace('[\$,]', '', regex=True)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')

        # rating
        df['rating'] = df['rating'].astype(str).str.extract(r'(\d+\.?\d*)')[0]
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')

        # colors
        df['colors'] = df['colors'].astype(str).str.extract(r'(\d+)')[0]
        df['colors'] = pd.to_numeric(df['colors'], errors='coerce')

        # size
        df['size'] = df['size'].astype(str).str.replace('Size:', '', regex=False).str.strip()
        df['size'] = df['size'].astype('string')

        # gender
        df['gender'] = df['gender'].astype(str).str.replace('Gender:', '', regex=False).str.strip()
        df['gender'] = df['gender'].astype('category')

        print("✅ Transformasi data berhasil dilakukan.")

    except TypeError as te:
        print(f"❌ Tipe input tidak valid: {te}")
    except KeyError as ke:
        print(f"❌ Kolom tidak ditemukan: {ke}")
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat transformasi data: {e}")

def deleteDuplicate(df):
    """
    Menghapus baris duplikat berdasarkan kolom 'title', menyimpan hanya baris pertama.
    Dilengkapi dengan error handling.
    """
    try:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input bukan DataFrame.")

        if 'title' not in df.columns:
            raise KeyError("Kolom 'title' tidak ditemukan dalam DataFrame.")

        original_len = len(df)
        df.drop_duplicates(subset=['title'], keep='first', inplace=True)
        removed = original_len - len(df)
        print(f"✅ {removed} duplikat berdasarkan 'title' berhasil dihapus.")

    except TypeError as te:
        print(f"❌ Tipe input tidak valid: {te}")
    except KeyError as ke:
        print(f"❌ Kolom tidak ditemukan: {ke}")
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat menghapus duplikat: {e}")

def convertPriceToRupiah(df, exchangeRate=16000):
    """
    Mengonversi harga dari dolar ke rupiah berdasarkan nilai tukar yang diberikan.
    - Menghapus simbol $ dari kolom 'price'.
    - Mengubah ke float, lalu dikalikan exchangeRate.
    
    Parameters:
        df (pd.DataFrame): DataFrame yang mengandung kolom 'price'.
        exchangeRate (float): Nilai tukar USD ke IDR. Default 16000.

    Returns:
        None – proses dilakukan langsung pada df (inplace).
    """
    try:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input bukan DataFrame.")

        if 'price' not in df.columns:
            raise KeyError("Kolom 'price' tidak ditemukan dalam DataFrame.")

        if not isinstance(exchangeRate, (int, float)):
            raise TypeError("exchangeRate harus berupa angka.")

        # Bersihkan simbol $ dan konversi ke numerik
        df['price'] = df['price'].replace('[\$,]', '', regex=True)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')

        # Konversi ke rupiah
        df['price'] = df['price'] * exchangeRate
        print(f"✅ Konversi harga ke Rupiah berhasil dengan kurs {exchangeRate}.")

    except TypeError as te:
        print(f"❌ Tipe data tidak valid: {te}")
    except KeyError as ke:
        print(f"❌ Kolom tidak ditemukan: {ke}")
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat mengonversi harga: {e}")


def addScrapeTimestamp(df):
    """
    Menambahkan kolom 'scrape_timestamp' berisi waktu saat fungsi dijalankan.
    Format waktu dalam datetime64[ns]. Dilengkapi dengan error handling.
    """
    try:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Input bukan DataFrame.")

        timestamp = pd.to_datetime(datetime.now())
        df['scrape_timestamp'] = timestamp
        df['scrape_timestamp'] = df['scrape_timestamp'].astype('datetime64[ns]')
        
        print("✅ Kolom 'scrape_timestamp' berhasil ditambahkan.")

    except TypeError as te:
        print(f"❌ Tipe input tidak valid: {te}")
    except Exception as e:
        print(f"❌ Terjadi kesalahan saat menambahkan timestamp: {e}")
