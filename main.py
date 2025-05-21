from utils.extract import scrape_web
from utils.transform import (
    transform_to_DataFrame, deleteUnknownProduct, deletePriceUnavailable,
    transformData, deleteDuplicate, convertPriceToRupiah, addScrapeTimestamp
)
from utils.load import export_to_csv, upload_df_to_gsheet, load_df_to_postgresql

def main():
    try:
        # Konfigurasi
        SPREADSHEET_ID = '1ey13qZUTxmIlBt82PKOZgRXMENc0LUy46z7cFnnWVgE'
        service_file = "C:/Users/ASUS/Downloads/DATA_PIPELINE/google-sheet-API.json"
        url = 'https://fashion-studio.dicoding.dev/page{}.html'
        csv_filename = 'fashion_studio.csv'

        print("🔍 Memulai proses scraping...")
        scrap_data = scrape_web(base_url=url)

        print("📄 Mengubah hasil scrape ke DataFrame...")
        df = transform_to_DataFrame(scrap_data)

        print("🧹 Membersihkan data...")
        deleteUnknownProduct(df)
        deletePriceUnavailable(df)
        transformData(df)
        deleteDuplicate(df)
        convertPriceToRupiah(df)
        addScrapeTimestamp(df)

        print("💾 Mengekspor data ke CSV...")
        export_to_csv(df, csv_filename)

        print("📤 Mengunggah ke Google Sheets...")
        upload_df_to_gsheet(df, service_file, SPREADSHEET_ID)

        print("🗃️ Menyimpan ke PostgreSQL...")
        load_df_to_postgresql(
            df=df,
            db_name='shopscrap',
            user='postgres',
            password='se7kalo2',
            host='localhost',
            port=5432,
            table_name='produk_fashion',
            if_exists='append'
        )

        print("✅ Semua proses selesai tanpa error.")

    except Exception as e:
        print(f"❌ Terjadi kesalahan dalam main(): {e}")


if __name__ == "__main__":
    main()
