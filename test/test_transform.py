import unittest
import pandas as pd
from utils.transform import (
    transform_to_DataFrame, deleteUnknownProduct, deletePriceUnavailable,
    transformData, deleteDuplicate, convertPriceToRupiah, addScrapeTimestamp
)

class TestTransformFunctions(unittest.TestCase):

    def setUp(self):
        # Buat data dummy untuk test
        self.raw_data = [
            {
                'title': 'Pants 001',
                'price': '$12',
                'rating': '4.5/5',
                'colors': '3 colors',
                'size': 'Size: M',
                'gender': 'Gender: Men'
            },
            {
                'title': 'Unknown Product',
                'price': 'Price Unavailable',
                'rating': '3.0/5',
                'colors': '2 colors',
                'size': 'Size: L',
                'gender': 'Gender: Women'
            },
            {
                'title': 'Pants 001',  # Duplicate
                'price': '$2.00',
                'rating': '4.0/5',
                'colors': '1 colors',
                'size': 'Size: S',
                'gender': 'Gender: Unisex'
            }
        ]
        self.df = transform_to_DataFrame(self.raw_data)

    def test_delete_unknown_product(self):
        deleteUnknownProduct(self.df)
        self.assertFalse((self.df['title'].str.lower() == 'unknown product').any())

    def test_delete_price_unavailable(self):
        deletePriceUnavailable(self.df)
        self.assertFalse((self.df['price'].astype(str).str.lower() == 'price unavailable').any())

    def test_transform_data(self):
        transformData(self.df)
        self.assertTrue(pd.api.types.is_numeric_dtype(self.df['price']))
        self.assertTrue(pd.api.types.is_numeric_dtype(self.df['rating']))
        self.assertTrue(pd.api.types.is_numeric_dtype(self.df['colors']))
        self.assertTrue(pd.api.types.is_string_dtype(self.df['size']))
        self.assertTrue(pd.api.types.is_categorical_dtype(self.df['gender']))

    def test_delete_duplicates(self):
        initial_len = len(self.df)
        deleteDuplicate(self.df)
        self.assertLess(len(self.df), initial_len)

    def test_convert_price_to_rupiah(self):
        transformData(self.df)  # Pastikan kolom price sudah numerik
        convertPriceToRupiah(self.df, exchangeRate=15000)
        self.assertTrue((self.df['price'] >= 15000).any())

    def test_add_scrape_timestamp(self):
        addScrapeTimestamp(self.df)
        self.assertIn('scrape_timestamp', self.df.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(self.df['scrape_timestamp']))

if __name__ == '__main__':
    unittest.main()
