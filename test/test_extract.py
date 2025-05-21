import unittest
from bs4 import BeautifulSoup
from utils.extract import fetching_content, extractWebElement, scrape_web


class TestScrapingFunctions(unittest.TestCase):

    def test_fetching_content_valid_url(self):
        url = 'https://fashion-studio.dicoding.dev/'
        content = fetching_content(url)
        self.assertIsNotNone(content)
        self.assertIn(b"<html", content)  # minimal konten html ada

    def test_extractWebElement_valid_element(self):
        # Contoh dummy HTML element
        html = '''
            <article class="collection-card">
            <h3 class="product-title">Test Product</h3>
            <span class="price">$99</span>      
            <p>4.5/5 Rating</p>
            <p>2 colors</p>
            <p>Size: M</p>
            <p>Gender: Men</p>
            </article>

        '''
        soup = BeautifulSoup(html, 'html.parser')
        element = soup.find('article')
        result = extractWebElement(element)

        expected_keys = {'title', 'price', 'rating', 'colors', 'size', 'gender'}
        self.assertTrue(set(result.keys()) == expected_keys)
        self.assertEqual(result['title'], 'Test Product')
        self.assertEqual(result['price'], '$99')
        self.assertEqual(result['rating'], '4.5/5 Rating')
        self.assertEqual(result['colors'], '2 colors')

    def test_scrape_web_basic(self):
        # Ambil hanya 1 halaman untuk test kecepatan
        data = scrape_web('https://fashion-studio.dicoding.dev/page{}.html', start_page=1, delay=0)
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
        self.assertTrue('title' in data[0])  # minimal kolom 'title' ada

if __name__ == '__main__':
    unittest.main()
