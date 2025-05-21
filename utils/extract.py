import requests
from bs4 import BeautifulSoup
import time

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    )
}

def fetching_content(url):
    """Mengambil konten HTML dari URL yang diberikan."""
    session = requests.Session()
    try:
        response = session.get(url, headers=HEADERS, timeout=10)  # tambahkan timeout agar tidak menggantung
        response.raise_for_status()  # akan raise jika status >= 400
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Terjadi kesalahan ketika melakukan requests terhadap {url}: {e}")
        return None

def extractWebElement(element):
    """
    Mengambil data produk berupa judul, harga, ketersediaan, dan atribut lainnya dari elemen HTML.

    Menggunakan error handling untuk menghindari AttributeError dan IndexError.
    """
    try:
        title = element.select_one('.product-title').text.strip() if element.select_one('.product-title') else 'N/A'
        price = element.select_one('.price').text.strip() if element.select_one('.price') else 'N/A'
        
        p_tags = element.find_all('p')
        rating = p_tags[0].text.strip() if len(p_tags) > 0 else 'N/A'
        colors = p_tags[1].text.strip() if len(p_tags) > 1 else 'N/A'
        size = p_tags[2].text.strip() if len(p_tags) > 2 else 'N/A'
        gender = p_tags[3].text.strip() if len(p_tags) > 3 else 'N/A'

        result = {
            'title': title,
            'price': price,
            'rating': rating,
            'colors': colors,
            'size': size,
            'gender': gender
        }

    except Exception as e:
        print(f"Terjadi kesalahan saat mengekstrak elemen: {e}")
        result = {
            'title': 'ERROR',
            'price': 'ERROR',
            'rating': 'ERROR',
            'colors': 'ERROR',
            'size': 'ERROR',
            'gender': 'ERROR'
        }

    return result



def scrape_web(base_url, start_page=1, delay=2):
    data = []
    page_number = start_page

    while True:
        try:
            if page_number == 1:
                url = 'https://fashion-studio.dicoding.dev/'  # Halaman awal
            else:
                url = base_url.format(page_number)

            print(f"Scraping halaman: {url}")
            content = fetching_content(url)

            if not content:
                print(f"Gagal mengambil konten halaman {url}. Menghentikan scraping.")
                break

            soup = BeautifulSoup(content, "html.parser")

            # Cek jika halaman error berdasarkan konten teks
            if "Page Not Found" in soup.text or "page not found" in soup.text:
                print(f"Halaman error ditemukan di {url}. Menghentikan scraping.")
                break

            web_elements = soup.find_all(class_='collection-card')
            print(f"Jumlah elemen ditemukan: {len(web_elements)}")

            for element in web_elements:
                try:
                    shop = extractWebElement(element)
                    data.append(shop)
                except Exception as e:
                    print(f"❌ Gagal mengekstrak data dari satu elemen: {e}")
                    continue  # lanjut ke elemen berikutnya

            # Cek tombol next
            next_button = soup.find('li', class_='page-item next')
            if not next_button or 'disabled' in next_button.get('class', []):
                print("✅ Tombol next disabled atau tidak ditemukan. Menghentikan scraping.")
                break
            else:
                page_number += 1
                time.sleep(delay)

        except Exception as e:
            print(f"❌ Terjadi kesalahan tak terduga saat scraping halaman {page_number}: {e}")
            break

    return data

