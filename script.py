import requests
import cloudscraper
import json
from bs4 import BeautifulSoup as bs
import pandas as pd
import re
import time
import random
from requests.exceptions import RequestException
from typing import List, Dict, Any
import logging
from fake_useragent import UserAgent
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)

class MudahScraper:
    def __init__(self, max_retries: int = 3, base_delay: int = 2):
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.ua = UserAgent()
        
        # Define all possible keys for car details
        self.keys = [
            "price",
            # "category_id",
            "location", "condition", "make", "model",
            "car_type", "transmission", "engine_capacity", "mileage",
            "manufactured_date", "ads_id", "family", "variant", "series",
            "style", "seat", "country_origin", "cc", "comp_ratio", "kw",
            "torque", "engine", "fuel_type", "length", "width", "height",
            "wheelbase", "kerbwt", "fueltk", "brake_front", "brake_rear",
            "suspension_front", "suspension_rear", "steering", "tyres_front",
            "tyres_rear", "wheel_rim_front", "wheel_rim_rear"
        ]

    def _get_random_headers(self) -> Dict[str, str]:
        """Generate random headers for requests."""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }

    def _make_request(self, url: str, retry_count: int = 0) -> requests.Response:
        """Make a request with retry logic and exponential backoff."""
        try:
            # Add random delay between requests
            jitter = random.uniform(0.5, 1.5)
            delay = (self.base_delay * (2 ** retry_count)) * jitter
            time.sleep(delay)
            
            response = self.scraper.get(url, headers=self._get_random_headers())
            response.raise_for_status()
            return response
            
        except RequestException as e:
            if retry_count < self.max_retries:
                logging.warning(f"Request failed for {url}. Retrying... ({retry_count + 1}/{self.max_retries})")
                return self._make_request(url, retry_count + 1)
            else:
                logging.error(f"Max retries reached for {url}. Error: {str(e)}")
                raise

    def get_car_urls(self, page_url: str, expected_urls: int = 40, max_retries: int = 3) -> List[str]:
        """
        Extract car URLs from a page using multiple methods with retry mechanism.
        
        Args:
            page_url: The URL of the page to scrape
            expected_urls: Expected number of URLs per page (default 40)
            max_retries: Maximum number of retry attempts (default 3)
        """
        retry_count = 0
        best_result = []
        
        while retry_count < max_retries:
            try:
                # Add exponential backoff delay on retries
                if retry_count > 0:
                    delay = self.base_delay * (2 ** (retry_count - 1)) + random.uniform(1, 3)
                    logging.info(f"Retry #{retry_count} for {page_url}. Waiting {delay:.2f} seconds...")
                    time.sleep(delay)
                
                response = self._make_request(page_url)
                soup = bs(response.text, 'html.parser')
                car_urls = set()  # Using set to avoid duplicates
                
                # Method 1: Extract from LD+JSON data
                script = soup.find('script', type='application/ld+json')
                if script is not None:
                    try:
                        data = json.loads(script.text)
                        item_list_element = data[2].get('itemListElement', [])
                        car_urls.update(item['item']['url'] for item in item_list_element)
                    except (json.JSONDecodeError, IndexError, KeyError) as e:
                        logging.warning(f"Error parsing LD+JSON data: {str(e)}")

                # Method 2: Extract from regular HTML elements
                listing_elements = soup.select('div[class*="listing-card"]') or \
                                 soup.select('div[class*="listing-item"]') or \
                                 soup.select('div[class*="product-card"]')
                
                for element in listing_elements:
                    link = element.find('a', href=re.compile(r'/cars/.+\.html?'))
                    if not link:
                        link = element.find('a', href=re.compile(r'.+\.html?'))
                    
                    if link and 'href' in link.attrs:
                        url = link['href']
                        if not url.startswith('http'):
                            url = f"https://www.mudah.my{url}"
                        car_urls.add(url)
                
                # Method 3: Search for next data script
                next_data = soup.find('script', id='__NEXT_DATA__')
                if next_data:
                    try:
                        data = json.loads(next_data.string)
                        listings = data.get('props', {}).get('pageProps', {}).get('items', [])
                        for listing in listings:
                            if 'url' in listing:
                                car_urls.add(listing['url'])
                    except (json.JSONDecodeError, KeyError) as e:
                        logging.warning(f"Error parsing __NEXT_DATA__: {str(e)}")

                result_urls = list(car_urls)
                
                # Keep track of the best result so far
                if len(result_urls) > len(best_result):
                    best_result = result_urls
                
                # If we got the expected number of URLs, return immediately
                if len(result_urls) >= expected_urls:
                    logging.info(f"Successfully collected {len(result_urls)} URLs from {page_url}")
                    return result_urls
                
                # Log warning and continue to retry
                logging.warning(f"Attempt {retry_count + 1}/{max_retries}: Found {len(result_urls)} URLs on {page_url} (expected {expected_urls})")
                retry_count += 1
                
            except Exception as e:
                logging.error(f"Error on attempt {retry_count + 1} for {page_url}: {str(e)}")
                retry_count += 1
        
        # After all retries, return the best result we got
        logging.warning(f"After {max_retries} attempts, best result was {len(best_result)} URLs for {page_url}")
        return best_result

    def parse_mcdparams(self, car_mcdparams: List[Dict]) -> List[Dict]:
        """Parse additional car parameters from mcdParams."""
        other_dets = []
        for group in car_mcdparams:
            for params in group.get('params', []):
                other_dets.append({
                    'realValue': params.get('realValue', ''),
                    'id': params.get('id', ''),
                    'value': params.get('value', ''),
                    'label': params.get('label', '')
                })
        return other_dets

    def get_page_urls(self, state: str = "", brand: str = "", start: int = 1, end: int = 1) -> List[str]:
        """Generate page URLs based on parameters."""
        base_url = "https://www.mudah.my"
        state = state.lower() if state else "malaysia"
        
        if brand.lower() == 'none' or not brand:
            url = f"{base_url}/{state}/cars-for-sale?o="
        else:
            url = f"{base_url}/{state}/cars-for-sale/{brand}?o="
        
        return [url + str(page) for page in range(start, end + 1)]

    def get_car_info(self, url: str) -> Dict[Any, Any]:
        """Extract car information from a single URL."""
        try:
            response = self._make_request(url)
            soup = bs(response.text, 'html.parser')
            
            script_tag = soup.find('script', id="__NEXT_DATA__") or soup.find('script', type='application/json')
            if not script_tag:
                logging.warning(f"No script tag found for URL: {url}")
                return None

            data = json.loads(script_tag.string)
            car_ads_no = re.search(r'-(\d+)\.htm', url).group(1)
            
            dict_id = [{
                'realValue': '',
                'id': 'ads_id',
                'value': car_ads_no,
                'label': 'id ads'
            }]
            
            props = data.get('props', {})
            ads_id = props.get('initialState', {}).get('adDetails', {}).get('byID', {}).get(car_ads_no, {})
            
            # Get both categoryParams and mcdParams
            car_attrs = ads_id.get('attributes', {}).get('categoryParams', [])
            car_mcdparams = ads_id.get('attributes', {}).get('mcdParams', [])
            
            # Combine all parameters
            mcd_params = self.parse_mcdparams(car_mcdparams)
            car_dets = car_attrs + dict_id + mcd_params
            
            return car_dets
            
        except Exception as e:
            logging.error(f"Error processing car info for {url}: {str(e)}")
            return None

    def scrape_cars(self, state: str, brand: str, start: int, end: int, expected_urls_per_page: int = 40) -> pd.DataFrame:
        """Main method to scrape car information sequentially with progress bars and retry logic."""
        logging.info(f"Starting scrape for {brand} cars in {state} from page {start} to {end}")
        
        # Get all page URLs
        page_urls = self.get_page_urls(state, brand, start, end)
        car_urls = []
        
        # Get all car URLs sequentially with progress bar
        with tqdm(total=len(page_urls), desc="Collecting pages", unit="page") as pbar:
            for url in page_urls:
                urls = self.get_car_urls(url, expected_urls=expected_urls_per_page)
                car_urls.extend(urls)
                pbar.update(1)
                pbar.set_postfix({
                    "Cars found": len(urls),
                    "Expected": expected_urls_per_page,
                    "Success": len(urls) >= expected_urls_per_page
                })
        
        logging.info(f"Found total of {len(car_urls)} car listings")
        
        # Process car information sequentially with progress bar
        car_data = []
        with tqdm(total=len(car_urls), desc="Processing cars", unit="car") as pbar:
            for url in car_urls:
                result = self.get_car_info(url)
                if result:
                    car_data.append(result)
                    pbar.set_postfix({"Success": True})
                else:
                    pbar.set_postfix({"Success": False})
                pbar.update(1)

        # Process the collected data into a DataFrame with progress bar
        processed_data = []
        with tqdm(total=len(car_data), desc="Processing data", unit="record") as pbar:
            for car_attrs in car_data:
                car_dict = {}
                for item in car_attrs:
                    if item['id'] in self.keys:
                        car_dict[item['id']] = item['value']
                processed_data.append(car_dict)
                pbar.update(1)

        return pd.DataFrame(processed_data)

def main():
    scraper = MudahScraper()
    
    print("\n=== Mudah.my Car Scraper ===\n")
    
    # Get user inputs
    state = input("Enter state name: ")
    brand = input("Car brand: ")
    start = int(input("Start page: "))
    end = int(input("Last page: "))
    
    print("\nStarting scraper...\n")
    
    try:
        # Start scraping
        df = scraper.scrape_cars(state, brand, start, end)
        
        # Save results with progress bar
        timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
        filename = f'D:/3. Data Analysis Project/Mudah Website/Mudah Car/mudah_cars_{brand}_{state}_{start}_to_{end}_{timestamp}.csv' 
        
        print("\nSaving results...")
        df.to_csv(filename, index=False)
        
        print(f"\nSuccessfully scraped {len(df)} cars!")
        print(f"Data saved to: {filename}")
        
    except Exception as e:
        logging.error(f"An error occurred during scraping: {str(e)}")
        print("\nAn error occurred. Check scraper.log for details.")

if __name__ == "__main__":
    main()