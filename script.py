import requests
import cloudscraper
import json
from bs4 import BeautifulSoup as bs
import pandas as pd
import re
import time
import random
import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import RequestException
from typing import List, Dict, Any, Optional
import logging
from fake_useragent import UserAgent
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)


class MudahScraper:
    def __init__(self, max_retries: int = 3, base_delay: int = 2, max_workers: int = 5):
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_workers = max_workers
        self.ua = UserAgent()

        self.keys = [
            "price",
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

    def _make_request(self, url: str) -> requests.Response:
        """Make a request with iterative exponential backoff retry."""
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                jitter = random.uniform(0.5, 1.5)
                delay = (self.base_delay * (2 ** attempt)) * jitter
                time.sleep(delay)

                response = self.scraper.get(url, headers=self._get_random_headers())
                response.raise_for_status()
                return response

            except RequestException as e:
                last_error = e
                if attempt < self.max_retries:
                    logging.warning(f"Request failed for {url}. Retrying... ({attempt + 1}/{self.max_retries})")
                else:
                    logging.error(f"Max retries reached for {url}. Error: {str(e)}")

        raise last_error

    def get_car_urls(self, page_url: str, expected_urls: int = 40, max_retries: int = 3) -> List[str]:
        """Extract car listing URLs from a page using three fallback parsing strategies."""
        best_result: List[str] = []

        for attempt in range(max_retries):
            if attempt > 0:
                delay = self.base_delay * (2 ** (attempt - 1)) + random.uniform(1, 3)
                logging.info(f"Retry #{attempt} for {page_url}. Waiting {delay:.2f} seconds...")
                time.sleep(delay)

            try:
                response = self._make_request(page_url)
                soup = bs(response.text, 'html.parser')
                car_urls: set = set()

                # Strategy 1: LD+JSON structured data
                script = soup.find('script', type='application/ld+json')
                if script is not None:
                    try:
                        data = json.loads(script.text)
                        item_list_element = data[2].get('itemListElement', [])
                        car_urls.update(item['item']['url'] for item in item_list_element)
                    except (json.JSONDecodeError, IndexError, KeyError) as e:
                        logging.warning(f"Error parsing LD+JSON data: {str(e)}")

                # Strategy 2: HTML listing card elements
                listing_elements = (
                    soup.select('div[class*="listing-card"]') or
                    soup.select('div[class*="listing-item"]') or
                    soup.select('div[class*="product-card"]')
                )
                for element in listing_elements:
                    link = (
                        element.find('a', href=re.compile(r'/cars/.+\.html?')) or
                        element.find('a', href=re.compile(r'.+\.html?'))
                    )
                    if link and 'href' in link.attrs:
                        url = link['href']
                        if not url.startswith('http'):
                            url = f"https://www.mudah.my{url}"
                        car_urls.add(url)

                # Strategy 3: __NEXT_DATA__ JSON blob
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
                if len(result_urls) > len(best_result):
                    best_result = result_urls

                if len(result_urls) >= expected_urls:
                    logging.info(f"Successfully collected {len(result_urls)} URLs from {page_url}")
                    return result_urls

                logging.warning(
                    f"Attempt {attempt + 1}/{max_retries}: Found {len(result_urls)} URLs "
                    f"on {page_url} (expected {expected_urls})"
                )

            except Exception as e:
                logging.error(f"Error on attempt {attempt + 1} for {page_url}: {str(e)}")

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
        """Generate paginated Mudah.my search URLs for a given state and brand."""
        base_url = "https://www.mudah.my"
        state = state.lower() if state else "malaysia"

        if not brand or brand.lower() == 'none':
            url = f"{base_url}/{state}/cars-for-sale?o="
        else:
            url = f"{base_url}/{state}/cars-for-sale/{brand}?o="

        return [url + str(page) for page in range(start, end + 1)]

    def get_car_info(self, url: str) -> Optional[List[Dict]]:
        """Extract structured attribute list from a single car listing URL."""
        try:
            response = self._make_request(url)
            soup = bs(response.text, 'html.parser')

            script_tag = (
                soup.find('script', id="__NEXT_DATA__") or
                soup.find('script', type='application/json')
            )
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

            car_attrs = ads_id.get('attributes', {}).get('categoryParams', [])
            car_mcdparams = ads_id.get('attributes', {}).get('mcdParams', [])
            mcd_params = self.parse_mcdparams(car_mcdparams)

            return car_attrs + dict_id + mcd_params

        except Exception as e:
            logging.error(f"Error processing car info for {url}: {str(e)}")
            return None

    def _attrs_to_dict(self, car_attrs: List[Dict]) -> Dict[str, Any]:
        """Flatten a list of attribute dicts into a single key-value record."""
        return {item['id']: item['value'] for item in car_attrs if item['id'] in self.keys}

    def scrape_cars(
        self,
        state: str,
        brand: str,
        start: int,
        end: int,
        expected_urls_per_page: int = 40,
        checkpoint_every: int = 100,
    ) -> pd.DataFrame:
        """Scrape car listings; fetches details concurrently and checkpoints progress to disk."""
        logging.info(f"Starting scrape for '{brand or 'all'}' cars in '{state}', pages {start}–{end}")

        page_urls = self.get_page_urls(state, brand, start, end)
        car_urls: List[str] = []

        with tqdm(total=len(page_urls), desc="Collecting pages", unit="page") as pbar:
            for url in page_urls:
                urls = self.get_car_urls(url, expected_urls=expected_urls_per_page)
                car_urls.extend(urls)
                pbar.update(1)
                pbar.set_postfix({"found": len(urls), "expected": expected_urls_per_page})

        logging.info(f"Found {len(car_urls)} listings — fetching details with {self.max_workers} workers")

        processed_data: List[Dict] = []
        checkpoint_path = f"checkpoint_{brand or 'all'}_{state}.csv"

        with tqdm(total=len(car_urls), desc="Processing cars", unit="car") as pbar:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_url = {executor.submit(self.get_car_info, url): url for url in car_urls}
                for i, future in enumerate(as_completed(future_to_url), 1):
                    result = future.result()
                    if result:
                        processed_data.append(self._attrs_to_dict(result))
                        pbar.set_postfix({"success": True})
                    else:
                        pbar.set_postfix({"success": False})
                    pbar.update(1)

                    if i % checkpoint_every == 0:
                        pd.DataFrame(processed_data).to_csv(checkpoint_path, index=False)
                        logging.info(f"Checkpoint saved: {len(processed_data)} records → {checkpoint_path}")

        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)

        return pd.DataFrame(processed_data)

    def update_master(self, new_df: pd.DataFrame, master_path: str) -> pd.DataFrame:
        """Merge new data into the master Excel file, deduplicating on ads_id."""
        if os.path.exists(master_path):
            master_df = pd.read_excel(master_path)
            combined = pd.concat([master_df, new_df], ignore_index=True)
        else:
            combined = new_df

        if 'ads_id' in combined.columns:
            before = len(combined)
            combined = combined.drop_duplicates(subset='ads_id', keep='last')
            removed = before - len(combined)
            logging.info(f"Deduplication: {before} → {len(combined)} records ({removed} duplicates removed)")

        combined.to_excel(master_path, index=False)
        logging.info(f"Master file updated: {master_path}")
        return combined


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mudah.my car listing scraper")
    parser.add_argument("--state", default="malaysia", help="State to scrape (default: malaysia)")
    parser.add_argument("--brand", default="", help="Car brand filter (default: all brands)")
    parser.add_argument("--start", type=int, default=1, help="Start page number (default: 1)")
    parser.add_argument("--end", type=int, default=1, help="End page number (default: 1)")
    parser.add_argument("--workers", type=int, default=5, help="Concurrent workers for detail fetching (default: 5)")
    parser.add_argument("--output-dir", default=".", help="Directory to save output CSV (default: current dir)")
    parser.add_argument("--master", default="MasterMudahCarData.xlsx", help="Path to master Excel file")
    parser.add_argument("--update-master", action="store_true", help="Merge results into master Excel file after scraping")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.start > args.end:
        print(f"Error: --start ({args.start}) must be <= --end ({args.end})")
        return
    if args.workers < 1:
        print("Error: --workers must be at least 1")
        return

    scraper = MudahScraper(max_workers=args.workers)

    print(f"\n=== Mudah.my Car Scraper ===")
    print(f"State: {args.state} | Brand: {args.brand or 'all'} | Pages: {args.start}–{args.end} | Workers: {args.workers}\n")

    try:
        df = scraper.scrape_cars(args.state, args.brand, args.start, args.end)

        timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
        os.makedirs(args.output_dir, exist_ok=True)
        brand_label = args.brand or 'all'
        filename = os.path.join(
            args.output_dir,
            f"mudah_cars_{brand_label}_{args.state}_{args.start}_to_{args.end}_{timestamp}.csv"
        )
        df.to_csv(filename, index=False)
        print(f"\nScraped {len(df)} cars → {filename}")

        if args.update_master:
            scraper.update_master(df, args.master)
            print(f"Master file updated: {args.master}")

    except Exception as e:
        logging.error(f"Scraper failed: {str(e)}")
        print("\nAn error occurred. Check scraper.log for details.")


if __name__ == "__main__":
    main()
