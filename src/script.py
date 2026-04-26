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
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import RequestException
from typing import List, Dict, Any, Optional
import logging
from fake_useragent import UserAgent
from tqdm import tqdm


def _clean_body(text: Optional[str]) -> str:
    """Normalize Mudah's HTML-flavored ad body to plain text."""
    if not text:
        return ''
    # Mudah uses <br>, <br/>, <br /> as line breaks; collapse all to \n
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    # Strip any other stray HTML tags (rare, but defensive)
    text = re.sub(r'<[^>]+>', '', text)
    return text.strip()


def _parse_published(text: str) -> str:
    """Convert Mudah's relative date strings ('Today 15:49', 'Yesterday 10:30',
    '22 Apr 09:15') to absolute 'YYYY-MM-DD HH:MM'."""
    if not text:
        return ''
    today = datetime.now()
    text = text.strip()
    low = text.lower()
    try:
        if low.startswith('today'):
            time_part = text.split(' ', 1)[1] if ' ' in text else '00:00'
            return f"{today.strftime('%Y-%m-%d')} {time_part}"
        if low.startswith('yesterday'):
            y = today - timedelta(days=1)
            time_part = text.split(' ', 1)[1] if ' ' in text else '00:00'
            return f"{y.strftime('%Y-%m-%d')} {time_part}"
        try:
            dt = datetime.strptime(f"{today.year} {text}", '%Y %d %b %H:%M')
            if dt > today:
                dt = dt.replace(year=today.year - 1)
            return dt.strftime('%Y-%m-%d %H:%M')
        except ValueError:
            pass
        try:
            return datetime.strptime(text, '%d %b %Y').strftime('%Y-%m-%d')
        except ValueError:
            pass
    except Exception:
        pass
    return text

import sys

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except (AttributeError, OSError):
    pass

os.makedirs('../logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../logs/scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


class MudahScraper:
    def __init__(self, max_retries: int = 3, base_delay: int = 2, max_workers: int = 2):
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

        self._request_lock = threading.Lock()
        self._last_request_time = 0.0
        self._min_request_interval = (3.0, 4.0)

        self.keys = [
            "url",
            "ads_id", "subject", "body", "price",
            "condition", "make", "model", "motorcycle_make", "motorcycle_model",
            "manufactured_date", "mileage",
            "location", "region", "subregion",
            "seller_name", "company_ad",
            "car_type", "transmission", "engine_capacity",
            "family", "variant", "series",
            "style", "seat", "country_origin", "cc", "comp_ratio", "kw",
            "torque", "engine", "fuel_type", "length", "width", "height",
            "wheelbase", "kerbwt", "fueltk", "brake_front", "brake_rear",
            "suspension_front", "suspension_rear", "steering", "tyres_front",
            "tyres_rear", "wheel_rim_front", "wheel_rim_rear",
            "published", "Tarikh_Kemaskini",
        ]

    def _get_random_headers(self) -> Dict[str, str]:
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }

    def _throttle(self) -> None:
        """Block until at least min_request_interval has passed since the last request.
        Holding the lock during sleep serializes pacing across worker threads."""
        with self._request_lock:
            target_gap = random.uniform(*self._min_request_interval)
            elapsed = time.monotonic() - self._last_request_time
            if elapsed < target_gap:
                time.sleep(target_gap - elapsed)
            self._last_request_time = time.monotonic()

    def _make_request(self, url: str) -> requests.Response:
        """Make a request with rate limiting and a fixed-schedule retry backoff."""
        retry_waits = [2, 3, 5]
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                if attempt == 0:
                    self._throttle()
                else:
                    wait = retry_waits[min(attempt - 1, len(retry_waits) - 1)]
                    time.sleep(wait)

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

    def get_car_urls(self, page_url: str, expected_urls: int = 20, max_retries: int = 3) -> List[str]:
        """Extract car listing URLs from a page using three fallback parsing strategies."""
        best_result: List[str] = []

        retry_waits = [2, 3, 5]
        for attempt in range(max_retries):
            if attempt > 0:
                delay = retry_waits[min(attempt - 1, len(retry_waits) - 1)]
                logging.info(f"Retry #{attempt} for {page_url}. Waiting {delay} seconds...")
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
        for group in car_mcdparams or []:
            if not isinstance(group, dict):
                continue
            for params in (group.get('params') or []):
                other_dets.append({
                    'realValue': params.get('realValue', ''),
                    'id': params.get('id', ''),
                    'value': params.get('value', ''),
                    'label': params.get('label', '')
                })
        return other_dets

    def get_page_urls(self, state: str = "", category: str = "cars", brand: str = "", start: int = 1, end: int = 1) -> List[str]:
        """Generate paginated Mudah.my search URLs for a given state, category, and brand."""
        base_url = "https://www.mudah.my"
        state = state.lower() if state else "malaysia"

        category_map = {
            "cars": "cars-for-sale",
            "motorcycles": "motorcycles-for-sale",
        }
        category_path = category_map.get(category.lower(), "cars-for-sale")

        if not brand or brand.lower() == 'none':
            url = f"{base_url}/{state}/{category_path}?o="
        else:
            url = f"{base_url}/{state}/{category_path}/{brand}?o="

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

            props = data.get('props', {})
            ads_id = props.get('initialState', {}).get('adDetails', {}).get('byID', {}).get(car_ads_no, {})

            attributes = ads_id.get('attributes', {}) or {}
            car_attrs = attributes.get('categoryParams') or []
            car_mcdparams = attributes.get('mcdParams') or []
            mcd_params = self.parse_mcdparams(car_mcdparams)

            top_level = [
                {'id': 'url', 'value': url, 'realValue': '', 'label': 'Listing URL'},
                {'id': 'ads_id', 'value': car_ads_no, 'realValue': '', 'label': 'Ad ID'},
                {'id': 'subject', 'value': attributes.get('subject', ''), 'realValue': '', 'label': 'Subject'},
                {'id': 'body', 'value': _clean_body(attributes.get('body')), 'realValue': '', 'label': 'Description'},
                {'id': 'region', 'value': attributes.get('regionName', ''), 'realValue': '', 'label': 'Region'},
                {'id': 'subregion', 'value': attributes.get('subregionName', ''), 'realValue': '', 'label': 'Subregion'},
                {'id': 'seller_name', 'value': attributes.get('name', ''), 'realValue': '', 'label': 'Seller'},
                {'id': 'company_ad', 'value': attributes.get('companyAd', ''), 'realValue': '', 'label': 'Company Ad'},
                {'id': 'published', 'value': _parse_published(attributes.get('publishedDatetime', '')), 'realValue': '', 'label': 'Published'},
                {'id': 'Tarikh_Kemaskini', 'value': datetime.now().strftime('%Y-%m-%d'), 'realValue': '', 'label': 'Scrape Date'},
            ]

            return car_attrs + top_level + mcd_params

        except Exception as e:
            logging.error(f"Error processing car info for {url}: {str(e)}")
            return None

    def _attrs_to_dict(self, car_attrs: List[Dict]) -> Dict[str, Any]:
        """Flatten a list of attribute dicts into a single key-value record."""
        return {item['id']: item['value'] for item in car_attrs if item['id'] in self.keys}

    def scrape_cars(
        self,
        state: str,
        category: str,
        brand: str,
        start: int,
        end: int,
        expected_urls_per_page: int = 20,
        checkpoint_every: int = 100,
    ) -> pd.DataFrame:
        """Scrape listings; fetches details concurrently and checkpoints progress to disk."""
        logging.info(f"Starting scrape for '{brand or 'all'}' {category} in '{state}', pages {start}–{end}")

        page_urls = self.get_page_urls(state, category, brand, start, end)
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
                        logging.info(f"Checkpoint saved: {len(processed_data)} records -> {checkpoint_path}")

        if os.path.exists(checkpoint_path):
            os.remove(checkpoint_path)

        return pd.DataFrame(processed_data)

    def update_db(self, new_df: pd.DataFrame, category: str) -> int:
        """Upsert scraped rows into the per-category SQLite database.

        Each successfully scraped row is also a confirmation that the listing
        was live at this moment, so on insert OR update we set:
          - last_seen_at, last_checked_at = now
          - availability_status = 'available'
          - first_seen_at = now (only on initial insert; preserved on update)

        Also appends one row to availability_checks per scraped listing so the
        scraper's audit trail merges with recheck.py's.

        Returns the number of rows attempted.
        """
        from db import connect  # local import: db.py needs schema files at import time
        if new_df is None or new_df.empty:
            logging.info("update_db: no rows to upsert")
            return 0

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = connect(category)

        # Restrict to columns the schema knows about (cars vs motorcycles diverge)
        valid_cols = {r['name'] for r in conn.execute('PRAGMA table_info(listings)').fetchall()}
        df = new_df[[c for c in new_df.columns if c in valid_cols]].copy()

        if 'ads_id' not in df.columns:
            raise ValueError("Scraped data is missing the required `ads_id` column.")
        df['ads_id'] = pd.to_numeric(df['ads_id'], errors='coerce')
        bad = df['ads_id'].isna().sum()
        if bad:
            logging.warning(f"update_db: dropping {bad} rows with non-numeric ads_id")
            df = df[df['ads_id'].notna()].copy()
        df['ads_id'] = df['ads_id'].astype('int64')

        # Stamp tracking columns on every row
        df['first_seen_at'] = now
        df['last_seen_at'] = now
        df['last_checked_at'] = now
        df['availability_status'] = 'available'

        # NaN -> None so sqlite stores NULL
        df = df.astype(object).where(df.notna(), None)

        cols = list(df.columns)
        placeholders = ', '.join(['?'] * len(cols))
        quoted = ', '.join(f'"{c}"' for c in cols)
        # Preserve first_seen_at on conflict; everything else takes the new value
        update_cols = [c for c in cols if c not in ('ads_id', 'first_seen_at')]
        update_set = ', '.join(f'"{c}" = excluded."{c}"' for c in update_cols)
        upsert_sql = (
            f'INSERT INTO listings ({quoted}) VALUES ({placeholders}) '
            f'ON CONFLICT(ads_id) DO UPDATE SET {update_set}'
        )

        rows = [tuple(r) for r in df.itertuples(index=False, name=None)]
        check_rows = [(int(r['ads_id']), now, 200, 'available') for _, r in df.iterrows()]

        with conn:  # one transaction for the whole batch
            conn.executemany(upsert_sql, rows)
            conn.executemany(
                'INSERT INTO availability_checks (ads_id, checked_at, http_status, detected_status) '
                'VALUES (?, ?, ?, ?)',
                check_rows,
            )

        logging.info(f"update_db: upserted {len(rows)} rows into {category} DB and logged {len(check_rows)} checks")
        return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mudah.my listing scraper for cars and motorcycles")
    parser.add_argument("--category", default=None, choices=["cars", "motorcycles"], help="Category to scrape")
    parser.add_argument("--state", default=None, help="State to scrape")
    parser.add_argument("--brand", default=None, help="Brand filter. See brands.md for available brands.")
    parser.add_argument("--start", type=int, default=None, help="Start page number")
    parser.add_argument("--end", type=int, default=None, help="End page number")
    parser.add_argument("--pages", type=int, default=None, help="Number of pages to scrape from --start (alternative to --end)")
    parser.add_argument("--workers", type=int, default=2, help="Concurrent workers for detail fetching (default: 2)")
    parser.add_argument("--output-dir", default="../data/raw", help="Directory to save output CSV (default: ../data/raw)")
    parser.add_argument("--update-db", action="store_true", help="Upsert results into the per-category SQLite database (data/master/cardata_<category>.db)")
    return parser.parse_args()


CATEGORY_CHOICES = ["cars", "motorcycles"]

STATE_CHOICES = [
    "malaysia",
    "johor", "kedah", "kelantan", "kuala-lumpur", "labuan", "melaka",
    "negeri-sembilan", "pahang", "penang", "perak", "perlis", "putrajaya",
    "sabah", "sarawak", "selangor", "terengganu",
]


def _prompt_int(prompt: str, min_val: int = 1) -> int:
    while True:
        try:
            value = int(input(prompt))
            if value < min_val:
                print(f"Please enter a number >= {min_val}.")
            else:
                return value
        except ValueError:
            print("Invalid input. Please enter a whole number.")


def _prompt_choice(label: str, choices: List[str], default_index: int = 0) -> str:
    print(f"\n{label}:")
    for i, choice in enumerate(choices, 1):
        marker = " (default)" if i - 1 == default_index else ""
        print(f"  {i}. {choice}{marker}")
    while True:
        raw = input(f"Select 1-{len(choices)} [default: {default_index + 1}]: ").strip()
        if raw == "":
            return choices[default_index]
        try:
            idx = int(raw)
            if 1 <= idx <= len(choices):
                return choices[idx - 1]
        except ValueError:
            pass
        print(f"Please enter a number between 1 and {len(choices)}.")


def _prompt_inputs(args: argparse.Namespace) -> argparse.Namespace:
    print("\n=== Mudah.my Listing Scraper ===")

    if args.category is None:
        args.category = _prompt_choice("Category", CATEGORY_CHOICES, default_index=0)

    if args.state is None:
        args.state = _prompt_choice("State", STATE_CHOICES, default_index=0)

    if args.brand is None:
        brand_input = input("Brand (leave blank for all): ").strip().lower()
        args.brand = brand_input

    if args.start is None:
        args.start = _prompt_int("Start page [default: 1]: ", min_val=1)

    if args.pages is not None:
        args.end = args.start + args.pages - 1
    elif args.end is None:
        args.end = args.start + _prompt_int("How many pages to scrape? ") - 1

    return args


def main():
    args = parse_args()
    args = _prompt_inputs(args)

    if args.start > args.end:
        print(f"Error: --start ({args.start}) must be <= --end ({args.end})")
        return
    if args.workers < 1:
        print("Error: --workers must be at least 1")
        return

    scraper = MudahScraper(max_workers=args.workers)

    print(f"\nCategory: {args.category} | State: {args.state} | Brand: {args.brand or 'all'} | Pages: {args.start}–{args.end} | Workers: {args.workers}\n")

    try:
        df = scraper.scrape_cars(args.state, args.category, args.brand, args.start, args.end)

        timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
        os.makedirs(args.output_dir, exist_ok=True)
        brand_label = args.brand or 'all'
        filename = os.path.join(
            args.output_dir,
            f"mudah_{args.category}_{brand_label}_{args.state}_{args.start}_to_{args.end}_{timestamp}.csv"
        )
        df.to_csv(filename, index=False)
        print(f"\nScraped {len(df)} {args.category} -> {filename}")

        if args.update_db:
            from db import db_path_for
            n = scraper.update_db(df, args.category)
            print(f"Upserted {n} rows into {db_path_for(args.category)}")

    except Exception as e:
        logging.error(f"Scraper failed: {str(e)}")
        print("\nAn error occurred. Check ../logs/scraper.log for details.")


if __name__ == "__main__":
    main()
