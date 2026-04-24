# CarData

Car listing data scraped from [Mudah.my](https://www.mudah.my), Malaysia's largest classifieds site. The master dataset is updated regularly as listings change frequently.

> **Note:** Mudah.my caps browsable listings at ~250 pages. Re-running every 1–2 days is recommended to keep data fresh.

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Usage

### Interactive mode (recommended)

Run the script with no arguments and it will prompt you for inputs:

```bash
python script.py
```

You'll be asked for:
1. **Category** — `cars` or `motorcycles` (default: cars)
2. **State** — e.g. `selangor`, `johor` (default: malaysia)
3. **Brand** — leave blank for all brands
4. **Start page** — default 1
5. **How many pages to scrape**

### Non-interactive mode (for automation)

Pass any of the flags below to skip the corresponding prompt:

| Flag | Default | Description |
|---|---|---|
| `--category` | *prompted* | Category: `cars` or `motorcycles` |
| `--state` | *prompted* | State to scrape (e.g. `selangor`, `johor`) |
| `--brand` | *prompted* | Brand filter (see `brands.md` for available brands) |
| `--start` | *prompted* | Start page number |
| `--end` | *prompted* | End page number (max ~250) |
| `--pages` | — | Number of pages to scrape from `--start` (alternative to `--end`) |
| `--workers` | `2` | Concurrent workers for fetching listing details |
| `--output-dir` | `data/raw` | Directory to save the output CSV |
| `--master` | `data/master/MasterMudahCarData.xlsx` | Path to the master Excel file |
| `--update-master` | off | Merge results into the master Excel file after scraping |

### Examples

**Scrape cars — all Toyota listings in Selangor:**
```bash
python script.py --category cars --state selangor --brand toyota --start 1 --end 50
```

**Scrape motorcycles — all brands nationwide, first 5 pages:**
```bash
python script.py --category motorcycles --brand "" --start 1 --pages 5
```

**Scrape all car brands nationwide and update master:**
```bash
python script.py --category cars --start 1 --end 250 --workers 4 --update-master
```

**Save output to a specific folder:**
```bash
python script.py --state kuala-lumpur --brand honda --start 1 --end 30 --output-dir ./data/raw
```

See [`brands.md`](brands.md) for a complete list of available brands.

---

## Output Columns

Each row in the output CSV/Excel represents one listing. Columns vary slightly between cars and motorcycles.

### Common fields (both categories)

| Column | Description |
|---|---|
| `ads_id` | Unique Mudah listing ID |
| `subject` | Full listing title |
| `price` | Listed price (MYR) |
| `condition` | New / Used |
| `manufactured_date` | Year of manufacture |
| `location` | State + area (e.g. "Johor - Skudai") |
| `region` | State only |
| `subregion` | Area only |
| `seller_name` | Dealer or seller name |
| `company_ad` | `1` = dealer listing, blank = private |
| `published` | Listing date/time, parsed to full datetime (`YYYY-MM-DD HH:MM`) |
| `Tarikh_Kemaskini` | Date the row was scraped (`YYYY-MM-DD`) |

### Cars — additional fields

| Column | Description |
|---|---|
| `make` / `model` | Brand and model (e.g. Toyota / Vios) |
| `variant` / `series` / `family` / `style` | Trim and body classification |
| `mileage` | Odometer reading |
| `transmission` | Auto / Manual |
| `engine_capacity`, `cc`, `kw`, `torque`, `comp_ratio` | Engine specs |
| `engine`, `fuel_type` | Engine code + fuel type |
| `car_type` | Body type (Sedan, SUV, etc.) |
| `seat`, `country_origin` | Seats + country of manufacture |
| `length`, `width`, `height`, `wheelbase`, `kerbwt`, `fueltk` | Dimensions and weights |
| `brake_front`, `brake_rear`, `suspension_front`, `suspension_rear`, `steering` | Chassis specs |
| `tyres_front`, `tyres_rear`, `wheel_rim_front`, `wheel_rim_rear` | Wheels and tyres |

### Motorcycles — additional fields

| Column | Description |
|---|---|
| `motorcycle_make` / `motorcycle_model` | Brand and model (e.g. Yamaha / Y15ZR) |

> Motorcycle listings expose fewer technical specs on Mudah than cars. Fields from the cars table will appear as blanks in motorcycle output.

---

## Master Data

The master file (`data/master/MasterMudahCarData.xlsx`) accumulates all scraped runs. Use `--update-master` to merge new results — duplicates are automatically removed based on `ads_id`.

The master file is **not tracked in git** due to its size. Store it locally or in shared cloud storage (e.g. Google Drive, S3).
