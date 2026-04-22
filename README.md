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

```bash
python script.py [OPTIONS]
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--state` | `malaysia` | State to scrape (e.g. `selangor`, `johor`) |
| `--brand` | all brands | Car brand filter (e.g. `toyota`, `honda`) |
| `--start` | `1` | Start page number |
| `--end` | `1` | End page number (max ~250) |
| `--workers` | `5` | Concurrent workers for fetching listing details |
| `--output-dir` | `.` | Directory to save the output CSV |
| `--master` | `MasterMudahCarData.xlsx` | Path to the master Excel file |
| `--update-master` | off | Merge results into the master Excel file after scraping |

### Examples

Scrape all Toyota listings in Selangor across pages 1–50:
```bash
python script.py --state selangor --brand toyota --start 1 --end 50
```

Scrape all brands nationwide and update the master file:
```bash
python script.py --state malaysia --start 1 --end 250 --workers 10 --update-master
```

Save output to a specific folder:
```bash
python script.py --state kuala-lumpur --brand honda --start 1 --end 30 --output-dir ./data
```

---

## Output Columns

Each row in the output CSV/Excel represents one car listing with up to 37 fields:

| Column | Description |
|---|---|
| `ads_id` | Unique Mudah listing ID |
| `price` | Listed price (MYR) |
| `make` | Car manufacturer (e.g. Toyota) |
| `model` | Car model (e.g. Vios) |
| `variant` | Trim/variant |
| `manufactured_date` | Year of manufacture |
| `mileage` | Odometer reading |
| `transmission` | Auto / Manual |
| `engine_capacity` | Engine displacement |
| `fuel_type` | Petrol / Diesel / Electric |
| `condition` | New / Used |
| `location` | State/region of listing |
| `car_type` | Body type (Sedan, SUV, etc.) |
| `cc`, `kw`, `torque` | Engine specs |
| `length`, `width`, `height`, `wheelbase` | Dimensions |
| `seat` | Seating capacity |
| ... | Additional specs (brakes, suspension, tyres, etc.) |

---

## Master Data

The master file (`MasterMudahCarData.xlsx`) accumulates all scraped runs. Use `--update-master` to merge new results — duplicates are automatically removed based on `ads_id`.

The master file is **not tracked in git** due to its size. Store it locally or in shared cloud storage (e.g. Google Drive, S3).
