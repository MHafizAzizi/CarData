# Mudah Motorcycle Listing Availability Tracking Design

## Purpose

This document summarises the proposed design for tracking whether Mudah motorcycle listings are still available over time.

The current problem is that the dataset does not directly show whether a listing has been sold or removed. The only practical signal is whether the listing URL can still be accessed during later scraper runs.

Important assumption:

> If a listing URL is no longer accessible, it should not be treated as confirmed sold. It should only be treated as unavailable.

The system should therefore track listing availability, not confirmed sale status.

---

## Recommended Core Fields

Add the following columns to the scraped listing sheet or database table:

```text
listing_url
first_seen_at
last_seen_at
last_checked_at
availability_status
url_check_history
```

### Field Definitions

| Field | Type | Description |
|---|---|---|
| `listing_url` | string | Full Mudah listing URL. Used for repeated availability checks. |
| `first_seen_at` | datetime | First timestamp when the scraper found the listing. |
| `last_seen_at` | datetime | Last timestamp when the listing page was successfully available. |
| `last_checked_at` | datetime | Most recent timestamp when the scraper checked the listing URL. |
| `availability_status` | string | Current high-level listing status. Only use `available` or `unavailable`. |
| `url_check_history` | JSON array | Historical record of URL availability checks. |

---

## `availability_status` Rule

The `availability_status` column should only contain two possible values:

```text
available
unavailable
```

### Meaning

| Value | Meaning |
|---|---|
| `available` | The listing URL is currently accessible and the page exists. |
| `unavailable` | The listing URL is no longer accessible based on the scraper's rule. |

### Important Note

`unavailable` does **not** mean the motorcycle is confirmed sold.

It may mean:

- the item was sold,
- the seller deleted the ad,
- the ad expired,
- Mudah removed the ad,
- the URL changed,
- the scraper was blocked,
- the page temporarily failed.

Therefore, the correct interpretation is:

```text
unavailable = listing page cannot currently be found or accessed
```

not:

```text
unavailable = sold
```

---

## `url_check_history` JSON Design

The `url_check_history` field should contain a JSON array.

Each object in the array represents one scraper check event.

Only include these three keys:

```text
checked_at
page_available
detected_status
```

### JSON Key Definitions

| Key | Type | Example | Description |
|---|---|---|---|
| `checked_at` | string datetime | `"2026-04-25 07:36:05"` | Timestamp when the URL was checked. |
| `page_available` | boolean | `true` or `false` | Whether the listing page was accessible. |
| `detected_status` | string | `"available"` or `"unavailable"` | Status detected during that specific check. |

---

## Example JSON Value

Pretty version:

```json
[
  {
    "checked_at": "2026-04-25 07:36:05",
    "page_available": true,
    "detected_status": "available"
  },
  {
    "checked_at": "2026-04-26 07:30:00",
    "page_available": true,
    "detected_status": "available"
  },
  {
    "checked_at": "2026-04-27 07:30:00",
    "page_available": false,
    "detected_status": "unavailable"
  }
]
```

Compact one-line version for CSV/Excel:

```json
[{"checked_at":"2026-04-25 07:36:05","page_available":true,"detected_status":"available"},{"checked_at":"2026-04-26 07:30:00","page_available":true,"detected_status":"available"},{"checked_at":"2026-04-27 07:30:00","page_available":false,"detected_status":"unavailable"}]
```

For CSV storage, the compact one-line version is safer because multi-line JSON inside a cell can make imports and exports more fragile.

---

## How the Scraper Should Update the Fields

Each time the scraper checks a listing URL, it should append a new object to `url_check_history`.

### Case 1: Page is available

Append this object:

```json
{
  "checked_at": "2026-04-25 07:36:05",
  "page_available": true,
  "detected_status": "available"
}
```

Update summary columns:

```text
availability_status = available
last_seen_at = current check timestamp
last_checked_at = current check timestamp
```

### Case 2: Page is unavailable

Append this object:

```json
{
  "checked_at": "2026-04-27 07:30:00",
  "page_available": false,
  "detected_status": "unavailable"
}
```

Update summary columns:

```text
availability_status = unavailable
last_checked_at = current check timestamp
```

`last_seen_at` should remain unchanged because the page was not visible during this check.

---

## Recommended Availability Logic

The simplest logic:

```text
If page_available = true:
    availability_status = available

If page_available = false:
    availability_status = unavailable
```

However, the safer logic is:

```text
If page_available = true:
    availability_status = available

If page_available = false for only 1 check:
    keep availability_status = available or mark internally as pending

If page_available = false for 2 or 3 consecutive checks:
    availability_status = unavailable
```

The safer logic reduces false unavailable status caused by temporary network errors, blocking, timeout, or site issues.

If the dataset must only contain `available` and `unavailable`, use the safer logic internally, but only write the final status as one of those two values.

---

## Example Row

| ads_id | listing_url | first_seen_at | last_seen_at | last_checked_at | availability_status | url_check_history |
|---|---|---|---|---|---|---|
| `114475795` | `https://www.mudah.my/used-yamaha-r25-v2-full-loan-otr-free-loan-114475795.htm` | `2026-04-25 07:36:05` | `2026-04-26 07:30:00` | `2026-04-27 07:30:00` | `unavailable` | `[{"checked_at":"2026-04-25 07:36:05","page_available":true,"detected_status":"available"},{"checked_at":"2026-04-26 07:30:00","page_available":true,"detected_status":"available"},{"checked_at":"2026-04-27 07:30:00","page_available":false,"detected_status":"unavailable"}]` |

---

## Why Keep Both Summary Columns and JSON History?

Do not rely only on `url_check_history`.

Keep normal columns for easy filtering and dashboarding:

```text
first_seen_at
last_seen_at
last_checked_at
availability_status
```

Use JSON only as the historical audit trail:

```text
url_check_history
```

This gives two benefits:

1. Dashboards and SQL filters can use simple columns.
2. The full checking timeline is still preserved in JSON.

Recommended interpretation:

```text
availability_status = current status
url_check_history = evidence behind the current status
```

---

## Suggested Implementation Notes for Claude

When implementing the scraper update logic:

1. Read the existing row for the listing.
2. Parse the existing `url_check_history` JSON array.
3. Check the listing URL.
4. Create a new check object with:
   - `checked_at`
   - `page_available`
   - `detected_status`
5. Append the object to the JSON array.
6. Update:
   - `last_checked_at`
   - `last_seen_at` if page is available
   - `availability_status`
7. Save the updated row.

Pseudo-logic:

```text
new_check = {
    "checked_at": current_timestamp,
    "page_available": page_available,
    "detected_status": "available" if page_available else "unavailable"
}

url_check_history.append(new_check)

last_checked_at = current_timestamp

if page_available:
    availability_status = "available"
    last_seen_at = current_timestamp
else:
    availability_status = "unavailable"
```

Safer pseudo-logic with consecutive failed checks:

```text
if page_available:
    availability_status = "available"
    last_seen_at = current_timestamp
else:
    if last N checks are unavailable:
        availability_status = "unavailable"
    else:
        keep previous availability_status
```

Even with safer internal logic, the stored `availability_status` must still only be:

```text
available
unavailable
```

---

## Data Quality Notes

Potential issues to handle:

| Issue | Recommendation |
|---|---|
| Empty `url_check_history` | Initialise as empty array `[]`. |
| Invalid JSON in cell | Reset carefully or log the row for manual review. |
| Missing `listing_url` | Skip availability check and log the issue. |
| Temporary scraper block | Avoid instantly marking unavailable after one failed check. |
| Reposted listing with new URL | Consider a future `listing_fingerprint` field to detect possible reposts. |

---

## Final Recommended Schema

```text
ads_id
listing_url
subject
price
location
region
subregion
seller_name
condition
motorcycle_make
motorcycle_model
manufactured_date
first_seen_at
last_seen_at
last_checked_at
availability_status
url_check_history
```

Optional future field:

```text
listing_fingerprint
```

`listing_fingerprint` can be generated from stable fields such as:

```text
motorcycle_make + motorcycle_model + manufactured_date + price + region + subregion + seller_name
```

This can help detect reposted listings where the original `ads_id` is no longer useful.

---

## External Reference Notes

- JSON supports arrays, objects, strings, booleans, numbers, and null values. This makes the proposed `url_check_history` structure valid JSON.
- If this data is later moved into PostgreSQL, consider using `jsonb` instead of plain `json` for better querying and indexing.
