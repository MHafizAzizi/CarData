-- CarData SQLite schema — MOTORCYCLES database
-- Companion file: schema_cars.sql (keep shared columns in sync).
-- Datetimes are ISO-8601 TEXT ('YYYY-MM-DD HH:MM:SS').

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS listings (
    -- identity
    ads_id              INTEGER PRIMARY KEY,
    url                 TEXT UNIQUE,
    -- listing content (shared with cars)
    subject             TEXT,
    body                TEXT,
    price               TEXT,
    condition           TEXT,
    manufactured_date   TEXT,
    mileage             TEXT,
    location            TEXT,
    region              TEXT,
    subregion           TEXT,
    seller_name         TEXT,
    company_ad          TEXT,
    published           TEXT,
    -- motorcycle-specific
    motorcycle_make     TEXT,
    motorcycle_model    TEXT,
    -- availability tracking (shared with cars)
    first_seen_at       TEXT NOT NULL,
    last_seen_at        TEXT,
    last_checked_at     TEXT,
    availability_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK (availability_status IN ('available','unavailable','unknown'))
);

CREATE TABLE IF NOT EXISTS availability_checks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ads_id          INTEGER NOT NULL REFERENCES listings(ads_id) ON DELETE CASCADE,
    checked_at      TEXT NOT NULL,
    http_status     INTEGER,
    detected_status TEXT NOT NULL
        CHECK (detected_status IN ('available','soft_404','removed','blocked','transient'))
);

CREATE INDEX IF NOT EXISTS idx_checks_ads_time
    ON availability_checks(ads_id, checked_at DESC);

CREATE INDEX IF NOT EXISTS idx_listings_status
    ON listings(availability_status, last_checked_at);

INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '1');
INSERT OR IGNORE INTO meta (key, value) VALUES ('category', 'motorcycles');
