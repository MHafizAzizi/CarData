-- CarData SQLite schema
-- All datetimes are stored as ISO-8601 TEXT ('YYYY-MM-DD HH:MM:SS') for
-- lexicographic sortability and compatibility with sqlite's date functions.

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS listings (
    -- identity
    ads_id              INTEGER PRIMARY KEY,
    url                 TEXT UNIQUE,
    -- listing content
    subject             TEXT,
    body                TEXT,
    price               TEXT,
    -- common attributes
    condition           TEXT,
    make                TEXT,
    model               TEXT,
    motorcycle_make     TEXT,
    motorcycle_model    TEXT,
    manufactured_date   TEXT,
    mileage             TEXT,
    location            TEXT,
    region              TEXT,
    subregion           TEXT,
    seller_name         TEXT,
    company_ad          TEXT,
    -- car-specific
    car_type            TEXT,
    transmission        TEXT,
    engine_capacity     TEXT,
    family              TEXT,
    variant             TEXT,
    series              TEXT,
    style               TEXT,
    seat                TEXT,
    country_origin      TEXT,
    cc                  TEXT,
    comp_ratio          TEXT,
    kw                  TEXT,
    torque              TEXT,
    engine              TEXT,
    fuel_type           TEXT,
    length              TEXT,
    width               TEXT,
    height              TEXT,
    wheelbase           TEXT,
    kerbwt              TEXT,
    fueltk              TEXT,
    brake_front         TEXT,
    brake_rear          TEXT,
    suspension_front    TEXT,
    suspension_rear     TEXT,
    steering            TEXT,
    tyres_front         TEXT,
    tyres_rear          TEXT,
    wheel_rim_front     TEXT,
    wheel_rim_rear      TEXT,
    published           TEXT,
    -- availability tracking
    first_seen_at       TEXT NOT NULL,
    last_seen_at        TEXT,
    last_checked_at     TEXT,
    availability_status TEXT NOT NULL DEFAULT 'unknown'
        CHECK (availability_status IN ('available','unavailable','unknown'))
);

-- Append-only audit trail of every probe.
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

-- Schema version marker. Bump and migrate when changing tables.
INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', '1');
