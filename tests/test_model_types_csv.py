"""Validation guards for the curated model-type mapping CSVs:
data/reference/motorcycles_model_types.csv and cars_model_types.csv.

The mappings are hand-edited (new models from future scrapes get rows added
manually), so the failure mode is a human typo: a duplicated key, an empty or
misspelled type, or a stray column. These tests make `pytest` catch a bad
edit before it ever reaches the DB via --enrich-types.
"""

import csv
import sys
from collections import Counter
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "src"))

from reference import (  # noqa: E402
    CAR_TYPE_TO_GROUP,
    TYPE_TO_GROUP,
    car_types_path,
    load_car_types,
    load_model_types,
    model_types_path,
)

EXPECTED_COLUMNS = [
    "motorcycle_make", "motorcycle_model", "motorcycle_type",
    "classification_method", "evidence", "source_url", "verification_status",
]

EXPECTED_CAR_COLUMNS = [
    "car_make", "car_model", "vehicle_type",
    "classification_method", "evidence", "source_url", "verification_status",
]

ALLOWED_STATUSES = {"Verified", "Verified sample", "LLM-reviewed", "Needs source URL"}


@pytest.fixture(scope="module")
def rows():
    path = model_types_path()
    assert path.exists(), f"mapping CSV missing: {path}"
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == EXPECTED_COLUMNS, (
            f"column drift: {reader.fieldnames}"
        )
        return list(reader)


class TestModelTypesCsv:
    def test_no_duplicate_keys(self, rows):
        keys = Counter(
            (r["motorcycle_make"].casefold(), r["motorcycle_model"].casefold())
            for r in rows
        )
        dupes = {k: n for k, n in keys.items() if n > 1}
        assert not dupes, f"duplicate (make, model) keys: {dupes}"

    def test_every_type_in_vocab(self, rows):
        bad = [
            (r["motorcycle_make"], r["motorcycle_model"], r["motorcycle_type"])
            for r in rows
            if r["motorcycle_type"] not in TYPE_TO_GROUP
        ]
        assert not bad, f"unknown motorcycle_type values: {bad}"

    def test_keys_non_empty(self, rows):
        bad = [
            r for r in rows
            if not r["motorcycle_make"].strip() or not r["motorcycle_model"].strip()
        ]
        assert not bad, f"rows with empty make/model: {bad}"

    def test_verification_status_vocab(self, rows):
        bad = sorted({r["verification_status"] for r in rows} - ALLOWED_STATUSES)
        assert not bad, f"unknown verification_status values: {bad}"

    def test_sorted_alphabetically(self, rows):
        """Alphabetical (make, model) order keeps git diffs reviewable —
        new models insert in a predictable spot."""
        keys = [
            (r["motorcycle_make"].casefold(), r["motorcycle_model"].casefold())
            for r in rows
        ]
        assert keys == sorted(keys), "CSV must stay sorted by (make, model)"

    def test_loader_accepts_real_file(self):
        """load_model_types() parses the real CSV without raising (it raises
        ValueError on any type outside TYPE_TO_GROUP)."""
        mapping = load_model_types()
        assert len(mapping) > 900  # sanity: full mapping, not a stub


@pytest.fixture(scope="module")
def car_rows():
    path = car_types_path()
    assert path.exists(), f"mapping CSV missing: {path}"
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == EXPECTED_CAR_COLUMNS, (
            f"column drift: {reader.fieldnames}"
        )
        return list(reader)


class TestCarTypesCsv:
    def test_no_duplicate_keys(self, car_rows):
        keys = Counter(
            (r["car_make"].casefold(), r["car_model"].casefold())
            for r in car_rows
        )
        dupes = {k: n for k, n in keys.items() if n > 1}
        assert not dupes, f"duplicate (make, model) keys: {dupes}"

    def test_every_type_in_vocab(self, car_rows):
        bad = [
            (r["car_make"], r["car_model"], r["vehicle_type"])
            for r in car_rows
            if r["vehicle_type"] not in CAR_TYPE_TO_GROUP
        ]
        assert not bad, f"unknown vehicle_type values: {bad}"

    def test_keys_non_empty(self, car_rows):
        bad = [
            r for r in car_rows
            if not r["car_make"].strip() or not r["car_model"].strip()
        ]
        assert not bad, f"rows with empty make/model: {bad}"

    def test_verification_status_vocab(self, car_rows):
        bad = sorted({r["verification_status"] for r in car_rows} - ALLOWED_STATUSES)
        assert not bad, f"unknown verification_status values: {bad}"

    def test_sorted_alphabetically(self, car_rows):
        """Alphabetical (make, model) order keeps git diffs reviewable —
        new models insert in a predictable spot."""
        keys = [
            (r["car_make"].casefold(), r["car_model"].casefold())
            for r in car_rows
        ]
        assert keys == sorted(keys), "CSV must stay sorted by (make, model)"

    def test_loader_accepts_real_file(self):
        """load_car_types() parses the real CSV without raising (it raises
        ValueError on any type outside CAR_TYPE_TO_GROUP)."""
        mapping = load_car_types()
        assert len(mapping) > 1000  # sanity: full mapping, not a stub
