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
    STUB_STATUS,
    STUB_TYPE,
    TYPE_TO_GROUP,
    car_types_path,
    load_car_types,
    load_model_types,
    model_types_path,
    stub_unmapped_types,
)

EXPECTED_COLUMNS = [
    "motorcycle_make", "motorcycle_model", "motorcycle_type",
    "classification_method", "evidence", "source_url", "verification_status",
]

EXPECTED_CAR_COLUMNS = [
    "car_make", "car_model", "vehicle_type",
    "classification_method", "evidence", "source_url", "verification_status",
]

ALLOWED_STATUSES = {
    "Verified", "Verified sample", "LLM-reviewed", "Needs source URL",
    "Auto-stub",  # written by reference.stub_unmapped_types for unmapped pairs
}


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


# Header (make col, model col, type col) per category — mirrors the CSVs.
_HEADERS = {
    "motorcycles": "motorcycle_make,motorcycle_model,motorcycle_type,"
                   "classification_method,evidence,source_url,verification_status\n",
    "cars": "car_make,car_model,vehicle_type,"
            "classification_method,evidence,source_url,verification_status\n",
}


@pytest.fixture
def temp_reference(tmp_path, monkeypatch):
    """Redirect the model-type CSV paths to an isolated temp dir so
    stub_unmapped_types() never touches the real reference files."""
    import reference
    monkeypatch.setattr(reference, "REFERENCE_DIR", tmp_path)
    return tmp_path


@pytest.mark.parametrize("category,make_col,model_col,type_col", [
    ("motorcycles", "motorcycle_make", "motorcycle_model", "motorcycle_type"),
    ("cars", "car_make", "car_model", "vehicle_type"),
])
class TestStubUnmappedTypes:
    def _seed(self, tmp_path, category):
        path = tmp_path / f"{category}_model_types.csv"
        # one pre-existing row ('Zeta Zz') to prove dedupe + sort.
        head = _HEADERS[category]
        path.write_text(head + f"Zeta,Zz,{STUB_TYPE},manual,,,Verified\n",
                        encoding="utf-8")
        return path

    def test_appends_sorts_and_dedupes(
        self, temp_reference, category, make_col, model_col, type_col
    ):
        path = self._seed(temp_reference, category)
        added = stub_unmapped_types(category, [
            ("Acme", "One"),       # new -> stub
            ("Zeta", "Zz"),        # already present (case-exact) -> skip
            ("zeta", "zz"),        # already present (casefold) -> skip
            ("", "Blank"),         # empty make -> skip
            ("Blank", ""),         # empty model -> skip
        ])
        assert added == 1

        with open(path, encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        # sorted by (make, model): Acme before Zeta
        assert [r[make_col] for r in rows] == ["Acme", "Zeta"]
        stub = rows[0]
        assert stub[type_col] == STUB_TYPE
        assert stub["verification_status"] == STUB_STATUS

    def test_idempotent(
        self, temp_reference, category, make_col, model_col, type_col
    ):
        self._seed(temp_reference, category)
        assert stub_unmapped_types(category, [("Acme", "One")]) == 1
        # second call with the same pair adds nothing
        assert stub_unmapped_types(category, [("Acme", "One")]) == 0

    def test_stub_type_is_loadable(
        self, temp_reference, category, make_col, model_col, type_col
    ):
        """The stub type must be in the type vocab so the loader accepts it
        instead of raising ValueError."""
        self._seed(temp_reference, category)
        stub_unmapped_types(category, [("Acme", "One")])
        loader = load_model_types if category == "motorcycles" else load_car_types
        mapping = loader()  # must not raise
        assert mapping[("acme", "one")][1] == "Unknown"
