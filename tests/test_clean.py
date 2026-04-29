"""Tests for src/clean.py — column cleaners and pipeline."""

import pandas as pd
import pytest

from src.clean import (
    clean,
    clean_body,
    clean_company_ad,
    clean_engine_capacity,
    clean_manufactured_date,
    clean_mileage,
    clean_numeric,
    clean_price,
    clean_subject,
    clean_text,
    clean_year,
    dedup_reposts,
)


# ---------------------------------------------------------------------------
# clean_price
# ---------------------------------------------------------------------------


class TestCleanPrice:
    def test_strips_rm_and_commas(self):
        s = pd.Series(["RM 3,500", "RM3500", "rm 1,200"])
        result = clean_price(s)
        assert list(result) == [3500, 3500, 1200]

    def test_numeric_passthrough(self):
        s = pd.Series(["50000", "12000"])
        assert list(clean_price(s)) == [50000, 12000]

    def test_non_numeric_becomes_na(self):
        s = pd.Series(["negotiable", "N/A"])
        result = clean_price(s)
        assert result.isna().all()

    def test_returns_int64(self):
        s = pd.Series(["RM 5,000"])
        assert clean_price(s).dtype == pd.Int64Dtype()


# ---------------------------------------------------------------------------
# clean_mileage
# ---------------------------------------------------------------------------


class TestCleanMileage:
    def test_strips_km(self):
        s = pd.Series(["50,000 km", "100000KM", "25000 Km"])
        result = clean_mileage(s)
        assert list(result) == [50000, 100000, 25000]

    def test_non_numeric_becomes_na(self):
        s = pd.Series(["low mileage", ""])
        assert clean_mileage(s).isna().all()


# ---------------------------------------------------------------------------
# clean_engine_capacity
# ---------------------------------------------------------------------------


class TestCleanEngineCapacity:
    def test_strips_cc(self):
        s = pd.Series(["1500cc", "150CC", "250 cc"])
        result = clean_engine_capacity(s)
        assert list(result) == [1500, 150, 250]

    def test_numeric_only_passthrough(self):
        s = pd.Series(["600", "1000"])
        assert list(clean_engine_capacity(s)) == [600, 1000]


# ---------------------------------------------------------------------------
# clean_manufactured_date
# ---------------------------------------------------------------------------


class TestCleanManufacturedDate:
    def test_plain_year(self):
        s = pd.Series(["2022", "2019", "2015"])
        assert list(clean_manufactured_date(s)) == [2022, 2019, 2015]

    def test_1995_or_older_maps_to_1995(self):
        s = pd.Series(["1995 or older", "1995 Or Older"])
        result = clean_manufactured_date(s)
        assert list(result) == [1995, 1995]

    def test_garbage_becomes_na(self):
        s = pd.Series(["unknown", "N/A"])
        assert clean_manufactured_date(s).isna().all()

    def test_returns_int64(self):
        s = pd.Series(["2020"])
        assert clean_manufactured_date(s).dtype == pd.Int64Dtype()


# ---------------------------------------------------------------------------
# clean_company_ad
# ---------------------------------------------------------------------------


class TestCleanCompanyAd:
    def test_string_0_and_1(self):
        s = pd.Series(["0", "1", "0", "1"])
        result = clean_company_ad(s)
        assert list(result) == [0, 1, 0, 1]

    def test_whitespace_stripped(self):
        s = pd.Series([" 1 ", " 0 "])
        assert list(clean_company_ad(s)) == [1, 0]

    def test_non_numeric_becomes_na(self):
        s = pd.Series(["yes", "no"])
        assert clean_company_ad(s).isna().all()

    def test_returns_int64(self):
        s = pd.Series(["1"])
        assert clean_company_ad(s).dtype == pd.Int64Dtype()


# ---------------------------------------------------------------------------
# clean_subject
# ---------------------------------------------------------------------------


class TestCleanSubject:
    def test_strips_whitespace(self):
        s = pd.Series(["  New R25  ", "Y15ZR "])
        result = clean_subject(s)
        assert result[0] == "New R25"
        assert result[1] == "Y15ZR"

    def test_strips_emoji(self):
        s = pd.Series(["Z250 🌕 z250 RAYA ✅ RdyStock 😊"])
        result = clean_subject(s)
        assert "🌕" not in result[0]
        assert "✅" not in result[0]
        assert "😊" not in result[0]

    def test_strips_trailing_full_loan_noise(self):
        s = pd.Series(["Yamaha MT15 - Full Loan Ready Stock"])
        result = clean_subject(s)
        assert result[0] == "Yamaha MT15"

    def test_strips_trailing_ready_stock_noise(self):
        s = pd.Series(["Honda Vario125 ~ Ready Stock Full Loan"])
        result = clean_subject(s)
        assert "Ready Stock" not in result[0]

    def test_strips_trailing_low_deposit_noise(self):
        s = pd.Series(["Kawasaki Z250 - Low Deposit/ Free Delivery"])
        result = clean_subject(s)
        assert result[0] == "Kawasaki Z250"

    def test_no_noise_unchanged(self):
        s = pd.Series(["Yamaha 135LC FI"])
        assert clean_subject(s)[0] == "Yamaha 135LC FI"

    def test_none_becomes_na(self):
        s = pd.Series([None, float("nan")])
        assert clean_subject(s).isna().all()


# ---------------------------------------------------------------------------
# clean_body
# ---------------------------------------------------------------------------


class TestCleanBody:
    def test_redacts_phone_numbers(self):
        s = pd.Series(["Call us at 018-7690060 for more info about this motorcycle."])
        result = clean_body(s)
        assert "018-7690060" not in result[0]
        assert "[PHONE]" in result[0]

    def test_redacts_phone_without_dash(self):
        s = pd.Series(["Contact 0128637378 now to buy this great bike today!"])
        result = clean_body(s)
        assert "0128637378" not in result[0]
        assert "[PHONE]" in result[0]

    def test_strips_emoji(self):
        s = pd.Series(["Ready Stock ✅ Low Deposit 🤗 Full Loan available now for all!"])
        result = clean_body(s)
        assert "✅" not in result[0]
        assert "🤗" not in result[0]

    def test_strips_separator_lines(self):
        body = "Some content here\n============================\nMore content follows below."
        s = pd.Series([body])
        result = clean_body(s)
        assert "====" not in result[0]

    def test_strips_star_separator_lines(self):
        body = "Header info\n*****************************\nDetail info below."
        s = pd.Series([body])
        result = clean_body(s)
        assert "*****" not in result[0]

    def test_collapses_multiple_blank_lines(self):
        body = "Line one\n\n\n\n\nLine two more content here is available."
        s = pd.Series([body])
        result = clean_body(s)
        assert "\n\n\n" not in result[0]

    def test_short_body_becomes_na(self):
        s = pd.Series(["Pm", "Nego", "berminat pm je"])
        result = clean_body(s)
        assert result.isna().all()

    def test_empty_becomes_na(self):
        s = pd.Series([None, "", "   "])
        assert clean_body(s).isna().all()

    def test_normal_body_preserved(self):
        body = (
            "2022 Yamaha Y15ZR in excellent condition. "
            "Mileage low, well maintained, original parts. "
            "Serious buyer only. Price negotiable for cash."
        )
        s = pd.Series([body])
        result = clean_body(s)
        assert isinstance(result[0], str)
        assert len(result[0]) > 20


# ---------------------------------------------------------------------------
# dedup_reposts
# ---------------------------------------------------------------------------


class TestDedupReposts:
    def _make_df(self, rows):
        return pd.DataFrame(rows, columns=["ads_id", "subject", "price", "motorcycle_make"])

    def test_removes_true_repost_keeps_highest_ads_id(self):
        df = self._make_df([
            (100, "Yamaha Y15ZR", "RM 5,000", "Yamaha"),
            (200, "Yamaha Y15ZR", "RM 5,000", "Yamaha"),
        ])
        result, removed = dedup_reposts(df, "motorcycle_make")
        assert removed == 1
        assert len(result) == 1
        assert result["ads_id"].iloc[0] == 200

    def test_keeps_same_subject_different_price(self):
        df = self._make_df([
            (100, "Lc 135", "RM 3,100", "Yamaha"),
            (200, "Lc 135", "RM 8,288", "Yamaha"),
        ])
        result, removed = dedup_reposts(df, "motorcycle_make")
        assert removed == 0
        assert len(result) == 2

    def test_keeps_same_subject_different_make(self):
        df = self._make_df([
            (100, "Sport 150", "RM 6,000", "Honda"),
            (200, "Sport 150", "RM 6,000", "Yamaha"),
        ])
        result, removed = dedup_reposts(df, "motorcycle_make")
        assert removed == 0
        assert len(result) == 2

    def test_no_make_col_returns_unchanged(self):
        df = pd.DataFrame({"ads_id": [1, 2], "subject": ["a", "a"], "price": ["100", "100"]})
        result, removed = dedup_reposts(df, "motorcycle_make")
        assert removed == 0
        assert len(result) == 2

    def test_case_insensitive_subject_match(self):
        df = self._make_df([
            (100, "yamaha y15zr", "RM 5,000", "yamaha"),
            (200, "YAMAHA Y15ZR", "RM 5,000", "YAMAHA"),
        ])
        result, removed = dedup_reposts(df, "motorcycle_make")
        assert removed == 1


# ---------------------------------------------------------------------------
# clean() pipeline — motorcycles
# ---------------------------------------------------------------------------


class TestCleanPipelineMotorcycles:
    def _base_df(self):
        return pd.DataFrame({
            "ads_id":           [1, 2, 3],
            "subject":          ["New R25  ", "Z250 🌕 RAYA ✅", "Yamaha MT15 - Full Loan Ready Stock"],
            "body":             [
                "Call 018-7690060 for info about this motorcycle available now today!",
                "Ready Stock ✅ great bike for daily use. Low deposit and full loan available.",
                None,
            ],
            "price":            ["RM 5,000", "RM 21,000", "RM 12,498"],
            "manufactured_date": ["2023", "1995 or older", "2024"],
            "company_ad":       ["1", "0", "1"],
            "motorcycle_make":  ["Yamaha", "Kawasaki", "Yamaha"],
            "motorcycle_model": ["R25", "Z250", "MT15"],
            "condition":        ["New", "Used", "New"],
            "location":         ["johor - johor bahru", "KL - Cheras", "Selangor"],
            "mileage":          [None, None, None],
        })

    def test_price_numeric(self):
        df = clean(self._base_df(), category="motorcycles")
        assert df["price"].dtype == pd.Int64Dtype()
        assert df["price"].iloc[0] == 5000

    def test_manufactured_date_1995_or_older(self):
        df = clean(self._base_df(), category="motorcycles")
        assert df["manufactured_date"].iloc[1] == 1995

    def test_company_ad_int(self):
        df = clean(self._base_df(), category="motorcycles")
        assert df["company_ad"].dtype == pd.Int64Dtype()

    def test_subject_cleaned(self):
        df = clean(self._base_df(), category="motorcycles")
        assert df["subject"].iloc[0] == "New R25"
        assert "🌕" not in str(df["subject"].iloc[1])
        assert "Full Loan" not in str(df["subject"].iloc[2])

    def test_body_phone_redacted(self):
        df = clean(self._base_df(), category="motorcycles")
        assert "018-7690060" not in str(df["body"].iloc[0])
        assert "[PHONE]" in str(df["body"].iloc[0])

    def test_body_null_when_short(self):
        df = clean(self._base_df(), category="motorcycles")
        assert pd.isna(df["body"].iloc[2])

    def test_location_title_cased(self):
        df = clean(self._base_df(), category="motorcycles")
        assert df["location"].iloc[0] == "Johor - Johor Bahru"

    def test_row_count_unchanged_no_dupes(self):
        df = clean(self._base_df(), category="motorcycles")
        assert len(df) == 3

    def test_price_outlier_flagged(self, capsys):
        df = self._base_df()
        df.loc[0, "price"] = "RM 100"
        clean(df, category="motorcycles")
        captured = capsys.readouterr()
        assert "WARNING" in captured.out
        assert "price < 1000" in captured.out

    def test_dedup_removes_repost(self):
        df = self._base_df()
        # Add true repost: same subject+price+make, higher ads_id
        extra = df.iloc[[0]].copy()
        extra["ads_id"] = 99
        df = pd.concat([df, extra], ignore_index=True)
        result = clean(df, category="motorcycles")
        assert len(result) == 3
        # Highest ads_id kept
        assert 99 in result["ads_id"].values
        assert 1 not in result["ads_id"].values
