"""Leakage, feature-contract and split checks, independent of external services."""
import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from final_ml import (FEATURE_COLUMNS, HORIZON_DAYS, NUMERIC_FEATURES, aqi_category,
                      build_features, forecastability, temporal_split, training_labels)


def source_frame(days=25, start="2021-12-15", city_id=1, city_name="Delhi"):
    frame = pd.DataFrame({"date": pd.date_range(start, periods=days)})
    frame["measurement_id"] = np.arange(days) + 1 + city_id * 1000
    frame["city_id"] = city_id
    frame["city_name"] = city_name
    frame["run_id"] = "unit_test"
    frame["aqi"] = np.arange(days, dtype=float) + 100
    for offset, name in enumerate(NUMERIC_FEATURES):
        frame[name] = np.arange(days, dtype=float) + offset + 1
    return frame


class FeatureContractTests(unittest.TestCase):
    def test_features_are_the_same_day_pollutant_readings(self):
        frame = source_frame()
        features = build_features(frame)
        self.assertEqual(HORIZON_DAYS, 0)
        self.assertEqual(len(features), len(frame))
        self.assertTrue((features.origin_date == features.target_date).all())
        first = features.iloc[0]
        self.assertEqual(first.origin_date, "2021-12-15")
        for offset, name in enumerate(NUMERIC_FEATURES):
            self.assertEqual(first[name], offset + 1)

    def test_observed_aqi_and_category_are_not_features(self):
        frame = source_frame()
        frame["aqi_bucket"] = "Poor"
        events = build_features(frame)
        self.assertNotIn("aqi", FEATURE_COLUMNS)
        self.assertNotIn("aqi_bucket", FEATURE_COLUMNS)
        self.assertNotIn("target_aqi", events.columns)
        self.assertNotIn("aqi", events.columns)
        labeled = training_labels(frame)
        self.assertEqual(labeled.iloc[0].target_aqi, 100)
        self.assertEqual(labeled.iloc[0].target_date, "2021-12-15")
        self.assertEqual(len(labeled), len(events))

    def test_changing_aqi_does_not_change_features(self):
        frame = source_frame()
        changed = frame.copy()
        changed["aqi"] = 999
        columns = ["origin_date"] + FEATURE_COLUMNS
        pd.testing.assert_frame_equal(build_features(frame)[columns], build_features(changed)[columns])

    def test_cities_keep_their_own_readings(self):
        first = source_frame(days=5)
        second = source_frame(days=5, city_id=2, city_name="Mumbai")
        second["pm25"] += 500
        features = build_features(pd.concat([second, first]).sample(frac=1, random_state=1))
        self.assertEqual(features.loc[features.city_id.eq(1), "pm25"].min(), 1)
        self.assertEqual(features.loc[features.city_id.eq(2), "pm25"].min(), 501)

    def test_missing_negative_and_duplicate_sources_are_rejected(self):
        frame = source_frame()
        bad = frame.copy()
        bad.loc[0, "pm25"] = np.nan
        with self.assertRaisesRegex(ValueError, "Missing"):
            build_features(bad)
        bad = frame.copy()
        bad.loc[0, "so2"] = -1
        with self.assertRaisesRegex(ValueError, "Negative"):
            build_features(bad)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            build_features(pd.concat([frame, frame.iloc[[0]]]))

    def test_split_groups_all_cities_by_target_date(self):
        dates = ["2021-12-31", "2022-01-01", "2022-12-31", "2023-01-01", "2024-12-31"]
        frame = pd.DataFrame({"target_date": dates * 2, "city_id": [1] * 5 + [2] * 5})
        train, validation, test = temporal_split(frame)
        self.assertEqual(set(train.target_date), {"2021-12-31"})
        self.assertEqual(set(validation.target_date), {"2022-01-01", "2022-12-31"})
        self.assertEqual(set(test.target_date), {"2023-01-01", "2024-12-31"})
        self.assertLess(train.target_date.max(), validation.target_date.min())
        self.assertLess(validation.target_date.max(), test.target_date.min())

    def test_cpcb_categories(self):
        self.assertEqual(aqi_category([0, 50, 51, 100, 150, 250, 350, 401]),
                         ["Good", "Good", "Satisfactory", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"])

    def test_forecastability_detects_no_persistence_in_random_series(self):
        rng = np.random.default_rng(0)
        frame = source_frame(days=600, start="2016-01-01")
        frame["aqi"] = rng.uniform(50, 450, len(frame))
        evidence = forecastability(frame)
        self.assertLess(evidence["max_abs_lag1_autocorrelation"], 0.15)
        trending = source_frame(days=600, start="2016-01-01")
        self.assertGreater(forecastability(trending)["max_abs_lag1_autocorrelation"], 0.9)


if __name__ == "__main__":
    unittest.main()
