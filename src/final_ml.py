"""Same-day AQI estimation (nowcast) from the validated India dimensional warehouse.

The model receives the pollutant readings of one city and one local day and
estimates the AQI of that same city-day. Feature engineering is shared by
offline training and the Kafka producer; the observed AQI and AQI category are
never inference features.

Why not next-day forecasting: the exploratory analysis and the second delivery
showed that day-to-day AQI persistence in this dataset is practically zero
(lag-1 autocorrelation close to 0 in every city) and there is no seasonality,
so yesterday's values cannot predict tomorrow. The relationship between the
pollutants and the AQI of the same day is strong but non-linear (the CPCB AQI
is the maximum of piecewise sub-indices). That is the relationship this model
learns. The evidence is stored in the metadata under ``forecastability``.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

CITIES = ["Bangalore", "Chennai", "Delhi", "Kolkata", "Mumbai"]
# Pollutant readings of the observation day, named as in india_dw.fact_air_quality.
NUMERIC_FEATURES = ["pm25", "pm10", "no_value", "no2", "nox", "nh3", "co", "so2", "o3"]
FEATURE_COLUMNS = ["city_name"] + NUMERIC_FEATURES
# 0 = the estimated AQI belongs to the same local day as the readings.
HORIZON_DAYS = 0
SOURCE_MEASURES = ["aqi"] + NUMERIC_FEATURES
REQUIRED_COLUMNS = ["measurement_id", "city_id", "city_name", "date", "run_id"] + SOURCE_MEASURES
WAREHOUSE_SQL = """SELECT measurement_id, city_id, city_name, date, run_id,
    aqi, pm25, pm10, no_value, no2, nox, nh3, co, so2, o3
    FROM india_dw.air_quality_enriched v
    WHERE EXISTS (SELECT 1 FROM india_dw.etl_runs r WHERE r.run_id=v.run_id
                  AND (r.quality_result->>'success')::boolean IS TRUE)
    ORDER BY city_id, date"""

# Official CPCB AQI categories, used to translate an estimated AQI into a category.
AQI_BINS = [-np.inf, 50, 100, 200, 300, 400, np.inf]
AQI_LABELS = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"]


def aqi_category(values) -> list[str]:
    """Map AQI values to CPCB categories (0-50 Good ... >400 Severe)."""
    return pd.cut(pd.Series(values, dtype=float), bins=AQI_BINS, labels=AQI_LABELS).astype(str).tolist()


def _source_frame(df: pd.DataFrame) -> pd.DataFrame:
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Missing warehouse columns: {sorted(missing)}")
    if df.empty:
        raise ValueError("The validated warehouse is empty")
    source = df[REQUIRED_COLUMNS].copy()
    if source.isna().any().any():
        raise ValueError("Missing source values: repair the ETL before training or publishing")
    source["date"] = pd.to_datetime(source["date"], errors="raise")
    if source["date"].dt.tz is not None or not source["date"].eq(source["date"].dt.normalize()).all():
        raise ValueError("Source dates must be local calendar days without time or timezone")
    if source.duplicated(["city_id", "date"]).any() or source["measurement_id"].duplicated().any():
        raise ValueError("Duplicate source measurement or city/day")
    if not source["city_name"].isin(CITIES).all():
        raise ValueError("Unknown city; the model supports the five documented cities")
    if (source.groupby("city_id")["city_name"].nunique() != 1).any():
        raise ValueError("A city identifier maps to multiple names")
    if (source.groupby("city_name")["city_id"].nunique() != 1).any():
        raise ValueError("A city name maps to multiple identifiers")
    for name in SOURCE_MEASURES:
        source[name] = pd.to_numeric(source[name], errors="raise")
        if not np.isfinite(source[name].to_numpy(dtype=float)).all():
            raise ValueError(f"Non-finite source values in {name}")
    if (source[SOURCE_MEASURES] < 0).any().any():
        raise ValueError("Negative AQI or pollutant value")
    return source.sort_values(["city_id", "date"]).reset_index(drop=True)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Build one inference row per city-day from that day's pollutant readings.

    ``origin_date`` is the day of the readings and ``target_date`` the day whose
    AQI is estimated; with HORIZON_DAYS = 0 both are the same local day. The
    observed AQI and its category are not copied into the features.
    """
    source = _source_frame(df)
    day = source["date"].dt.strftime("%Y-%m-%d")
    features = source[["measurement_id", "city_id", "run_id"]].copy()
    features["origin_date"] = day
    features["target_date"] = (source["date"] + pd.Timedelta(days=HORIZON_DAYS)).dt.strftime("%Y-%m-%d")
    features[FEATURE_COLUMNS] = source[FEATURE_COLUMNS]
    return features.sort_values(["target_date", "city_id"]).reset_index(drop=True)


def training_labels(df: pd.DataFrame) -> pd.DataFrame:
    """Attach the observed AQI of the same city-day for supervised learning only."""
    source = _source_frame(df)
    labels = source[["measurement_id", "aqi"]].rename(columns={"aqi": "target_aqi"})
    return build_features(source).merge(
        labels, on="measurement_id", how="inner", validate="one_to_one"
    ).sort_values(["target_date", "city_id"]).reset_index(drop=True)


def temporal_split(labeled: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fixed, chronological target-day partitions; all cities share cutoffs."""
    dates = pd.to_datetime(labeled["target_date"], errors="raise")
    train_frame = labeled.loc[dates.between("2015-01-01", "2021-12-31")].copy()
    validation = labeled.loc[dates.between("2022-01-01", "2022-12-31")].copy()
    test = labeled.loc[dates.between("2023-01-01", "2024-12-31")].copy()
    if any(part.empty for part in [train_frame, validation, test]):
        raise ValueError("Training, validation and test periods must all contain observations")
    return train_frame, validation, test


def forecastability(df: pd.DataFrame) -> dict:
    """Evidence on whether next-day forecasting is supported by the data.

    Computed on the training period only (2015-2021): lag-1 autocorrelation of
    AQI per city and the spread of monthly mean AQI (seasonality).
    """
    source = _source_frame(df)
    period = source.loc[source["date"].between("2015-01-01", "2021-12-31")]
    autocorrelation = {
        city: float(group.sort_values("date")["aqi"].autocorr(1))
        for city, group in period.groupby("city_name")
    }
    monthly = period.groupby(period["date"].dt.month)["aqi"].mean()
    return {
        "period": "2015-01-01 to 2021-12-31 (training data only)",
        "aqi_lag1_autocorrelation_by_city": autocorrelation,
        "max_abs_lag1_autocorrelation": float(max(abs(v) for v in autocorrelation.values())),
        "monthly_mean_aqi_range": float(monthly.max() - monthly.min()),
        "conclusion": ("Next-day AQI is not predictable from past values in this dataset: persistence "
                       "is close to zero and there is no seasonal pattern. The model therefore estimates "
                       "the AQI of the same day from that day's pollutant readings."),
    }


def read_warehouse(database_url: str) -> pd.DataFrame:
    import psycopg

    if not database_url:
        raise ValueError("Set INDIA_DATABASE_URL or pass --database-url")
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(WAREHOUSE_SQL)
            return pd.DataFrame(cursor.fetchall(), columns=[c.name for c in cursor.description])


def _scores(actual, predicted) -> dict:
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    return {
        "mae": float(mean_absolute_error(actual, predicted)),
        "rmse": float(np.sqrt(mean_squared_error(actual, predicted))),
        "r2": float(r2_score(actual, predicted)),
        "category_accuracy": float(np.mean(np.array(aqi_category(actual)) == np.array(aqi_category(predicted)))),
    }


def _pipeline(estimator):
    from sklearn.compose import ColumnTransformer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    # The ETL and event contract reject missing data. No imputation masks errors.
    preprocessor = ColumnTransformer([
        ("numeric", StandardScaler(), NUMERIC_FEATURES),
        ("city", OneHotEncoder(handle_unknown="error", sparse_output=False), ["city_name"]),
    ], remainder="drop")
    return Pipeline([("preprocessor", preprocessor), ("regressor", estimator)])


def _period(part: pd.DataFrame) -> dict:
    return {"rows": int(len(part)), "start": str(part.target_date.min()), "end": str(part.target_date.max())}


def candidate_models() -> dict:
    from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
    from sklearn.linear_model import Ridge

    return {
        "ridge": _pipeline(Ridge(alpha=10.0)),
        "hist_gradient_boosting": _pipeline(HistGradientBoostingRegressor(
            max_iter=500, learning_rate=0.1, max_leaf_nodes=31, random_state=42
        )),
        "random_forest": _pipeline(RandomForestRegressor(
            n_estimators=60, min_samples_leaf=3, random_state=42, n_jobs=-1
        )),
    }


def train(database_url: str, outdir: str | Path) -> dict:
    """Select on 2022 only, refit through 2022, then evaluate untouched 2023-24."""
    import joblib
    from sklearn.base import clone

    frame = read_warehouse(database_url)
    labeled = training_labels(frame)
    training, validation, test = temporal_split(labeled)
    if set(training.city_name.unique()) != set(CITIES):
        raise ValueError("The training warehouse must include all five documented cities")
    candidates = candidate_models()
    validation_scores = {}
    for name, candidate in candidates.items():
        candidate.fit(training[FEATURE_COLUMNS], training.target_aqi)
        validation_scores[name] = _scores(
            validation.target_aqi, candidate.predict(validation[FEATURE_COLUMNS])
        )
    validation_scores["mean_baseline"] = _scores(
        validation.target_aqi, np.full(len(validation), training.target_aqi.mean())
    )
    selected_name = min(candidates, key=lambda name: validation_scores[name]["mae"])
    selected = clone(candidates[selected_name])
    development = pd.concat([training, validation], ignore_index=True)
    selected.fit(development[FEATURE_COLUMNS], development.target_aqi)
    predictions = selected.predict(test[FEATURE_COLUMNS])
    reference_mean = float(development.target_aqi.mean())
    linear = clone(candidates["ridge"]).fit(development[FEATURE_COLUMNS], development.target_aqi)
    test_scores = {
        "selected_model": _scores(test.target_aqi, predictions),
        "linear_reference": _scores(test.target_aqi, linear.predict(test[FEATURE_COLUMNS])),
        "mean_baseline": _scores(test.target_aqi, np.full(len(test), reference_mean)),
    }
    output = Path(outdir)
    output.mkdir(parents=True, exist_ok=True)
    temporary_model = output / "model.pending.joblib"
    joblib.dump(selected, temporary_model, compress=3)
    model_path = output / "model.joblib"
    temporary_model.replace(model_path)
    model_sha = hashlib.sha256(model_path.read_bytes()).hexdigest()
    metadata = {
        "model_version": model_sha[:16],
        "model_sha256": model_sha,
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "model_name": selected_name,
        "target": "AQI of the same city and local day, estimated from that day's pollutant readings (nowcast)",
        "horizon_days": HORIZON_DAYS,
        "use_case": ("Give an immediate AQI value and CPCB category as soon as a city's daily pollutant "
                     "readings arrive, without waiting for the official AQI calculation and publication."),
        "feature_columns": FEATURE_COLUMNS,
        "numeric_features": NUMERIC_FEATURES,
        "cities": CITIES,
        "aqi_categories": dict(zip(AQI_LABELS, ["0-50", "51-100", "101-200", "201-300", "301-400", ">400"])),
        "source": "PostgreSQL india_dw.air_quality_enriched (validated dimensional warehouse)",
        "source_rows": int(len(frame)),
        "labeled_rows": int(len(labeled)),
        "dataset_sha256": hashlib.sha256(labeled.to_csv(index=False).encode("utf-8")).hexdigest(),
        "source_run_ids": sorted(frame.run_id.astype(str).unique().tolist()),
        "split": {"train": _period(training), "validation": _period(validation), "test": _period(test)},
        "fit_through": str(development.target_date.max()),
        "selection_rule": ("Lowest validation MAE among Ridge, HistGradientBoosting and RandomForest; "
                           "test never selects the model"),
        "selection_validation_mae": validation_scores[selected_name]["mae"],
        "metrics": {"validation": validation_scores, "test": test_scores},
        "baseline_mean_aqi": reference_mean,
        "beats_mean_baseline_on_test": test_scores["selected_model"]["mae"] < test_scores["mean_baseline"]["mae"],
        "by_city_test": {
            city: _scores(test.loc[test.city_name.eq(city), "target_aqi"], predictions[test.city_name.eq(city).to_numpy()])
            for city in CITIES
        },
        "forecastability": forecastability(frame),
        "preprocessing": {
            "numeric": "StandardScaler fitted on the development partition only",
            "categorical": "OneHotEncoder fitted on city_name; unknown cities are rejected",
            "missing": "Missing/non-finite source values are rejected; no hidden imputation",
            "leakage": "The observed AQI and AQI category of the day are never inference features",
        },
        "limitations": [
            "Academic AQI estimation experiment, not a validated public warning system.",
            "The model reproduces the relationship between pollutant readings and AQI learned from 2015-2022; "
            "if the official AQI formula or sensor calibration changes, it must be retrained.",
            "It estimates the current day's AQI; it is not a forecast of future air quality.",
            "Next-day forecasting was evaluated in the exploratory analysis and rejected: the data show "
            "practically no day-to-day persistence or seasonality (see forecastability).",
            "The original Kaggle source does not establish measurement units or station provenance for every pollutant.",
            "Daily timestamps are assumed to represent local days in India, consistent with the earlier ETL.",
            "The stream replays historical validated observations; event creation time is distinct from the historical observation date.",
        ],
        "versions": {name: importlib.metadata.version(name) for name in ["scikit-learn", "pandas", "numpy", "joblib"]},
    }
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    result = test[["city_id", "city_name", "origin_date", "target_date", "target_aqi"]].copy()
    result["predicted_aqi"] = predictions
    result["actual_category"] = aqi_category(result.target_aqi)
    result["predicted_category"] = aqi_category(result.predicted_aqi)
    result["mean_baseline"] = reference_mean
    result["error"] = result.predicted_aqi - result.target_aqi
    result.to_csv(output / "test_predictions.csv", index=False)
    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("INDIA_DATABASE_URL"))
    parser.add_argument("--output", default="artifacts")
    args = parser.parse_args()
    metadata = train(args.database_url, args.output)
    print(json.dumps({"model_name": metadata["model_name"], "model_version": metadata["model_version"],
                      "test": metadata["metrics"]["test"]}, indent=2))


if __name__ == "__main__":
    main()
