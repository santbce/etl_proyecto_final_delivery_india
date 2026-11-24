# India air quality — final delivery

Samuel Flores · Santiago Becerra · ETL G51

Repository: [santbce/etl_proyecto_final_delivery_india](https://github.com/santbce/etl_proyecto_final_delivery_india).

This extends the [second delivery](README_PREVIOUS_DELIVERIES.md). It preserves the three sources (Kaggle air-quality measurements, Wikipedia city coordinates, Open-Meteo ERA5 weather), Airflow, Great Expectations and the PostgreSQL star schema. It adds an AQI estimation model (same-day AQI and CPCB category from the day's pollutant readings), real Kafka producer/consumer, a Python static report and a Streamlit dashboard of the live estimates.

The Kafka stream is a **historical replay**, not live sensor data. Model accuracy must be assessed separately from the correctness of the data architecture.

## Read and demonstrate

- [Technical report](docs/entrega_final.md)
- [Architecture diagram](docs/arquitectura_final.mmd)
- [Relationship to classroom materials](docs/material_clase.md)
- [Measured results and verification evidence](docs/resultados_final.md)
- [ML notebook](notebooks/03_modelo_predictivo.ipynb)
- [Streaming demonstration notebook](notebooks/04_demo_streaming.ipynb)
- Generated static report: `reports/final/report.html`
- Model, metadata and holdout predictions: `artifacts/`
- Previous EDA notebooks remain in `notebooks/`.

## Reproduce locally

Requirements: Docker Desktop with Linux containers; allocate adequate memory for Kafka and Airflow. Run from this repository directory. The development passwords below are for local demo services. Ports are bound to localhost.

`compose.yaml` uses the project name `india-final`. Another checkout using that name shares its containers and PostgreSQL volume. If another copy is already running, use a different Compose project name consistently with `-p` and override the host ports, or stop the earlier stack before starting this one.

```sh
docker compose up -d --wait postgres kafka
docker compose build tools
docker compose run --rm tools python src/final_setup.py
```

### Data

Raw data is not stored in this repository (see `.gitignore`). The three sources are obtained when the project runs:

1. **Air-quality measurements (first delivery):** `src/final_setup.py` downloads the public first-delivery repository ([santbce/etl-calidad-aire-india](https://github.com/santbce/etl-calidad-aire-india)), unpacks its SQLite database into `primera_entrega/` and verifies its SHA-256, so every run extracts exactly the same records. The original dataset is the Kaggle [AQI Air Quality Data India 2015–2024](https://www.kaggle.com/datasets/sathvikisikella/cleaned-air-quality-india). The DAG task `preparar_ejecucion` performs the same step automatically.
2. **City coordinates:** scraped from Wikipedia by the DAG.
3. **Weather:** requested from the Open-Meteo historical API by the DAG.

Downloaded and generated files (`data/raw/`, `data/*.zip`, `primera_entrega/`, SQLite databases and database dumps) stay local and are ignored by Git.

### Full extraction and Airflow path

This fetches the three sources and runs quality checks before loading. Wikipedia/Open-Meteo must be reachable. The final DAG includes training, the static report and a finite 100-event Kafka replay.

```sh
docker compose run --rm tools airflow db migrate
docker compose run --rm tools airflow dags test etl_india_final 2026-11-22
```

`dags test` is a real single-DAG execution, not evidence of unattended scheduling. To use the scheduler/webserver:

```sh
docker compose --profile airflow up -d airflow
```

Open http://localhost:8088. `airflow standalone` generates a local admin password in `runtime/airflow/standalone_admin_password.txt`. Keep that runtime file private. Unpause only `etl_india_final` if scheduled runs are desired; the preserved second-delivery DAG is paused by default. The final DAG is daily, with `catchup=False`, one active run, and no automatic retries for quality-contract failures.

### Offline snapshot alternative

If a source may be unavailable during a demonstration, save a local snapshot of the validated warehouse after a successful DAG run. The dump stays on your machine and is not committed:

```sh
docker compose exec -T postgres pg_dump -U india -d india > reports/warehouse.sql
```

To use it, restore it **once into a new, empty project database**, then re-run the quality checks before training. A restored snapshot is explicitly labelled; it is not presented as a fresh API extraction.

```sh
docker compose exec -T postgres psql -U india -d india -v ON_ERROR_STOP=1 -f /opt/project/reports/warehouse.sql
docker compose run --rm tools python src/final_snapshot.py --run-id snapshot_revalidation
docker compose run --rm tools python src/final_ml.py
docker compose run --rm tools python src/final_report.py
docker compose run --rm tools python src/final_stream.py create-topic
```

Do not restore a dump on top of a populated schema.

### Live inference demonstration

After either path has produced `artifacts/model.joblib` and `artifacts/metadata.json`:

```sh
docker compose run --rm tools python src/final_stream.py create-topic
docker compose --profile live up -d consumer dashboard
docker compose run --rm tools python src/final_stream.py produce --limit 100 --delay 0.5 --replay-id classroom-demo-1
```

Open http://localhost:8502. Keep it open while publishing: the counter, chart and rows change as the consumer processes messages. To run another replay, choose a new replay ID. Reusing the same replay ID and source run makes event IDs deterministic, so the consumer stores no duplicate prediction. After retraining, restart the consumer to load the new model once:

```sh
docker compose restart consumer
```

The consumer does not query the warehouse and never retrains. It validates schema/calendar/features, predicts once per unique event and durably writes SQLite before committing its Kafka offset. Malformed events are recorded in `rejected_events`. Unexpected inference/storage failures stop the consumer without acknowledging the message. Streamlit reads only the prediction store; it has no analytical database query.

### Verification and shutdown

```sh
docker compose run --rm tools python -m pytest tests/test_final_ml.py tests/test_final_stream.py tests/test_final_report.py -q
```

After a successful full DAG run, audit its saved task states, validation, source provenance and model artifacts:

```sh
docker compose run --rm tools python tests/audit_final.py
```

The audit requires the DAG's local execution files and Airflow metadata; restoring a database dump alone does not supply them. With a validated warehouse, model and running Kafka broker, the separate integration test checks delivery, inference, invalid messages and duplicate handling:

```sh
docker compose run --rm tools python tests/integration_final.py
docker compose run --rm tools python tests/integration_recovery.py
```

Inspect the consumer and stop the project services when finished:

```sh
docker compose logs consumer
docker compose --profile live --profile airflow down
```

The shutdown preserves the named PostgreSQL volume, project artifacts and prediction store. Kafka has no persistent volume in this demo: recreating its container removes its topics and offsets. Run the `create-topic` command above before restarting the live demonstration. The runtime directory and credentials are excluded from Git. Do not run `down -v` unless you intentionally want to discard the project's database.

## Host Python / Jupyter option

The final notebooks run in a **local Python 3.12 Jupyter kernel**, with the infrastructure from Compose. They do not claim that Docker is available inside Google Colab. The original second-delivery Colab notebook is preserved unchanged.

```sh
python -m venv .venv
# Activate .venv in your operating system, then:
python -m pip install -r requirements-final.txt
```

Set `INDIA_DATABASE_URL=postgresql://india:india_demo_local@localhost:55432/india`, `KAFKA_BOOTSTRAP_SERVERS=localhost:19092` and `PYTHONPATH` to the absolute `src` directory. CLI entry points are `src/final_ml.py`, `src/final_report.py`, `src/final_stream.py`, and `streamlit run app/dashboard.py`.

## Prediction contract

Use case: as soon as a city's daily pollutant readings arrive, estimate that day's AQI and its CPCB category (Good, Satisfactory, Moderate, Poor, Very Poor, Severe) without waiting for the official calculation.

Target: the numeric AQI of the same city and local day. Inputs: `city_name` and the nine pollutant readings of that day (`pm25`, `pm10`, `no_value`, `no2`, `nox`, `nh3`, `co`, `so2`, `o3`). The observed AQI and AQI category never enter events or input features.

Why not a next-day forecast: the first version forecast tomorrow's AQI and did not beat a constant-mean baseline. The training data show why: day-to-day AQI autocorrelation is close to zero in every city and monthly averages are almost flat, so past values carry no information about the next day. The evidence is stored in `artifacts/metadata.json` under `forecastability` and discussed in the ML notebook.

All cities share date cutoffs: train 2015–2021, validation 2022, test 2023–2024. Ridge, HistGradientBoosting and Random Forest are compared by validation MAE; the chosen pipeline is refitted through 2022 and evaluated on 2023–2024 against a constant-mean baseline and the linear model, with MAE, RMSE, R² and CPCB category accuracy. Missing/negative/non-finite readings are rejected. The model is saved with preprocessing, its SHA-256 and library versions.

Topic `india.aqi.features.v1` has one partition for the demonstration. Events (schema `india.aqi.nowcast.v1`) include schema version, deterministic UUID, simulation flag, replay ID, publication timestamp, the historical measurement date (`origin_date` = `target_date`), city/measurement/run provenance and the documented `features` object. See the technical report for the field definitions.

## Value and limits

The static report supports retrospective comparisons across cities/time and assesses model error. The operational path demonstrates per-message AQI estimation and auditable delivery of each estimate. The model estimates the current day's AQI; it is not an early-warning forecast. Kaggle provenance/units, India local-day assumptions and ERA5 reanalysis availability remain limitations, and a change in the official AQI formula or sensor calibration requires retraining. There is no causal interpretation and no claim of real-world deployment.
