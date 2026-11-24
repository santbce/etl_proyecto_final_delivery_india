"""Verify real Kafka consumer restart, offline parity and artifact integrity.

Run directly after the final Airflow DAG and model training. The test creates a
random india.recovery.* topic and private runtime stores. It does not write to
the analytical warehouse or alter the trained model.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import uuid

import joblib
import numpy as np
import pandas as pd
import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from final_ml import FEATURE_COLUMNS, build_features
from final_stream import event_from_row, read_warehouse, validate_event


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def check(condition, explanation):
    if not condition:
        raise AssertionError(explanation)


def warehouse_fingerprint(database_url):
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute("""SELECT count(*), md5(string_agg(row_to_json(v)::text, E'\\n'
                              ORDER BY measurement_id))
                              FROM india_dw.air_quality_enriched v""")
            rows, digest = cursor.fetchone()
    return {"rows": int(rows), "md5": digest}


def committed_offset(bootstrap, group, topic):
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group,
                         "enable.auto.commit": False})
    try:
        offset = consumer.committed([TopicPartition(topic, 0)], timeout=15)[0].offset
    finally:
        consumer.close()
    return offset


def stored_rows(store):
    with sqlite3.connect(store) as connection:
        predictions = connection.execute("SELECT * FROM predictions ORDER BY kafka_offset").fetchall()
        rejected = connection.execute("SELECT * FROM rejected_events ORDER BY kafka_offset").fetchall()
    return predictions, rejected


def final_summary(log_path):
    summaries = []
    for line in log_path.read_text(encoding="utf-8").splitlines():
        try:
            candidate = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict) and all(k in candidate for k in ["predicted", "duplicate", "rejected"]):
            summaries.append(candidate)
    check(bool(summaries), "Consumer did not emit a final processing summary")
    return summaries[-1]


def run_recovery(database_url, bootstrap="localhost:19092", artifacts="artifacts",
                 output="evidencias/final/recovery.json"):
    token = uuid.uuid4().hex
    topic, group = f"india.recovery.{token}", f"india-recovery-{token}"
    artifacts = (ROOT / artifacts).resolve()
    runtime = ROOT / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    store = runtime / f"recovery_{token}.sqlite"
    bad_artifacts = runtime / f"bad_{token}"
    bad_store = runtime / f"bad_{token}.sqlite"
    processes = []
    admin = AdminClient({"bootstrap.servers": bootstrap})
    topic_created = False
    failure = None
    result = {"success": False, "status": False, "started_at_utc": utcnow(),
              "topic": topic, "group": group, "simulation": True,
              "consumer_warehouse_credentials_present": False, "warehouse_write_operations": 0,
              "commands": ["python tests/integration_recovery.py",
                           "python src/final_stream.py consume --max-messages 2 --idle-timeout 15",
                           "python src/final_stream.py consume --max-messages 3 --idle-timeout 15",
                           "python src/final_stream.py consume --artifacts runtime/<mutated-copy>"],
              "prediction_store": str(store.relative_to(ROOT)), "logs": {}}
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + environment.get("PYTHONPATH", "")
    environment["PYTHONUNBUFFERED"] = "1"
    environment.pop("INDIA_DATABASE_URL", None)

    def run_consumer(phase, limit, artifact_dir=artifacts, store_path=store):
        log_path = runtime / f"recovery_{token}_{phase}.log"
        result["logs"][phase] = str(log_path.relative_to(ROOT))
        command = [sys.executable, "src/final_stream.py", "consume", "--bootstrap", bootstrap,
                   "--topic", topic, "--group", group, "--artifacts", str(artifact_dir),
                   "--store", str(store_path), "--max-messages", str(limit), "--idle-timeout", "15"]
        with log_path.open("w", encoding="utf-8") as log:
            process = subprocess.Popen(command, cwd=ROOT, env=environment, stdout=log,
                                       stderr=subprocess.STDOUT, text=True)
            processes.append(process)
            try:
                code = process.wait(timeout=60)
            except subprocess.TimeoutExpired as exc:
                process.kill(); process.wait(timeout=10)
                raise AssertionError(f"{phase} consumer did not finish within 60 seconds") from exc
        return code, log_path

    producer = None

    def send_payloads(payloads, expected_offsets):
        delivered_offsets, errors = [], []

        def delivered(error, message):
            if error:
                errors.append(str(error))
            else:
                delivered_offsets.append(message.offset())

        for payload in payloads:
            producer.produce(topic, partition=0, key="recovery", value=payload, on_delivery=delivered)
            producer.poll(0)
        pending = producer.flush(35)
        check(pending == 0 and not errors, "Kafka did not acknowledge every recovery-test message")
        check(sorted(delivered_offsets) == expected_offsets, "Unexpected delivery offsets on the isolated topic")

    try:
        result["warehouse_before"] = warehouse_fingerprint(database_url)
        metadata = json.loads((artifacts / "metadata.json").read_text(encoding="utf-8"))
        original_hash = hashlib.sha256((artifacts / "model.joblib").read_bytes()).hexdigest()
        check(original_hash == metadata["model_sha256"], "The original trained artifact failed its integrity check")
        model = joblib.load(artifacts / "model.joblib")
        features = build_features(read_warehouse(database_url))
        sample = features.loc[(features.target_date >= "2023-01-01") &
                              (features.target_date <= "2023-12-31")].head(4)
        check(len(sample) == 4, "Four held-out 2023 replay rows are required")
        events = [validate_event(event_from_row(row, token)) for row in sample.to_dict("records")]
        check(all(e["target_date"] > str(metadata["fit_through"])[:10] for e in events),
              "Replay records must occur after the fitted training period")
        check(all("target_aqi" not in e and "target_aqi" not in e["features"] for e in events),
              "Events must exclude the target value")
        offline = np.asarray(model.predict(pd.DataFrame([e["features"] for e in events], columns=FEATURE_COLUMNS)), dtype=float)
        saved_predictions = pd.read_csv(artifacts / "test_predictions.csv")
        check(not saved_predictions.duplicated(["city_name", "target_date"]).any(), "Offline evaluation has duplicate city/date keys")
        lookup = saved_predictions.set_index(["city_name", "target_date"])["predicted_aqi"]
        csv_predictions = np.array([lookup.loc[(e["features"]["city_name"], e["target_date"])] for e in events], dtype=float)
        np.testing.assert_allclose(offline, csv_predictions, rtol=1e-12, atol=1e-12)
        raw_events = [json.dumps(event, allow_nan=False).encode("utf-8") for event in events]
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result(30)
        topic_created = True
        producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                             "acks": "all", "delivery.timeout.ms": 30000,
                             "client.id": f"india-recovery-{token}"})

        # First process persists two results and commits through offset 1.
        send_payloads(raw_events[:2], [0, 1])
        code, log = run_consumer("phase1", 2)
        check(code == 0, "First consumer process failed; inspect its runtime log")
        phase1 = final_summary(log)
        counts1 = {k: phase1[k] for k in ["predicted", "duplicate", "rejected"]}
        check(counts1 == {"predicted": 2, "duplicate": 0, "rejected": 0}, "First process did not consume exactly the two initial events")
        first_rows, first_rejections = stored_rows(store)
        check(len(first_rows) == 2 and not first_rejections, "First process did not persist exactly two predictions")
        offset1 = committed_offset(bootstrap, group, topic)
        check(offset1 == 2, "First consumer must commit offset 2")
        result["phase1"] = {"counts": counts1, "committed_offset": offset1,
                            "consumer_pid": processes[-1].pid, "exit_code": code}

        # A new process uses the SAME group and SAME store. The malformed UTF-8
        # message is quarantined, then valid inference resumes at offsets 3,4.
        send_payloads([b"\xff"] + raw_events[2:], [2, 3, 4])
        code, log = run_consumer("phase2", 3)
        check(code == 0, "Restarted consumer process failed; inspect its runtime log")
        phase2 = final_summary(log)
        counts2 = {k: phase2[k] for k in ["predicted", "duplicate", "rejected"]}
        check(counts2 == {"predicted": 2, "duplicate": 0, "rejected": 1}, "Restart did not resume at the committed offset and recover past corrupt UTF-8")
        all_rows, rejections = stored_rows(store)
        check(len(all_rows) == 4 and len(rejections) == 1, "Restarted store must contain four predictions and one rejection")
        check(all_rows[:2] == first_rows, "Restart changed the two previously persisted prediction records")
        # predictions columns: event_id,replay_id,city_name,origin_date,target_date,
        # predicted_aqi,model_version,processed_at,published_at,topic,partition,offset,event_json.
        check([r[11] for r in all_rows] == [0, 1, 3, 4], "Unexpected valid-message offsets in recovered store")
        check(rejections[0][0:3] == (topic, 0, 2), "Corrupt message was not quarantined at offset 2")
        check(len({r[0] for r in all_rows}) == 4, "Recovery store contains duplicate prediction IDs")
        check([r[0] for r in all_rows] == [e["event_id"] for e in events], "Stored prediction IDs differ from replay events")
        check(all(r[6] == metadata["model_version"] and r[9] == topic and r[10] == 0 for r in all_rows), "Stored prediction provenance mismatch")
        for row, event in zip(all_rows, events):
            check(json.loads(row[12]) == event, "Stored event differs from the replay input")
        live_predictions = np.asarray([r[5] for r in all_rows], dtype=float)
        np.testing.assert_allclose(live_predictions, offline, rtol=1e-12, atol=1e-12)
        np.testing.assert_allclose(live_predictions, csv_predictions, rtol=1e-12, atol=1e-12)
        offset2 = committed_offset(bootstrap, group, topic)
        check(offset2 == 5, "Restarted consumer must commit through all five outcomes")
        result["phase2"] = {"counts": counts2, "committed_offset": offset2,
                            "consumer_pid": processes[-1].pid, "exit_code": code}
        check(result["phase1"]["consumer_pid"] != result["phase2"]["consumer_pid"], "Recovery requires a separate consumer process")
        result["restart"] = {"same_group": True, "same_store": True,
                             "previous_records_unchanged": True, "valid_offsets": [0, 1, 3, 4],
                             "rejected_offsets": [2], "total_predictions": 4, "total_rejected": 1}
        result["offline_live_parity"] = [
            {"city_name": e["features"]["city_name"], "target_date": e["target_date"],
             "offline_model": float(p), "offline_csv": float(c), "consumer_prediction": float(l),
             "event_id": e["event_id"]}
            for e, p, c, l in zip(events, offline, csv_predictions, live_predictions)
        ]

        # Tamper only with a private copy. The consumer must reject its hash
        # before joblib deserialization, Kafka subscription or store creation.
        bad_artifacts.mkdir()
        shutil.copy2(artifacts / "metadata.json", bad_artifacts / "metadata.json")
        shutil.copy2(artifacts / "model.joblib", bad_artifacts / "model.joblib")
        mutated = bytearray((bad_artifacts / "model.joblib").read_bytes())
        check(bool(mutated), "The copied model artifact must not be empty")
        mutated[-1] ^= 1
        (bad_artifacts / "model.joblib").write_bytes(mutated)
        check(not bad_store.exists(), "The bad-artifact store path must be new")
        code, log = run_consumer("bad_artifact", 1, bad_artifacts, bad_store)
        check(code != 0, "Consumer accepted a mutated model artifact")
        check("Model artifact hash differs from training metadata" in log.read_text(encoding="utf-8"),
              "Consumer failed for an unexpected reason instead of the artifact hash check")
        check(not bad_store.exists(), "Consumer opened its result store before checking artifact integrity")
        check(hashlib.sha256((artifacts / "model.joblib").read_bytes()).hexdigest() == original_hash,
              "The original model artifact changed during the tamper test")
        result["artifact_integrity"] = {"mutated_copy_rejected": True, "exit_code": code,
                                        "no_store_created": True, "original_model_unchanged": True,
                                        "original_sha256": original_hash,
                                        "mutated_copy_sha256": hashlib.sha256(mutated).hexdigest()}
        result["warehouse_after"] = warehouse_fingerprint(database_url)
        check(result["warehouse_after"] == result["warehouse_before"], "Recovery testing changed the analytical dataset")
        result["warehouse_unchanged"] = True
        result["model_version"] = metadata["model_version"]
        result["success"] = result["status"] = True
    except Exception as exc:
        failure = exc
        result["failure_type"] = type(exc).__name__
        result["failure"] = "Recovery assertion or infrastructure operation failed; inspect the recorded local logs and terminal output."
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait(timeout=10)
        if topic_created:
            try:
                check(topic == f"india.recovery.{token}", "Unsafe recovery topic cleanup target")
                admin.delete_topics([topic], operation_timeout=15)[topic].result(30)
                result["test_topic_deleted"] = True
            except Exception:
                result["test_topic_deleted"] = False
        result["completed_at_utc"] = utcnow()
        output_path = ROOT / output
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    if failure is not None:
        raise failure
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    parser.add_argument("--database-url", default=os.getenv("INDIA_DATABASE_URL"))
    parser.add_argument("--artifacts", default="artifacts")
    parser.add_argument("--output", default="evidencias/final/recovery.json")
    args = parser.parse_args()
    if not args.database_url:
        parser.error("Set INDIA_DATABASE_URL or provide --database-url")
    result = run_recovery(args.database_url, args.bootstrap, args.artifacts, args.output)
    print(json.dumps({"success": result["success"], "restart": result["restart"],
                      "warehouse_unchanged": result["warehouse_unchanged"], "evidence": args.output}, indent=2))


if __name__ == "__main__":
    main()
