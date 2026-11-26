"""Use a fresh Kafka topic and isolated Redis keys on every run."""
import json
import os
import subprocess
import sys
import time
import uuid

run_id = uuid.uuid4().hex
os.environ["QUEUE_TOPIC"] = "consult-ci-" + run_id
os.environ["QUEUE_GROUP"] = "view-ci-" + run_id

from confluent_kafka import Producer
from fastapi.testclient import TestClient
from redis.exceptions import ResponseError

from queueboard import config
from queueboard.api import app
from queueboard.domain import TicketState
from queueboard.init_topic import create_topic
from queueboard.view import View


def launch():
    return subprocess.Popen([sys.executable, "-m", "queueboard.worker"])


def stop(worker):
    if worker.poll() is None:
        worker.kill()
    worker.wait(timeout=10)


def wait_until(worker, condition):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if worker.poll() is not None:
            raise AssertionError(f"Worker exited with {worker.returncode}")
        if condition():
            return
        time.sleep(.15)
    raise TimeoutError("Expected projection was not observed")


def main():
    create_topic()
    view = View()
    base = dict(ticket_id="CASE-42", version=1, queue="general", status="waiting")
    expected = dict(applied=4, duplicate=1, stale=1, conflict=1, invalid=1)
    worker = launch()
    try:
        with TestClient(app) as api:
            next_offsets = {}

            def track(partition, offset):
                key = str(partition)
                next_offsets[key] = max(next_offsets.get(key, 0), offset + 1)

            def send(data):
                reply = api.post("/events", json=data)
                assert reply.status_code == 202, reply.text
                receipt = reply.json()
                track(receipt["partition"], receipt["offset"])
                return receipt

            def read(name):
                reply = api.get("/queues/" + name)
                assert reply.status_code == 200
                return reply.json()

            def counts():
                return {name: read(name) for name in ("general", "claims")}

            assert api.post("/events", json=base | {"version":0}).status_code == 422
            first = send(base)
            wait_until(worker, lambda: view.progress()["outcomes"] == {"applied":1})
            assert read("general")["waiting"] == 1
            before_duplicate = counts()
            checkpoint = view.progress()
            # Same broker record redelivered after the Redis write.
            outcome = view.apply(first["partition"], first["offset"], first["offset"],
                                 TicketState(**base))
            assert outcome == "already_applied"
            assert (counts(), view.progress()) == (before_duplicate, checkpoint)
            # A stale writer cannot advance progress or change counts.
            try:
                view.apply(first["partition"], first["offset"] + 1, first["offset"],
                           TicketState(**(base | {"version":2, "status":"active"})))
            except ResponseError as error:
                assert "CHECKPOINT_CHANGED" in str(error)
            else:
                raise AssertionError("Stale checkpoint accepted")
            assert (counts(), view.progress()) == (before_duplicate, checkpoint)
            repeat = send(base)
            wait_until(worker, lambda: view.progress()["outcomes"] == {"applied":1, "duplicate":1})
            after_duplicate = counts()
            assert first["partition"] == repeat["partition"] and first["offset"] < repeat["offset"]
            assert after_duplicate == before_duplicate
            print("EVIDENCE_DUP " + json.dumps({"before":before_duplicate["general"],
                    "after":after_duplicate["general"], "receipts":[first,repeat],
                    "outcomes":view.progress()["outcomes"]}, sort_keys=True), flush=True)
            stop(worker)
            # A stopped consumer does not prevent Kafka from accepting input.
            send(base | {"version":2,"status":"active"})
            send(base | {"version":1})
            send(base | {"version":2,"status":"done"})
            send(base | {"version":3,"queue":"claims","status":"active"})
            send(base | {"ticket_id":"CASE-99","queue":"claims","status":"done"})
            assert read("general")["waiting"] == 1
            malformed = Producer({"bootstrap.servers":config.BROKERS})
            malformed_receipts = []
            malformed.produce(config.TOPIC, key="CASE-42", value=b"bad json",
                              on_delivery=lambda err, msg: malformed_receipts.append((err, msg)))
            assert malformed.flush(10) == 0
            error, record = malformed_receipts[0]
            assert error is None
            track(record.partition(), record.offset())
            worker = launch()
            wait_until(worker, lambda: view.progress()["outcomes"] == expected)
            assert read("general") == dict(queue="general", waiting=0, active=0, done=0)
            assert read("claims") == dict(queue="claims", waiting=0, active=1, done=1)
            def snapshot():
                return {"queues": counts(), "progress": api.get("/progress").json(),
                        "ticket_states": view.db.hgetall(view.keys[1])}

            baseline = snapshot()
            assert baseline["progress"]["next_offsets"] == next_offsets
            assert baseline["ticket_states"] == {"CASE-42":"3|claims|active", "CASE-99":"1|claims|done"}
            stop(worker)
            # Drop only this test run's four projection keys. Kafka is untouched.
            view.db.delete(*view.keys)
            assert view.progress() == {"next_offsets":{}, "outcomes":{}}
            empty = snapshot()
            assert empty["ticket_states"] == {}
            assert all(v == 0 for row in empty["queues"].values()
                       for k, v in row.items() if k != "queue")
            worker = launch()
            wait_until(worker, lambda: view.progress()["outcomes"] == expected)
            restored = snapshot()
            assert restored == baseline
            print("EVIDENCE_REPLAY " + json.dumps({"before":baseline, "cleared":empty, "after":restored},
                                                  sort_keys=True), flush=True)
            print("PASS: real Kafka/Redis, broker ack, repeated offset, duplicate, stale, conflict, "
                  "queue transfer, invalid record, worker restart, full rebuild with matching checkpoints")
            print("OUTCOMES", expected)
    finally:
        stop(worker)
        view.db.delete(*view.keys)
        view.db.close()
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({"bootstrap.servers": config.BROKERS})
        deletion = admin.delete_topics([config.TOPIC])
        deletion[config.TOPIC].result(timeout=30)


if __name__ == "__main__":
    main()
