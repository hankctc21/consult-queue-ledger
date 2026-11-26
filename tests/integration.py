"""Use a fresh Kafka topic and isolated Redis keys on every run."""
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
            def send(data):
                reply = api.post("/events", json=data)
                assert reply.status_code == 202, reply.text
                return reply.json()

            def read(name):
                reply = api.get("/queues/" + name)
                assert reply.status_code == 200
                return reply.json()

            assert api.post("/events", json=base | {"version":0}).status_code == 422
            first = send(base)
            wait_until(worker, lambda: view.progress()["outcomes"] == {"applied":1})
            assert read("general")["waiting"] == 1
            # Same broker record redelivered after the Redis write.
            outcome = view.apply(first["partition"], first["offset"], first["offset"],
                                 TicketState(**base))
            assert outcome == "already_applied"
            stop(worker)
            # A stopped consumer does not prevent Kafka from accepting input.
            send(base)
            send(base | {"version":2,"status":"active"})
            send(base | {"version":1})
            send(base | {"version":2,"status":"done"})
            send(base | {"version":3,"queue":"claims","status":"active"})
            send(base | {"ticket_id":"CASE-99","queue":"claims","status":"done"})
            assert read("general")["waiting"] == 1
            malformed = Producer({"bootstrap.servers":config.BROKERS})
            malformed.produce(config.TOPIC, key="CASE-42", value=b"bad json")
            assert malformed.flush(10) == 0
            worker = launch()
            wait_until(worker, lambda: view.progress()["outcomes"] == expected)
            assert read("general") == dict(queue="general", waiting=0, active=0, done=0)
            assert read("claims") == dict(queue="claims", waiting=0, active=1, done=1)
            baseline = (read("general"), read("claims"), api.get("/progress").json())
            stop(worker)
            # Drop only this test run's four projection keys. Kafka is untouched.
            view.db.delete(*view.keys)
            assert view.progress() == {"next_offsets":{}, "outcomes":{}}
            worker = launch()
            wait_until(worker, lambda: view.progress()["outcomes"] == expected)
            assert (read("general"), read("claims"), api.get("/progress").json()) == baseline
            print("PASS: real Kafka/Redis, broker ack, repeated offset, duplicate, stale, conflict, "
                  "queue transfer, invalid record, worker restart, full rebuild with matching checkpoints")
            print("OUTCOMES", expected)
    finally:
        stop(worker)
        view.db.delete(*view.keys)
        view.db.close()
        from confluent_kafka.admin import AdminClient
        AdminClient({"bootstrap.servers":config.BROKERS}).delete_topics([config.TOPIC])[config.TOPIC].result(15)


if __name__ == "__main__":
    main()
