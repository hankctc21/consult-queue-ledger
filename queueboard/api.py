import time
from contextlib import asynccontextmanager

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Path, Request, status
from redis.exceptions import RedisError
from typing import Annotated

from . import config
from .domain import TicketState
from .view import View


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = Producer({"bootstrap.servers": config.BROKERS, "enable.idempotence": True,
                         "acks": "all", "message.timeout.ms": 5000})
    app.state.producer = producer
    app.state.view = View()
    try:
        yield
    finally:
        producer.flush(6)
        app.state.view.db.close()


app = FastAPI(title="Consult Queue Ledger", version="1.0", lifespan=lifespan)


@app.post("/events", status_code=status.HTTP_202_ACCEPTED)
def publish(event: TicketState, request: Request):
    delivery = []

    def acknowledged(error, message):
        delivery.append((error, message))

    producer = request.app.state.producer
    try:
        producer.produce(config.TOPIC, key=event.ticket_id, value=event.model_dump_json(),
                         on_delivery=acknowledged)
    except BufferError as exc:
        raise HTTPException(503, "Producer queue full") from exc
    deadline = time.monotonic() + 6
    while not delivery and time.monotonic() < deadline:
        producer.poll(0.1)
    if not delivery or delivery[0][0] is not None:
        raise HTTPException(503, "Kafka acknowledgement unavailable; retry the same ticket version")
    message = delivery[0][1]
    return {"ticket_id": event.ticket_id, "partition": message.partition(), "offset": message.offset()}


@app.get("/queues/{name}")
def queue(name: Annotated[str, Path(pattern=r"^[a-z][a-z0-9-]{0,31}$")], request: Request):
    try:
        return request.app.state.view.queue(name)
    except RedisError as exc:
        raise HTTPException(503, "Queue view unavailable") from exc


@app.get("/progress")
def progress(request: Request):
    try:
        return request.app.state.view.progress()
    except RedisError as exc:
        raise HTTPException(503, "Queue view unavailable") from exc
