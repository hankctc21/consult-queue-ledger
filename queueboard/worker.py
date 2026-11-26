import logging

from confluent_kafka import Consumer, KafkaException


from . import config
from .domain import TicketState, start_offset
from .view import View

log = logging.getLogger(__name__)


def run():
    view = View()
    consumer = Consumer({"bootstrap.servers": config.BROKERS, "group.id": config.GROUP,
                         "session.timeout.ms": 10000, "enable.auto.commit": False, "enable.auto.offset.store": False,
                         "partition.assignment.strategy": "range", "auto.offset.reset": "error"})
    expected = {}

    def assign(client, partitions):
        expected.clear()
        for part in partitions:
            low, high = client.get_watermark_offsets(part, timeout=10)
            part.offset = start_offset(view.checkpoint(part.partition), low, high)
            expected[part.partition] = part.offset
        client.assign(partitions)

    consumer.subscribe([config.TOPIC], on_assign=assign)
    try:
        while True:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            try:
                event = TicketState.model_validate_json(msg.value() or b"")
                if msg.key() != event.ticket_id.encode():
                    raise ValueError("Ticket key mismatch")
            except ValueError:
                event = None
            p = msg.partition()
            result = view.apply(p, msg.offset(), expected[p], event)
            expected[p] = msg.offset() + 1
            if result in ("conflict", "invalid"):
                log.warning("%s at partition=%d offset=%d", result, p, msg.offset())
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
