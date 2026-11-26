from pathlib import Path

import redis

from . import config
from .domain import STATES, TicketState


class View:
    def __init__(self):
        self.db = redis.Redis.from_url(config.REDIS_URL, decode_responses=True,
                                       socket_connect_timeout=3, socket_timeout=5)
        self.keys = [f"{config.NAMESPACE}:{suffix}" for suffix in
                     ("next-offset", "tickets", "counts", "outcomes")]
        self.script = self.db.register_script(Path(__file__).with_name("project.lua").read_text())

    def checkpoint(self, partition: int) -> int | None:
        raw = self.db.hget(self.keys[0], str(partition))
        return int(raw) if raw is not None else None

    def apply(self, partition: int, offset: int, expected: int, event: TicketState | None) -> str:
        # Redis Lua numbers are doubles. Reject offsets outside the exact integer range.
        if not 0 <= offset < 2**53 - 1:
            raise ValueError("Offset outside projection range")
        fields = (event.ticket_id, event.version, event.queue, event.status) if event else ("", 0, "", "")
        return self.script(keys=self.keys, args=[partition, offset, expected, *fields])

    def queue(self, name: str) -> dict:
        values = self.db.hmget(self.keys[2], [f"{name}|{status}" for status in STATES])
        return {"queue": name, **{k: int(v or 0) for k, v in zip(STATES, values)}}

    def progress(self) -> dict:
        with self.db.pipeline() as pipe:
            pipe.hgetall(self.keys[0])
            pipe.hgetall(self.keys[3])
            offsets, outcomes = pipe.execute()
        return {"next_offsets": {p: int(v) for p, v in offsets.items()},
                "outcomes": {k: int(v) for k, v in outcomes.items()}}
