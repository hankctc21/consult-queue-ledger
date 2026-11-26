from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

QueueName = Annotated[str, Field(pattern=r"^[a-z][a-z0-9-]{0,31}$")]
Status = Literal["waiting", "active", "done"]
STATES = ("waiting", "active", "done")


class TicketState(BaseModel):
    """Versioned full state from an upstream ticket system."""
    model_config = ConfigDict(extra="forbid", strict=True)
    ticket_id: str = Field(pattern=r"^[A-Z][A-Z0-9-]{0,47}$")
    version: int = Field(ge=1, le=2_147_483_647)
    queue: QueueName
    status: Status


def start_offset(stored: int | None, low: int, high: int) -> int:
    position = 0 if stored is None else stored
    if not low <= position <= high:
        raise RuntimeError(f"Redis checkpoint {position} outside Kafka range [{low}, {high}]")
    return position
