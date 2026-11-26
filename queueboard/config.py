import os

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("QUEUE_TOPIC", "consultation-state-v1")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
GROUP = os.getenv("QUEUE_GROUP", "consult-queue-view-v1")
# Each independent replay uses its own consumer group and Redis namespace.
NAMESPACE = f"{{queueboard:{TOPIC}:{GROUP}}}"
PARTITIONS = 3
