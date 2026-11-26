from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from . import config


def create_topic():
    admin = AdminClient({"bootstrap.servers": config.BROKERS})
    definition = NewTopic(config.TOPIC, num_partitions=config.PARTITIONS, replication_factor=1,
                          config={"cleanup.policy": "delete", "retention.ms": "-1", "retention.bytes": "-1"})
    try:
        admin.create_topics([definition])[config.TOPIC].result(timeout=30)
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise


if __name__ == "__main__":
    create_topic()
