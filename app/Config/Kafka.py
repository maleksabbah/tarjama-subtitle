# subtitle: app/Config/Kafka.py
"""
Kafka producer + consumer factories.
"""
from typing import Optional

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from app.Config.Config import config


_producer: Optional[AIOKafkaProducer] = None


async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
            acks="all",
            compression_type="gzip",
        )
        await _producer.start()
    return _producer


async def close_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


def make_consumer(
    topics: list[str],
    group_id: str,
    auto_offset_reset: str = "earliest",
) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
        value_deserializer=lambda v: v.decode("utf-8"),
    )