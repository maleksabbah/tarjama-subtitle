# subtitle: app/Repositories/EventPublisher.py
"""Kafka producer wrapper."""
import json

from aiokafka import AIOKafkaProducer

from app.Config.Config import config


class EventPublisher:
    def __init__(self, producer: AIOKafkaProducer):
        self.producer = producer

    async def publish_completion(self, payload: dict) -> None:
        await self.producer.send_and_wait(
            config.TOPIC_COMPLETED,
            json.dumps(payload),
        )