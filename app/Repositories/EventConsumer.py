# subtitle: app/Repositories/EventConsumer.py
"""Kafka consumer wrapper."""
import json
from typing import AsyncIterator

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord


class EventConsumer:
    def __init__(self, consumer: AIOKafkaConsumer):
        self.consumer = consumer

    async def start(self) -> None:
        await self.consumer.start()

    async def stop(self) -> None:
        await self.consumer.stop()

    async def messages(self) -> AsyncIterator[dict]:
        async for record in self.consumer:
            yield self._decode(record)

    async def commit(self) -> None:
        await self.consumer.commit()

    @staticmethod
    def _decode(record: ConsumerRecord) -> dict:
        raw = record.value
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)