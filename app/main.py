# subtitle: app/main.py
"""
Subtitle worker entrypoint.
Builds Kafka, S3, Storage HTTP, processing, worker. Runs the consume loop.
"""
import asyncio

import httpx

from app.Config.Config import config
from app.Config.Kafka import get_producer, make_consumer, close_producer
from app.Repositories import (
    EventConsumer,
    EventPublisher,
    S3Client,
    StorageClient,
)
from app.Services import SubtitleProcessingService, SubtitleWorkerService


async def main() -> None:
    print("Starting Subtitle Service...")

    producer = await get_producer()
    publisher = EventPublisher(producer)

    consumer = EventConsumer(
        make_consumer(
            topics=[config.TOPIC_SUBTITLE_TASKS],
            group_id=config.GROUP_SUBTITLE_WORKER,
        )
    )

    s3 = S3Client()
    http_client = httpx.AsyncClient(timeout=10.0)
    storage = StorageClient(http_client)
    processing = SubtitleProcessingService()

    worker = SubtitleWorkerService(
        consumer=consumer,
        publisher=publisher,
        s3=s3,
        storage=storage,
        processing=processing,
    )

    print("Subtitle Service ready.")
    try:
        await worker.run()
    except KeyboardInterrupt:
        pass
    finally:
        await http_client.aclose()
        await close_producer()
        print("Subtitle Service stopped.")


if __name__ == "__main__":
    asyncio.run(main())