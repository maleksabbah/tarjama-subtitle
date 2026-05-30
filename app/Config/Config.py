# subtitle: app/Config/Config.py
"""
Subtitle worker config.
"""
import os


class Config:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    # Topics
    TOPIC_SUBTITLE_TASKS: str = os.getenv("TOPIC_SUBTITLE_TASKS", "tarjama.subtitle.tasks")
    TOPIC_COMPLETED: str = os.getenv("TOPIC_COMPLETED", "tarjama.completed")

    # Consumer group
    GROUP_SUBTITLE_WORKER: str = os.getenv("GROUP_SUBTITLE_WORKER", "tarjama.subtitle")

    # Storage
    STORAGE_URL: str = os.getenv("STORAGE_URL", "http://storage:8002")

    # ffmpeg
    FFMPEG_PATH: str = os.getenv("FFMPEG_PATH", "ffmpeg")

    # Subtitle styling
    FONT_SIZE: int = int(os.getenv("SUBTITLE_FONT_SIZE", "24"))

    # Fallback if video_meta.json is missing
    DEFAULT_FPS: float = float(os.getenv("DEFAULT_FPS", "25.0"))


config = Config()