"""
Subtitle Service Configuration
"""
import os


class Config:
    # Redis
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Queues
    QUEUE_SUBTITLE = "queue:subtitle"
    QUEUE_COMPLETED = "queue:completed"

    # Storage
    STORAGE_BASE = os.getenv("STORAGE_BASE", "./storage")
    RESULTS_DIR = os.getenv("RESULTS_DIR", "results")

    # FFmpeg
    FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")

    # Subtitle defaults
    FONT_SIZE = int(os.getenv("SUBTITLE_FONT_SIZE", "24"))
    FONT_COLOR = os.getenv("SUBTITLE_FONT_COLOR", "white")
    OUTLINE_COLOR = os.getenv("SUBTITLE_OUTLINE_COLOR", "black")


config = Config()