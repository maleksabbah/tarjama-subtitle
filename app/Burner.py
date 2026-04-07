"""
Subtitle Burner
Burns subtitles onto video using FFmpeg.
"""
import os
import subprocess
from app.Config import config


def burn_subtitles(video_path: str, srt_path: str, output_path: str) -> str:
    """Burn SRT subtitles onto video using FFmpeg subtitles filter."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Escape special characters in path for FFmpeg filter
    escaped_srt = srt_path.replace("\\", "/").replace(":", "\\:")

    cmd = [
        config.FFMPEG_PATH,
        "-i", video_path,
        "-vf", f"subtitles='{escaped_srt}':force_style="
               f"'FontSize={config.FONT_SIZE},"
               f"PrimaryColour=&H00FFFFFF,"
               f"OutlineColour=&H00000000,"
               f"Outline=2,"
               f"MarginV=30'",
        "-c:a", "copy",
        "-y",
        output_path,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg subtitle burn failed: {result.stderr}")

    return output_path