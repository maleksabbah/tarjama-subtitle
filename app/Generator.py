"""
Subtitle Generator
Merges chunk transcription JSONs into SRT and VTT subtitle files.
"""
import os
import re
import json
import glob

CHUNK_DURATION = 30.0  # seconds per chunk (must match media worker chunker)


def _chunk_index_from_filename(path: str) -> int:
    """Extract chunk index from a filename like 'chunk_0042.json'."""
    m = re.search(r"chunk_(\d+)", os.path.basename(path))
    return int(m.group(1)) if m else 0


def load_chunk_results(results_dir: str) -> list[dict]:
    """Load all chunk JSON results and merge into a single absolute timeline."""
    chunk_files = sorted(glob.glob(os.path.join(results_dir, "chunk_*.json")))
    if not chunk_files:
        raise RuntimeError(f"No chunk results found in {results_dir}")

    all_segments = []
    for chunk_file in chunk_files:
        idx = _chunk_index_from_filename(chunk_file)
        offset = idx * CHUNK_DURATION

        with open(chunk_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        segments = data.get("segments", [])
        if not segments and data.get("text"):
            segments = [{"start": 0.0, "end": CHUNK_DURATION, "text": data["text"]}]

        # Offset every segment by the chunk's position in the full audio
        for seg in segments:
            text = (seg.get("text") or "").strip()
            if not text:
                continue
            start = float(seg.get("start", 0.0)) + offset
            end = float(seg.get("end", CHUNK_DURATION)) + offset
            if end <= start:
                end = start + CHUNK_DURATION
            all_segments.append({"start": start, "end": end, "text": text})

    return all_segments


def merge_transcript(segments: list[dict]) -> dict:
    """Merge all segments into a final transcript with full text."""
    full_text = " ".join(seg.get("text", "") for seg in segments if seg.get("text"))
    return {
        "text": full_text.strip(),
        "segments": segments,
        "total_segments": len(segments),
    }


def format_timestamp_srt(seconds: float) -> str:
    """Convert seconds to SRT timestamp format: HH:MM:SS,mmm"""
    if seconds < 0:
        seconds = 0
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int(round((seconds - int(seconds)) * 1000))
    if millis >= 1000:
        millis = 0
        secs += 1
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


def format_timestamp_vtt(seconds: float) -> str:
    """Convert seconds to VTT timestamp format: HH:MM:SS.mmm"""
    if seconds < 0:
        seconds = 0
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int(round((seconds - int(seconds)) * 1000))
    if millis >= 1000:
        millis = 0
        secs += 1
    return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"


def generate_srt(segments: list[dict], output_path: str) -> str:
    """Generate SRT subtitle file from segments."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    lines = []
    for i, seg in enumerate(segments, 1):
        text = seg.get("text", "").strip()
        if not text:
            continue

        start = format_timestamp_srt(seg.get("start", 0.0))
        end = format_timestamp_srt(seg.get("end", 0.0))

        lines.append(str(i))
        lines.append(f"{start} --> {end}")
        lines.append(text)
        lines.append("")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return output_path


def generate_vtt(segments: list[dict], output_path: str) -> str:
    """Generate WebVTT subtitle file from segments."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    lines = ["WEBVTT", ""]
    for i, seg in enumerate(segments, 1):
        text = seg.get("text", "").strip()
        if not text:
            continue
        start = format_timestamp_vtt(seg.get("start", 0.0))
        end = format_timestamp_vtt(seg.get("end", 0.0))
        lines.append(f"{start} --> {end}")
        lines.append(text)
        lines.append("")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return output_path


def save_transcript(transcript: dict, output_path: str) -> str:
    """Save merged transcript as JSON."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(transcript, f, ensure_ascii=False, indent=2)
    return output_path


