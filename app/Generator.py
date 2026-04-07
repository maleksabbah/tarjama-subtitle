"""
Subtitle Generator
Merges chunk transcription JSONs into SRT and VTT subtitle files.
"""
import os
import json
import glob


def load_chunk_results(results_dir: str) -> list[dict]:
    """Load all chunk JSON results and merge into a single timeline."""
    chunk_files = sorted(glob.glob(os.path.join(results_dir, "chunk_*.json")))
    if not chunk_files:
        raise RuntimeError(f"No chunk results found in {results_dir}")

    all_segments = []
    for chunk_file in chunk_files:
        with open(chunk_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        segments = data.get("segments", [])
        if not segments and data.get("text"):
            segments = [{"start":0.0,"end":0.0,"text":data["text"]}]

        all_segments.extend(segments)
    return all_segments


def merge_transcript(segments: list[dict]) -> dict:
    """Merge all segments into a final transcript with full text."""
    full_text = " ".join(seg.get("text", "") for seg in segments if seg.get("text"))
    return {
        "text": full_text.strip(),
        "segments": segments,
        "total_segments": len(segments),
    }

def format_timestamp_srt(seconds:float) -> str:
    """Convert seconds to SRT timestamp format: HH:MM:SS,mmm"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int((seconds % 1) * 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


def format_timestamp_vtt(seconds: float) -> str:
    """Convert seconds to VTT timestamp format: HH:MM:SS.mmm"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int((seconds % 1) * 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"

def generate_srt(segments: list[dict],output_path:str) -> dict:
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
        lines.append("")  # blank line between entries

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return output_path
def generate_vtt(segments: list[dict], output_path: str) -> str:
    """Generate WebVTT subtitle file from segments."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    lines = ["WEBVTT",""]
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



