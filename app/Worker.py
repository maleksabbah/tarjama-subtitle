"""
Subtitle Worker (S3 version)
Pop task -> download chunk results from S3 -> merge -> generate SRT/VTT -> burn -> upload to S3 -> register files -> push completion.
Uses chunks_meta.json for frame-accurate absolute timestamps instead of hardcoded chunk duration.
"""
import httpx
import os
import json
import re
import tempfile
from app.Config import config
from app import Redis_client as rc
from app import S3_client as s3
from app.Generator import (
    merge_transcript,
    generate_srt,
    generate_vtt,
    save_transcript,
    format_timestamp_srt,
)
from app.Burner import burn_subtitles

STORAGE_URL = os.environ.get("STORAGE_URL", "http://storage:8002")
DEFAULT_FPS = 24.0


async def register_file(job_id, user_id, category, ftype, path, mime_type, size_bytes=0):
    """Register an output file with the storage service."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{STORAGE_URL}/files/register",
                json={
                    "job_id": job_id,
                    "user_id": user_id,
                    "category": category,
                    "type": ftype,
                    "path": path,
                    "mime_type": mime_type,
                    "size_bytes": size_bytes,
                },
            )
            print(f"  [SUBTITLE] Registered {ftype}: {path}")
    except Exception as e:
        print(f"  [SUBTITLE] register_file failed: {e}")


def snap_to_frame(seconds: float, fps: float) -> float:
    """Snap a timestamp to the nearest video frame boundary."""
    frame = round(seconds * fps)
    return frame / fps


def _chunk_index_from_key(key: str) -> int:
    m = re.search(r"chunk_(\d+)", key)
    return int(m.group(1)) if m else 0


def load_chunks_meta(job_id: str) -> dict:
    """
    Load chunks_meta.json from S3.
    Returns dict keyed by chunk_index with absolute_start, absolute_end, fps.
    Falls back to empty dict if not found (backward compat).
    """
    meta_key = f"chunks/{job_id}/chunks_meta.json"
    try:
        data_str = s3.download_json(meta_key)
        data = json.loads(data_str)
        # Build lookup by chunk index
        meta_by_index = {}
        for chunk in data.get("chunks", []):
            meta_by_index[chunk["chunk_index"]] = chunk
        return meta_by_index, data.get("fps", DEFAULT_FPS)
    except Exception as e:
        print(f"  [SUBTITLE] chunks_meta.json not found, using fallback: {e}")
        return {}, DEFAULT_FPS


def load_chunk_results_from_s3(job_id: str) -> list[dict]:
    """
    Load all chunk JSON results from S3 and merge into a single absolute timeline.
    Uses chunks_meta.json for precise frame-accurate offsets.
    Falls back to index-based estimation if meta not available.
    """
    prefix = f"results/{job_id}/chunk_"
    files = s3.list_files(prefix)

    if not files:
        raise RuntimeError(f"No chunk results found in S3 for job {job_id}")

    files = sorted(files, key=lambda f: f["key"])

    # Load chunk metadata for accurate offsets
    chunks_meta, fps = load_chunks_meta(job_id)

    all_segments = []
    estimated_offset = 0.0  # fallback accumulator

    for file_info in files:
        key = file_info["key"]
        if not key.endswith(".json"):
            continue

        idx = _chunk_index_from_key(key)

        # Get absolute start from metadata if available
        if idx in chunks_meta:
            absolute_start = chunks_meta[idx]["absolute_start"]
            chunk_duration = chunks_meta[idx]["duration"]
        else:
            # Fallback: accumulate from previous chunks
            absolute_start = estimated_offset
            chunk_duration = 30.0  # best guess fallback

        data_str = s3.download_json(key)
        data = json.loads(data_str)

        segments = data.get("segments", [])
        if not segments and data.get("text"):
            segments = [{"start": 0.0, "end": chunk_duration, "text": data["text"]}]

        for seg in segments:
            text = (seg.get("text") or "").strip()
            if not text:
                continue
            # Add absolute offset and snap to frame
            raw_start = float(seg.get("start", 0.0)) + absolute_start
            raw_end = float(seg.get("end", chunk_duration)) + absolute_start
            start = snap_to_frame(raw_start, fps)
            end = snap_to_frame(raw_end, fps)
            if end <= start:
                end = snap_to_frame(start + (1.0 / fps), fps)
            all_segments.append({"start": start, "end": end, "text": text})

        estimated_offset += chunk_duration

    return all_segments


async def process_task(message: dict):
    task_id = message["task_id"]
    job_id = message["job_id"]
    user_id = message.get("user_id", 0)
    original_video = message["original_video"]
    subtitle_format = message.get("format", "srt")
    burn = message.get("burn", False)

    print(f"  [SUBTITLE] Processing job {job_id}")

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Step 1: Load chunk results from S3
            print(f"  [SUBTITLE] Loading chunk results from S3...")
            segments = load_chunk_results_from_s3(job_id)
            transcript = merge_transcript(segments)
            print(f"  [SUBTITLE] Merged {len(segments)} segments")

            # Step 2: Save and upload transcript
            local_transcript = os.path.join(tmp_dir, "transcript.json")
            save_transcript(transcript, local_transcript)
            transcript_key = f"results/{job_id}/transcript.json"
            s3.upload_file(local_transcript, transcript_key)
            await register_file(job_id, user_id, "transcript", "json", transcript_key, "application/json")

            outputs = {"transcript": transcript_key}

            # Step 3: Generate subtitle files
            if subtitle_format in ("srt", "both"):
                local_srt = os.path.join(tmp_dir, "subtitles.srt")
                generate_srt(segments, local_srt)
                srt_key = f"results/{job_id}/subtitles.srt"
                s3.upload_file(local_srt, srt_key)
                outputs["srt"] = srt_key
                await register_file(job_id, user_id, "subtitle", "srt", srt_key, "application/x-subrip")
                print(f"  [SUBTITLE] Generated and uploaded SRT")

            if subtitle_format in ("vtt", "both"):
                local_vtt = os.path.join(tmp_dir, "subtitles.vtt")
                generate_vtt(segments, local_vtt)
                vtt_key = f"results/{job_id}/subtitles.vtt"
                s3.upload_file(local_vtt, vtt_key)
                outputs["vtt"] = vtt_key
                await register_file(job_id, user_id, "subtitle", "vtt", vtt_key, "text/vtt")
                print(f"  [SUBTITLE] Generated and uploaded VTT")

            # Step 4: Burn subtitles onto video (if requested)
            if burn:
                local_video = os.path.join(tmp_dir, "video.mp4")
                s3.download_file(original_video, local_video)

                local_srt_for_burn = os.path.join(tmp_dir, "subtitles.srt")
                if not os.path.exists(local_srt_for_burn):
                    generate_srt(segments, local_srt_for_burn)

                local_output = os.path.join(tmp_dir, "video_subtitled.mp4")
                print(f"  [SUBTITLE] Burning subtitles onto video...")
                burn_subtitles(local_video, local_srt_for_burn, local_output)

                video_key = f"results/{job_id}/video_subtitled.mp4"
                s3.upload_file(local_output, video_key)
                outputs["video"] = video_key
                await register_file(job_id, user_id, "video", "mp4", video_key, "video/mp4")
                print(f"  [SUBTITLE] Uploaded burned video")

            # Step 5: Push completion
            await rc.push_completed({
                "task_id": task_id,
                "job_id": job_id,
                "type": "subtitle",
                "status": "completed",
                "outputs": outputs,
            })

            print(f"  [SUBTITLE] Job {job_id} done")

    except Exception as e:
        print(f"  [SUBTITLE] Failed job {job_id}: {e}")
        await rc.push_completed({
            "task_id": task_id,
            "job_id": job_id,
            "type": "subtitle",
            "status": "failed",
            "error": str(e),
        })
