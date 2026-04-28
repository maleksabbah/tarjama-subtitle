"""
Subtitle Worker (S3 version)
Pop task → load single transcript JSON → snap timestamps to frame boundaries
→ generate SRT/VTT → burn → upload to S3 → register files → push completion.

Pushes TWO completion messages when burn is requested:
  1. stage="subtitles" after SRT/VTT/transcript are uploaded & registered
  2. stage="burn" after burned video is uploaded & registered

When burn is NOT requested, only the subtitles completion is sent (with
final=True so the orchestrator marks the job completed).
"""
import httpx
import os
import json
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
DEFAULT_FPS = 25.0


async def register_file(job_id, user_id, category, ftype, path, mime_type, size_bytes=0):
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


def load_video_meta(job_id: str) -> dict:
    """Load video_meta.json from S3. Returns fps and duration."""
    meta_key = f"audio/{job_id}/video_meta.json"
    try:
        data_str = s3.download_json(meta_key)
        return json.loads(data_str)
    except Exception as e:
        print(f"  [SUBTITLE] video_meta.json not found, using default fps: {e}")
        return {"fps": DEFAULT_FPS, "duration": 0}


def load_transcript_from_s3(job_id: str, fps: float) -> list[dict]:
    """
    Load the single transcript JSON from S3.
    Snaps all Whisper timestamps to frame boundaries using the video fps.
    """
    transcript_key = f"results/{job_id}/transcript.json"
    data_str = s3.download_json(transcript_key)
    data = json.loads(data_str)

    segments = data.get("segments", [])
    duration = data.get("duration_seconds", 0)

    if not segments and data.get("text"):
        segments = [{"start": 0.0, "end": duration, "text": data["text"]}]

    snapped = []
    for seg in segments:
        text = (seg.get("text") or "").strip()
        if not text:
            continue
        start = snap_to_frame(float(seg.get("start", 0.0)), fps)
        end = snap_to_frame(float(seg.get("end", duration)), fps)
        if end <= start:
            end = snap_to_frame(start + (1.0 / fps), fps)
        snapped.append({"start": start, "end": end, "text": text})

    return snapped


async def process_task(message: dict):
    task_id = message["task_id"]
    job_id = message["job_id"]
    user_id = message.get("user_id", 0)
    original_video = message["original_video"]
    subtitle_format = message.get("format", "srt")
    burn = message.get("burn", False)

    print(f"  [SUBTITLE] Processing job {job_id} (burn={burn})")

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            # ─── Step 1: Load video metadata for fps ────────────────────
            meta = load_video_meta(job_id)
            fps = meta.get("fps", DEFAULT_FPS)
            print(f"  [SUBTITLE] Using fps={fps:.3f} for frame-accurate timestamps")

            # ─── Step 2: Load transcript and snap timestamps ────────────
            print(f"  [SUBTITLE] Loading transcript from S3...")
            segments = load_transcript_from_s3(job_id, fps)
            transcript = merge_transcript(segments)
            print(f"  [SUBTITLE] Loaded {len(segments)} segments")

            # ─── Step 3: Save and upload transcript ─────────────────────
            local_transcript = os.path.join(tmp_dir, "transcript.json")
            save_transcript(transcript, local_transcript)
            transcript_key = f"results/{job_id}/transcript.json"
            s3.upload_file(local_transcript, transcript_key)
            await register_file(job_id, user_id, "transcript", "json", transcript_key, "application/json")

            outputs = {"transcript": transcript_key}

            # ─── Step 4: Generate subtitle files ────────────────────────
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

            # ─── Step 5: Push FIRST completion (subtitles done) ─────────
            # If burn is NOT requested, this is the final message.
            # If burn IS requested, the orchestrator will move job to "burning"
            # and wait for the second completion.
            await rc.push_completed({
                "task_id": task_id,
                "job_id": job_id,
                "type": "subtitle",
                "stage": "subtitles",
                "status": "completed",
                "final": not burn,
                "outputs": outputs,
            })
            print(f"  [SUBTITLE] Pushed subtitles-stage completion for job {job_id}")

            # ─── Step 6: Burn subtitles onto video (if requested) ───────
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
                await register_file(job_id, user_id, "video", "mp4", video_key, "video/mp4")
                print(f"  [SUBTITLE] Uploaded burned video")

                # ─── Step 7: Push SECOND completion (burn done) ─────────
                await rc.push_completed({
                    "task_id": task_id,
                    "job_id": job_id,
                    "type": "subtitle",
                    "stage": "burn",
                    "status": "completed",
                    "final": True,
                    "outputs": {"video": video_key},
                })
                print(f"  [SUBTITLE] Pushed burn-stage completion for job {job_id}")

            print(f"  [SUBTITLE] Job {job_id} done")

    except Exception as e:
        print(f"  [SUBTITLE] Failed job {job_id}: {e}")
        await rc.push_completed({
            "task_id": task_id,
            "job_id": job_id,
            "type": "subtitle",
            "stage": "burn" if burn else "subtitles",
            "status": "failed",
            "final": True,
            "error": str(e),
        })
