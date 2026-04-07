"""
Subtitle Worker (S3 version)
Pop task → download chunk results from S3 → merge → generate SRT/VTT → burn → upload to S3 → push completion.
"""
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


def load_chunk_results_from_s3(job_id: str) -> list[dict]:
    """Load all chunk JSON results from S3 and merge into a single timeline."""
    prefix = f"results/{job_id}/chunk_"
    files = s3.list_files(prefix)

    if not files:
        raise RuntimeError(f"No chunk results found in S3 for job {job_id}")

    # Sort by key name (chunk_0000.json, chunk_0001.json, ...)
    files = sorted(files, key=lambda f: f["key"])

    all_segments = []
    for file_info in files:
        if not file_info["key"].endswith(".json"):
            continue

        data_str = s3.download_json(file_info["key"])
        data = json.loads(data_str)

        segments = data.get("segments", [])
        if not segments and data.get("text"):
            segments = [{"start": 0.0, "end": 0.0, "text": data["text"]}]

        all_segments.extend(segments)

    return all_segments


async def process_task(message: dict):
    task_id = message["task_id"]
    job_id = message["job_id"]
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

            outputs = {"transcript": transcript_key}

            # Step 3: Generate subtitle files
            srt_key = None
            if subtitle_format in ("srt", "both"):
                local_srt = os.path.join(tmp_dir, "subtitles.srt")
                generate_srt(segments, local_srt)
                srt_key = f"results/{job_id}/subtitles.srt"
                s3.upload_file(local_srt, srt_key)
                outputs["srt"] = srt_key
                print(f"  [SUBTITLE] Generated and uploaded SRT")

            if subtitle_format in ("vtt", "both"):
                local_vtt = os.path.join(tmp_dir, "subtitles.vtt")
                generate_vtt(segments, local_vtt)
                vtt_key = f"results/{job_id}/subtitles.vtt"
                s3.upload_file(local_vtt, vtt_key)
                outputs["vtt"] = vtt_key
                print(f"  [SUBTITLE] Generated and uploaded VTT")

            # Step 4: Burn subtitles onto video (if requested)
            if burn:
                # Download original video
                local_video = os.path.join(tmp_dir, "video.mp4")
                s3.download_file(original_video, local_video)

                # Need SRT for burning
                local_srt_for_burn = os.path.join(tmp_dir, "subtitles.srt")
                if not os.path.exists(local_srt_for_burn):
                    generate_srt(segments, local_srt_for_burn)

                # Burn
                local_output = os.path.join(tmp_dir, "video_subtitled.mp4")
                print(f"  [SUBTITLE] Burning subtitles onto video...")
                burn_subtitles(local_video, local_srt_for_burn, local_output)

                # Upload burned video
                video_key = f"results/{job_id}/video_subtitled.mp4"
                s3.upload_file(local_output, video_key)
                outputs["video"] = video_key
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


